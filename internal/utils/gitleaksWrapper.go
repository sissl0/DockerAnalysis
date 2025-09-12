package utils

import (
	"compress/gzip"
	"context"
	"fmt"

	// "hash" // Hashing für Datei-Hashes. Entkommentieren, falls File-Hashes wieder benötigt werden.
	"os"
	"path/filepath"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/regclient/regclient/types/blob"
	"github.com/sissl0/DockerAnalysis/internal/types"
	"github.com/sissl0/DockerAnalysis/pkg/database"
	"github.com/zricethezav/gitleaks/v8/detect"
	"github.com/zricethezav/gitleaks/v8/sources"
)

const (
	cacheByteBudget = 500 * 1024 * 1024
	approxPerEntry  = 56
)

type Scanner struct {
	SecretWriter   *database.RotatingJSONLWriter
	FileInfoWriter *database.RotatingJSONLWriter
	StorageHandler *database.StorageHandler
	Detector       *detect.Detector
}

func NewScanner(num uint16, resultPath string, storagehandler *database.StorageHandler) (*Scanner, error) {
	if err := os.MkdirAll(fmt.Sprintf("%s/scanner_%d/secrets/", resultPath, num), 0755); err != nil {
		return nil, fmt.Errorf("error creating secrets directory: %v", err)
	}
	secretWriter, err := database.NewRotatingJSONLWriter(fmt.Sprintf("%s/scanner_%d/secrets/", resultPath, num), "secrets_", 500000000, 0)
	if err != nil {
		return nil, fmt.Errorf("error creating secret JSONL writer: %v", err)
	}

	if err := os.MkdirAll(fmt.Sprintf("%s/scanner_%d/fileinfos/", resultPath, num), 0755); err != nil {
		return nil, fmt.Errorf("error creating file info directory: %v", err)
	}
	fileInfoWriter, err := database.NewRotatingJSONLWriter(fmt.Sprintf("%s/scanner_%d/fileinfos/", resultPath, num), "fileinfo_", 500000000, 0)
	if err != nil {
		return nil, fmt.Errorf("error creating file info JSONL writer: %v", err)
	}
	detector, _ := detect.NewDetectorDefaultConfig()
	detector.MaxDecodeDepth = 3
	detector.BuildRuleIndex()

	return &Scanner{
		SecretWriter:   secretWriter,
		FileInfoWriter: fileInfoWriter,
		StorageHandler: storagehandler,
		Detector:       detector,
	}, nil
}

func (s *Scanner) Close() {
	s.SecretWriter.Close()
	s.FileInfoWriter.Close()
}

func (s *Scanner) Run(input <-chan types.Extracted) {
	for ext := range input {
		fmt.Printf("Scanning layer %s, Size: %d\n", ext.Record.Digest, ext.Record.Size)
		if err := s.ExtractScan(ext.Ctx, ext.Layer, ext.Record.Digest); err != nil {
			fmt.Printf("Error scanning layer %s: %v\n", ext.Record.Digest, err)
		}
		ext.Cancel() // Kontext jetzt freigeben
		s.StorageHandler.Release(ext.Reserved)
	}
}

func (s *Scanner) ExtractScan(ctx context.Context, reader blob.Reader, digest string) error {
	defer reader.Close()
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()

	layerFile := &sources.File{
		Content:         gz,
		Path:            fmt.Sprintf("%s.tar", digest),
		Config:          &s.Detector.Config,
		MaxArchiveDepth: s.Detector.MaxArchiveDepth,
	}

	cacheCap := int(cacheByteBudget / approxPerEntry)
	fCache := database.NewFragCache(cacheCap)

	getFragCache := func(h uint64) (bool, bool) { return fCache.Get(h) }
	setFragCache := func(h uint64, hasSecret bool) { fCache.Set(h, hasSecret) }

	var (
		currentPath string
		currentSize int64
		// currentHash  hash.Hash64            // AUS: Dateihash-Berechnung. Entkommentieren, falls File-Hashes wieder benötigt werden.
		// fileHashes   []string               // AUS: Sammlung der File-Hashes. Entkommentieren, falls File-Hashes wieder benötigt werden.
		// fileHashSeen = make(map[string]struct{}) // AUS: Deduplizierung der File-Hashes. Entkommentieren, falls File-Hashes wieder benötigt werden.
		fileCount    int
		maxDepth     int
		totalSize    int64
		secrets      []string
		fragHashList = make(map[uint64]bool)
		newSecrets   []types.SecretRecord
	)

	finalizeFile := func() {
		if currentPath == "" /* || currentHash == nil */ { // AUS: currentHash-Check. Entkommentieren, falls File-Hashes wieder benötigt werden.
			return
		}

		// AUS: Dateihash berechnen. Entkommentieren, falls File-Hashes wieder benötigt werden.
		// sum := fmt.Sprintf("%x", currentHash.Sum(nil))

		// schneller als strings.Split
		depth := 1
		if idx := strings.Count(currentPath, "/"); idx > 0 {
			depth = idx + 1
		}
		if depth > maxDepth {
			maxDepth = depth
		}

		if len(fragHashList) > 0 {
			for fh, hasSecret := range fragHashList {
				setFragCache(fh, hasSecret)
			}
		}

		// AUS: File-Hash-Deduplizierung/Speicherung. Entkommentieren, falls File-Hashes wieder benötigt werden.
		// if _, ok := fileHashSeen[sum]; !ok {
		// 	fileHashSeen[sum] = struct{}{}
		// 	fileHashes = append(fileHashes, sum)
		// }

		totalSize += currentSize
		fileCount++
		currentPath = ""
		currentSize = 0
		// currentHash = nil // AUS: Zurücksetzen des Dateihashes. Entkommentieren, falls File-Hashes wieder benötigt werden.

		// Neu initialisieren statt delete-Schleife (schneller)
		fragHashList = make(map[uint64]bool)
	}

	detectErr := layerFile.Fragments(ctx, func(fragment sources.Fragment, ferr error) error {
		if ferr != nil {
			fmt.Printf("fragment error path=%s err=%v (continue)\n", fragment.FilePath, ferr)
			return nil
		}
		if fragment.FilePath == "" || (len(fragment.Raw) == 0 && len(fragment.Bytes) == 0) {
			return nil
		}

		if fragment.FilePath != currentPath {
			finalizeFile()
			currentPath = fragment.FilePath
			currentSize = 0
			// currentHash = xxhash.New() // AUS: Dateihash initialisieren. Entkommentieren, falls File-Hashes wieder benötigt werden.
		}

		data := fragment.Bytes
		if len(data) == 0 && len(fragment.Raw) != 0 {
			data = []byte(fragment.Raw)
		}
		if len(data) == 0 {
			return nil
		}

		// _, _ = currentHash.Write(data) // AUS: Dateihash fortschreiben. Entkommentieren, falls File-Hashes wieder benötigt werden.
		currentSize += int64(len(data))

		fragHash := xxhash.Sum64(data)
		if _, seen := fragHashList[fragHash]; !seen {
			fragHashList[fragHash] = false
		}

		if hasSecret, ok := getFragCache(fragHash); ok {
			if hasSecret {
				secrets = append(secrets, fmt.Sprintf("%016x", fragHash))
			}
			return nil
		}

		findings := s.Detector.Detect(detect.Fragment(fragment))
		if len(findings) == 0 {
			setFragCache(fragHash, false)
			return nil
		}
		fragHashList[fragHash] = true
		setFragCache(fragHash, true)

		fragHashHex := fmt.Sprintf("%016x", fragHash)
		secrets = append(secrets, fragHashHex)

		for _, finding := range findings {
			newSecrets = append(newSecrets, types.SecretRecord{
				FragmentHash: fragHashHex,
				Secret: types.SecretInfo{
					Location:  currentPath,
					Type:      filepath.Ext(finding.File),
					Size:      currentSize,
					Origin:    finding.RuleID,
					Secret:    finding.Secret,
					StartLine: finding.StartLine,
				},
			})
		}
		return nil
	})
	finalizeFile()
	if detectErr != nil {
		fmt.Printf("scan finished with error: %v\n", detectErr)
	}

	fr := types.FileRecord{
		Digest:    digest,
		FileCount: fileCount,
		// FileHashes:       fileHashes, // AUS: Schreiben der File-Hashes. Entkommentieren, falls File-Hashes wieder benötigt werden.
		MaxDepth:         maxDepth,
		UncompressedSize: totalSize,
		Secrets:          secrets,
	}
	if err = s.FileInfoWriter.Write(fr); err != nil {
		return fmt.Errorf("error writing file record: %v", err)
	}
	if err = s.SecretWriter.Write(newSecrets); err != nil {
		return fmt.Errorf("error writing secrets: %v", err)
	}

	return nil
}
