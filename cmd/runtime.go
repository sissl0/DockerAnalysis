package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/sissl0/DockerAnalysis/internal/network"
	"github.com/sissl0/DockerAnalysis/internal/types"
	"github.com/sissl0/DockerAnalysis/internal/utils"
	"github.com/sissl0/DockerAnalysis/pkg/database"
)

type RuntimeHandler struct {
	rootCTX            context.Context
	resultPath         string
	storageHandler     *database.StorageHandler
	reader             *database.JSONLReader
	uncompressedFactor float64

	scanWorkers     int
	downloadWorkers int
}

func NewRuntimeHandler(rootCTX context.Context, reader *database.JSONLReader, resultPath string, maxStorage int64, scanWorkers int, downloadWorkers int) *RuntimeHandler {
	sh := database.NewStorageHandler(maxStorage)
	sh.WithContext(rootCTX)
	rh := &RuntimeHandler{
		rootCTX:            rootCTX,
		resultPath:         resultPath,
		storageHandler:     database.NewStorageHandler(maxStorage),
		reader:             reader,
		uncompressedFactor: 3.0,
		scanWorkers:        scanWorkers,
		downloadWorkers:    downloadWorkers,
	}
	return rh
}

func (r *RuntimeHandler) Run() error {
	inputChan := make(chan types.LayerEntry, r.downloadWorkers*4)
	outputChan := make(chan types.Extracted, r.scanWorkers*4)

	var downloadWG sync.WaitGroup
	var scanWG sync.WaitGroup

	// Downloader starten
	for i := 0; i < r.downloadWorkers; i++ {
		rc := network.NewRegClient(r.rootCTX, r.storageHandler)
		downloadWG.Add(1)
		go func(rc *network.RegClient) {
			defer downloadWG.Done()
			rc.Run(inputChan, outputChan) // wg-Parameter entfernt
			rc.Stop()
		}(rc)
	}

	for i := 0; i < r.scanWorkers; i++ {
		scanner, err := utils.NewScanner(uint16(i), r.resultPath, r.storageHandler)
		if err != nil {
			close(inputChan)
			return fmt.Errorf("error creating scanner %d: %v", i, err)
		}
		scanWG.Add(1)
		go func(s *utils.Scanner) {
			defer scanWG.Done()
			s.Run(outputChan) // wg-Parameter entfernt
			s.Close()
		}(scanner)
	}

	//repoCP := "balenalib/odyssey-x86-alpine-golang"
	//repoReached := false
	layerCP := "sha256:733b86c97151200d5790f5814ebe8aea50876842e76aca027cf2f52ff7ef02aa"
	layerReached := false

READ_LOOP:
	for r.reader.Scanner.Scan() {
		select {
		case <-r.rootCTX.Done():
			break READ_LOOP
		default:
		}

		line := r.reader.Scanner.Text()
		var rec types.LayerEntry
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			fmt.Println("Error unmarshalling JSON:", err)
			continue
		}
		// if rec.Repo == repoCP {
		// 	repoReached = true
		// }
		// if !repoReached {
		// 	continue
		// }
		if rec.Digest == layerCP {
			layerReached = true
		}
		if !layerReached {
			continue
		}
		select {
		case <-r.rootCTX.Done():
			break READ_LOOP
		case inputChan <- rec:
		}
	}

	// Producer fertig oder abgebrochen
	close(inputChan)

	// Warten bis Downloader leer sind
	downloadWG.Wait()

	// Downloader schreiben nicht mehr -> Ausgabe schließen
	close(outputChan)

	// Scanner fertig
	scanWG.Wait()

	if err := r.reader.Scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}
	return nil
}
