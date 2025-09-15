/*
Georg Heindl
Läd Layer mit Regctl parallel in den RAM und analysiert parallel mit Gitleaks-Fork.
Params:
- rootCTX: Kontext für Abbruch
- reader: JSONL-Reader mit Layern
- resultPath: Pfad für Ergebnisse
- maxStorage: Maximale Größe des Zwischenspeichers in Byte
- scanWorkers: Anzahl paralleler Scanner (<#CPU-Kerne stark empfohlen)
- downloadWorkers: Anzahl paralleler Downloader
*/
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

/*
Run startet den Runtime-Handler mit Download- und Scan-Workern.
Blockierend.
*/
func (r *RuntimeHandler) Run() error {
	inputChan := make(chan types.LayerEntry, r.downloadWorkers*4)
	outputChan := make(chan types.Extracted, r.scanWorkers*4)

	var downloadWG sync.WaitGroup
	var scanWG sync.WaitGroup

	for i := 0; i < r.downloadWorkers; i++ {
		rc := network.NewRegClient(r.rootCTX, r.storageHandler)
		downloadWG.Add(1)
		go func(rc *network.RegClient) {
			defer downloadWG.Done()
			rc.Run(inputChan, outputChan)
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
			s.Run(outputChan)
			s.Close()
		}(scanner)
	}

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

		select {
		case <-r.rootCTX.Done():
			break READ_LOOP
		case inputChan <- rec:
		}
	}

	close(inputChan)

	downloadWG.Wait()

	close(outputChan)

	scanWG.Wait()

	if err := r.reader.Scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}
	return nil
}
