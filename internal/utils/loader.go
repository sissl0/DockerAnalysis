/*
Georg Heindl
Hilfsfunktionen zum Laden und Verarbeiten der JSONL-Dateien mit Layer- und Tag-Informationen.
*/
package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/sissl0/DockerAnalysis/pkg/database"
)

type LayerEntry struct {
	Digest string `json:"layer_digest"`
	Repo   string `json:"repo"`
	Size   int64  `json:"size"`
}

/*
Läd Tags in eine JSONL-Datei mit Repo+Digest.
*/
func LoadTags(outputfile string) error {
	writer, err := database.NewJSONLWriter(outputfile)
	if err != nil {
		return fmt.Errorf("error creating JSONL writer: %w", err)
	}
	defer writer.Close()
	for i := 0; i < 56; i++ {
		tag_path := fmt.Sprintf("tags/tags__%d.jsonl", i)
		reader, err := database.NewJSONLReader(tag_path)
		if err != nil {
			fmt.Println("Error opening JSONL reader:", err)
			return err
		}

		for reader.Scanner.Scan() {
			line := reader.Scanner.Text()
			var record map[string]any
			err := json.Unmarshal([]byte(line), &record)
			if err != nil {
				fmt.Println("Error unmarshalling JSON:", err)
				continue
			}
			repo_digest := map[string]string{
				"repo":   record["repo_name"].(string),
				"digest": record["digest"].(string),
			}
			if err := writer.Write(repo_digest); err != nil {
				fmt.Println("Error writing to JSONL writer:", err)
				continue
			}
		}
	}

	return nil
}

/*
Läd Layer in eine JSONL-Datei mit Repo+Layer+Size.
MaxFiles = Anzahl der layer__X.jsonl Dateien.
Parallel, aber ungeeignet für sehr große Datenmengen (Redis speichert alle Layer im RAM).
*/
func LoadLayers(layerfilepath string, maxFiles int, outputfile string) error {
	writer, err := database.NewJSONLWriter(outputfile)
	if err != nil {
		return fmt.Errorf("error creating JSONL writer: %w", err)
	}
	defer writer.Close()

	redisCli := database.NewRedisClient("localhost:6379", "", 0)
	ctx := context.Background()

	type layerRec struct {
		Layer string
		Repo  string
		Size  float64
	}

	layerCh := make(chan layerRec, 1000)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < maxFiles; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			layerPath := fmt.Sprintf("%s/layers__%d.jsonl", layerfilepath, i)
			reader, err := database.NewJSONLReader(layerPath)
			if err != nil {
				errCh <- fmt.Errorf("error opening JSONL reader: %w", err)
				return
			}
			for reader.Scanner.Scan() {
				line := reader.Scanner.Text()
				var record map[string]any
				if err := json.Unmarshal([]byte(line), &record); err != nil {
					fmt.Println("Error unmarshalling JSON:", err)
					continue
				}
				if record["layer_digest"] == nil || record["repo"] == nil || record["size"] == nil {
					continue
				}
				layerCh <- layerRec{
					Layer: record["layer_digest"].(string),
					Repo:  record["repo"].(string),
					Size:  record["size"].(float64),
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(layerCh)
		close(errCh)
	}()

	var totalsize float64

	for layer := range layerCh {
		added, err := redisCli.AddToSet(ctx, "scanned_layers", layer.Layer)
		if err != nil {
			fmt.Println("Error adding to Redis:", err)
			continue
		}
		if added == 0 {
			continue
		}

		if err := writer.Write(map[string]any{
			"layer": layer.Layer,
			"repo":  layer.Repo,
			"size":  layer.Size,
		}); err != nil {
			fmt.Println("Error writing JSONL:", err)
			continue
		}
		totalsize += layer.Size * 1e-9
	}

	fmt.Println("Total size of layers:", totalsize)

	select {
	case e := <-errCh:
		return e
	default:
		return nil
	}
}

func makeLogBuckets() []int64 {
	var bounds []int64
	minSize := float64(1)
	maxSize := float64(10 * 1_000_000_000)
	steps := 9
	logMin := math.Log10(minSize)
	logMax := math.Log10(maxSize)
	step := (logMax - logMin) / float64(steps-1)
	for i := 0; i < steps; i++ {
		b := math.Pow(10, logMin+step*float64(i))
		bounds = append(bounds, int64(b))
	}
	bounds = append(bounds, math.MaxInt64)
	return bounds
}

func bucketIndex(size int64, bounds []int64) int {
	for i, b := range bounds {
		if size <= b {
			return i
		}
	}
	return len(bounds) - 1
}

func parseSize(num json.Number) (int64, bool) {
	if i, err := num.Int64(); err == nil {
		if i > 0 {
			return i, true
		}
		return 0, false
	}
	if f, err := num.Float64(); err == nil {
		if f <= 0 {
			return 0, false
		}
		return int64(math.Round(f)), true
	}
	return 0, false
}

func CreateSample(uniqueLayerPath string, samplePath string) error {
	reader, err := database.NewJSONLReader(uniqueLayerPath)
	if err != nil {
		return fmt.Errorf("error creating JSONL reader: %w", err)
	}
	defer reader.Close()

	bounds := makeLogBuckets()

	type raw struct {
		Digest string      `json:"layer_digest"`
		Layer  string      `json:"layer"`
		Repo   string      `json:"repo"`
		Size   json.Number `json:"size"`
	}

	totalSize := int64(0)
	bucketSizes := make([]int64, len(bounds))

	// PASS 1: Summen je Bucket
	for reader.Scanner.Scan() {
		var r raw
		if err := json.Unmarshal([]byte(reader.Scanner.Text()), &r); err != nil {
			continue
		}
		sz, ok := parseSize(r.Size)
		if !ok {
			continue
		}
		name := r.Digest
		if name == "" {
			name = r.Layer
		}
		if name == "" {
			continue
		}
		bi := bucketIndex(sz, bounds)
		bucketSizes[bi] += sz
		totalSize += sz
	}
	if err := reader.Scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}
	if totalSize == 0 {
		return fmt.Errorf("no data")
	}

	targetTotal := int64(150) * 1_000_000_000_000 // 150 TB (decimal)
	if targetTotal > totalSize {
		targetTotal = totalSize
	}

	// Quota (float) je Bucket proportional zur Bytegröße
	quotaRemain := make([]float64, len(bounds))
	bytesRemain := make([]int64, len(bounds))
	for i, bs := range bucketSizes {
		bytesRemain[i] = bs
		if bs == 0 {
			continue
		}
		quotaRemain[i] = float64(bs) * float64(targetTotal) / float64(totalSize)
	}

	// PASS 2: Bernoulli nach verbleibenden Quoten
	reader2, err := database.NewJSONLReader(uniqueLayerPath)
	if err != nil {
		return fmt.Errorf("error creating JSONL reader (pass2): %w", err)
	}
	defer reader2.Close()

	writer, err := database.NewJSONLWriter(samplePath)
	if err != nil {
		return fmt.Errorf("error creating writer: %w", err)
	}
	defer writer.Close()

	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))

	finalBytes := int64(0)
	selectedCount := 0

	for reader2.Scanner.Scan() {
		var r raw
		if err := json.Unmarshal([]byte(reader2.Scanner.Text()), &r); err != nil {
			continue
		}
		sz, ok := parseSize(r.Size)
		if !ok {
			continue
		}
		name := r.Digest
		if name == "" {
			name = r.Layer
		}
		if name == "" {
			continue
		}
		bi := bucketIndex(sz, bounds)
		if bytesRemain[bi] <= 0 || quotaRemain[bi] <= 0 {
			bytesRemain[bi] -= sz
			continue
		}

		// Wahrscheinlichkeit = (verbleibende Quote) / (verbleibende Bytes)
		prob := quotaRemain[bi] / float64(bytesRemain[bi])
		if prob > 1 {
			prob = 1
		} else if prob < 0 {
			prob = 0
		}

		if rng.Float64() <= prob {
			// Aufnahme
			entry := LayerEntry{
				Digest: name,
				Repo:   r.Repo,
				Size:   sz,
			}
			if err := writer.Write(entry); err != nil {
				return fmt.Errorf("write error: %w", err)
			}
			finalBytes += sz
			selectedCount++
			quotaRemain[bi] -= float64(sz)
			if quotaRemain[bi] < 0 {
				quotaRemain[bi] = 0
			}
		}

		bytesRemain[bi] -= sz
	}

	if err := reader2.Scanner.Err(); err != nil {
		return fmt.Errorf("scanner error pass2: %w", err)
	}

	// Statistik
	finalTB := float64(finalBytes) / 1e12
	targetTB := float64(targetTotal) / 1e12
	// Optional: Hinweis wenn deutliche Abweichung
	if diff := math.Abs(finalTB - targetTB); targetTB > 0 && diff/targetTB > 0.02 {
		fmt.Printf("Warn: sample deviates by %.2f%% from target\n", diff/targetTB*100)
	}

	fmt.Printf("Sampling seed: %d\n", seed)
	fmt.Printf("Input total size: %.2f TB\n", float64(totalSize)/1e12)
	fmt.Printf("Target size: %.2f TB\n", targetTB)
	fmt.Printf("Final sample size: %.2f TB (items=%d)\n", finalTB, selectedCount)

	return nil
}
