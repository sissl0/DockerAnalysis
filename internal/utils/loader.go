package utils

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/sissl0/DockerAnalysis/pkg/database"
)

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

	// Worker zum Parsen der JSONL-Files (parallel)
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

	// Channel schließen wenn alles gelesen ist
	go func() {
		wg.Wait()
		close(layerCh)
		close(errCh)
	}()

	// Consumer: schreibt nach Redis + JSONL
	var totalsize float64

	for layer := range layerCh {
		// Redis: SADD (einzeln, damit wir added==0 prüfen können)
		added, err := redisCli.AddToSet(ctx, "scanned_layers", layer.Layer)
		if err != nil {
			fmt.Println("Error adding to Redis:", err)
			continue
		}
		if added == 0 {
			continue // already in set
		}

		// JSONL schreiben
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

	// Fehler aus den Reader-Goroutines prüfen
	select {
	case e := <-errCh:
		return e
	default:
		return nil
	}
}

func ExtractTar(tarFilePath, outputPath string) error {
	file, err := os.Open(tarFilePath)
	if err != nil {
		return fmt.Errorf("error opening tar file: %w", err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("error creating gzip reader: %w", err)
	}
	defer gz.Close()

	tarReader := tar.NewReader(gz)
	fmt.Println(tarFilePath, outputPath)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("error reading tar file: %w", err)
		}

		targetPath := filepath.Join(outputPath, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("error creating directory: %w", err)
			}
		case tar.TypeReg:
			outFile, err := os.Create(targetPath)
			if err != nil {
				if err := os.MkdirAll(filepath.Dir(targetPath), os.ModePerm); err != nil {
					return fmt.Errorf("error creating parent directories: %w", err)
				}
				outFile, err = os.Create(targetPath)
				if err != nil {
					return fmt.Errorf("error creating file: %w", err)
				}
			}
			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("error writing file: %w", err)
			}
			outFile.Close()
			if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("error setting file permissions: %w", err)
			}
		default:
			return fmt.Errorf("unsupported tar header type: %v", header.Typeflag)
		}
	}

	return nil
}
