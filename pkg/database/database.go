package database

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type JSONLWriter struct {
	file   *os.File
	writer *bufio.Writer
}

func NewJSONLWriter(filePath string) (*JSONLWriter, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &JSONLWriter{
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

func (j *JSONLWriter) Write(record any) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = j.writer.Write(append(data, '\n'))
	return err
}

func (j *JSONLWriter) Close() error {
	if err := j.writer.Flush(); err != nil {
		return err
	}
	return j.file.Close()
}

type RotatingJSONLWriter struct {
	dir          string
	baseFilename string
	maxSize      int64 // maxSize is in bytes
	currentSize  int64
	currentFile  *JSONLWriter
	fileIndex    int
}

func NewRotatingJSONLWriter(dir, baseFilename string, maxSize int64, fileIndex int) (*RotatingJSONLWriter, error) {
	writer := &RotatingJSONLWriter{
		dir:          dir,
		baseFilename: baseFilename,
		maxSize:      maxSize,
		fileIndex:    fileIndex,
	}
	if err := writer.rotateFile(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (r *RotatingJSONLWriter) rotateFile() error {
	if r.currentFile != nil {
		if err := r.compressAndClose(); err != nil {
			return err
		}
	}
	filePath := filepath.Join(r.dir, fmt.Sprintf("%s_%d.jsonl", r.baseFilename, r.fileIndex))
	writer, err := NewJSONLWriter(filePath)
	if err != nil {
		return err
	}
	r.currentFile = writer
	r.currentSize = 0
	r.fileIndex++
	return nil
}

func (r *RotatingJSONLWriter) compressAndClose() error {
	if err := r.currentFile.Close(); err != nil {
		return err
	}
	filePath := filepath.Join(r.dir, fmt.Sprintf("%s_%d.jsonl", r.baseFilename, r.fileIndex-1))
	compressedPath := filePath + ".gz"
	inFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	outFile, err := os.Create(compressedPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, inFile)
	if err != nil {
		return err
	}

	return os.Remove(filePath)
}

func (r *RotatingJSONLWriter) Write(record any) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	dataSize := int64(len(data) + 1) // +1 for the newline character
	if r.currentSize+dataSize > r.maxSize {
		if err := r.rotateFile(); err != nil {
			return err
		}
	}
	if err := r.currentFile.Write(record); err != nil {
		return err
	}
	r.currentSize += dataSize
	return nil
}

func (r *RotatingJSONLWriter) Close() error {
	if r.currentFile != nil {
		return r.compressAndClose()
	}
	return nil
}

type JSONLReader struct {
	file    *os.File
	Scanner *bufio.Scanner
}

func NewJSONLReader(filePath string) (*JSONLReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	return &JSONLReader{
		file:    file,
		Scanner: scanner,
	}, nil
}

func (j *JSONLReader) Read() (map[string]any, error) {
	if j.Scanner.Scan() {
		var record map[string]any
		err := json.Unmarshal(j.Scanner.Bytes(), &record)
		return record, err
	}
	if err := j.Scanner.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (j *JSONLReader) Next() bool {
	return j.Scanner.Scan()
}

func (j *JSONLReader) Err() error {
	return j.Scanner.Err()
}

func (j *JSONLReader) Close() error {
	return j.file.Close()
}
