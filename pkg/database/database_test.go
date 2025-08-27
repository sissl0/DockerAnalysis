package database_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sissl0/DockerAnalysis/pkg/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONLWriter(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test.jsonl")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	writer, err := database.NewJSONLWriter(tempFile.Name())
	require.NoError(t, err)

	record := map[string]any{"reponame": "test"}
	err = writer.Write(record)
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	content, err := os.ReadFile(tempFile.Name())
	require.NoError(t, err)
	assert.Contains(t, string(content), `{"reponame":"test"}`)
}

func TestRotatingJSONLWriter(t *testing.T) {
	tempDir := t.TempDir()

	writer, err := database.NewRotatingJSONLWriter(tempDir, "test", 50, 0)
	require.NoError(t, err)

	record := map[string]any{"reponame": "test"}
	for range 10 {
		err = writer.Write(record)
		assert.NoError(t, err)
	}

	err = writer.Close()
	assert.NoError(t, err)

	files, err := os.ReadDir(tempDir)
	require.NoError(t, err)

	var compressedFiles int
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".gz" {
			compressedFiles++
		}
	}
	assert.Greater(t, compressedFiles, 0)
}

func TestJSONLReader(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test.jsonl")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	_, err = tempFile.WriteString(`{"reponame":"value1"}` + "\n")
	require.NoError(t, err)
	_, err = tempFile.WriteString(`{"reponame":"value2"}` + "\n")
	require.NoError(t, err)
	tempFile.Close()

	reader, err := database.NewJSONLReader(tempFile.Name())
	require.NoError(t, err)
	defer reader.Close()

	record, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "value1", record["reponame"])

	record, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "value2", record["reponame"])

	record, err = reader.Read()
	assert.NoError(t, err)
	assert.Nil(t, record)
}
