package types

import (
	"context"

	"github.com/regclient/regclient/types/blob"
)

type LayerEntry struct {
	Repo   string `json:"repo"`
	Digest string `json:"layer_digest"`
	Size   int64  `json:"size"`
}

type Extracted struct {
	Record   LayerEntry
	Layer    blob.Reader
	Ctx      context.Context
	Cancel   context.CancelFunc
	Reserved int64
}

type SecretInfo struct {
	Location  string `json:"file"`
	Type      string `json:"file_type"`
	Size      int64  `json:"file_size"`
	Origin    string `json:"origin"`
	Secret    string `json:"secret"`
	StartLine int    `json:"start_line"`
}

type SecretRecord struct {
	FragmentHash string     `json:"fragment_hash"`
	Secret       SecretInfo `json:"secrets"`
}

type FileRecord struct {
	Digest    string `json:"digest"`
	FileCount int    `json:"file_count"`
	// FileHashes       []string `json:"file_hashes"` // AUS: Entkommentieren, wenn benötigt
	MaxDepth         int      `json:"max_depth"`
	UncompressedSize int64    `json:"uncompressed_size"`
	Secrets          []string `json:"secrets,omitempty"`
}

type Image struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	LastPulled   string `json:"last_pulled"`
	LastPushed   string `json:"last_pushed"`
	Size         int64  `json:"size"`
	Digest       string `json:"digest"`
	Status       string `json:"status"`
}

type TagInfo struct {
	Images     []Image `json:"images"`
	LastPushed string  `json:"tag_last_pushed"`
}

type Layer struct {
	Size   int64  `json:"size"`
	Digest string `json:"digest"`
}

type RepoDigest struct {
	RepoName string `json:"repo"`
	Digest   string `json:"digest"`
}
