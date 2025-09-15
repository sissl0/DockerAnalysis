/*
Georg Heindl
Paralleler RegCtl Wrapper um Layer herunterzuladen.
Liest Layer-Informationen von einem Input-Channel und schreibt den Blob Reader in einen Output-Channel.
Daten werden im RAM gehalten.
Hat Speicherreservierung, um RAM-Overflow zu vermeiden.
Decompression Faktor wird als 3 angenommen.
Params:
- parent: Übergeordneter Kontext für Abbruch
- storageHandler: Handler zur Speicherplatzverwaltung
*/

package network

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/regclient/regclient"
	"github.com/regclient/regclient/types/descriptor"
	"github.com/regclient/regclient/types/ref"
	"github.com/sirupsen/logrus"
	"github.com/sissl0/DockerAnalysis/internal/types"
	"github.com/sissl0/DockerAnalysis/pkg/database"
)

type RegClient struct {
	Client             *regclient.RegClient
	rootCTX            context.Context
	rootCancel         context.CancelFunc
	logger             *logrus.Logger
	storagehandler     *database.StorageHandler
	uncompressedFactor float64
	scopeCount         int
}

func NewRegClient(parent context.Context, storageHandler *database.StorageHandler) *RegClient {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
	rc := regclient.New(regclient.WithLog(logger))

	if parent == nil {
		parent = context.Background()
	}
	rootCTX, rootCancel := context.WithCancel(parent)

	return &RegClient{
		Client:             rc,
		rootCTX:            rootCTX,
		rootCancel:         rootCancel,
		logger:             logger,
		storagehandler:     storageHandler,
		uncompressedFactor: 3.0,
		scopeCount:         0,
	}
}

/*
Liest Layer-Informationen von einem Input-Channel und schreibt den Blob Reader in einen Output-Channel.
*/
func (r *RegClient) Run(input <-chan types.LayerEntry, output chan<- types.Extracted) {
	for record := range input {
		select {
		case <-r.rootCTX.Done():
			return
		default:
		}
		ext, err := r.GetBlob(record)
		if err != nil {
			fmt.Printf("Error processing layer %s: %v\n", record.Digest, err)
			continue // nichts senden bei Fehler
		}
		output <- ext
	}
}

/*
Stoppt den RegClient und alle laufenden Operationen.
*/
func (r *RegClient) Stop() {
	if r.rootCancel != nil {
		r.rootCancel()
	}
}

/*
Läd Blob und gibt den Reader zurück.
Regctl entfernt Repositorys nicht aus Auth Bearer Scope, deshalb wird der Client alle 70 Anfragen neu erstellt.
(Ab 70 Anfragen tritt sonst ein Fehler auf: "401 Unauthorized: authentication required")
*/
func (r *RegClient) GetBlob(record types.LayerEntry) (types.Extracted, error) {
	ctx, cancel := context.WithTimeout(r.rootCTX, 30*time.Minute)
	// Kein defer cancel im Loop
	// --- Reservierungs-Schätzung
	pred := record.Size
	if p := int64(float64(record.Size) * r.uncompressedFactor); p > pred {
		pred = p
	}

	if !r.storagehandler.Acquire(pred) {
		cancel()
		return types.Extracted{}, fmt.Errorf("not enough storage for layer %s", record.Digest)
	}
	reserved := pred

	reference, err := ref.New(record.Repo)
	if err != nil {
		cancel()
		r.storagehandler.Release(reserved)
		return types.Extracted{}, fmt.Errorf("error parsing reference %s: %v", record.Repo, err)
	}
	desc := descriptor.Descriptor{
		Digest:    digest.Digest(record.Digest),
		MediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
		Size:      record.Size,
	}
	reader, err := r.Client.BlobGet(ctx, reference, desc)
	if err != nil {
		cancel()
		r.storagehandler.Release(reserved)
		return types.Extracted{}, fmt.Errorf("error getting blob %s from %s: %v", record.Digest, record.Repo, err)
	}

	if sc := reader.Response().StatusCode; sc == http.StatusTooManyRequests {
		reader.Close()
		cancel()
		r.storagehandler.Release(reserved)
		time.Sleep(300 * time.Second)
		return types.Extracted{}, fmt.Errorf("429 too many requests for %s", record.Digest)
	}

	if reader.Response().ProtoMajor == 2 && reader.Response().StatusCode == http.StatusInternalServerError {
		reader.Close()
		cancel()
		r.storagehandler.Release(reserved)
		r.Stop()
		return types.Extracted{}, fmt.Errorf("transient http2 500 for %s", record.Digest)
	}

	ext := types.Extracted{
		Record:   record,
		Layer:    reader,
		Ctx:      ctx,
		Cancel:   cancel,
		Reserved: reserved,
	}

	r.scopeCount++
	if r.scopeCount > 70 {
		r.scopeCount = 0
		r.Client.Close(ctx, reference)
		r.Client = regclient.New(regclient.WithLog(r.logger))
	}

	return ext, nil
}
