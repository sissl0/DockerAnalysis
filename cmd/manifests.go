/*
Georg Heindl
Paralleler Collector für Docker Image Manifeste.
Führt Auth Requests durch, um Token zu erhalten, und ruft dann die Manifeste ab.
Speichert die Layer-Informationen in einer JSONL-Datei.
Params:
- username: Docker Hub Benutzername
- accessToken: Docker Hub Access Token
- timeout: Timeout für HTTP Requests
- cookies: Cookies für Auth Requests (falls benötigt)
- writer: JSONL Writer zum Speichern der Layer-Informationen
- reader: JSONL Reader zum Lesen der Tags
- proxies: Liste von Proxy-URLs für die parallelen Clients (falls keine Proxies, dann Liste mit leeren String(s) übergeben)
*/
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/sissl0/DockerAnalysis/internal/network"
	"github.com/sissl0/DockerAnalysis/internal/types"
	"github.com/sissl0/DockerAnalysis/pkg/database"
)

const (
	authBaseURL          = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:"
	manifestBaseURL      = "https://registry-1.docker.io/v2/"
	manifestEndpoint     = "/manifests/"
	manifestAcceptHeader = "application/vnd.docker.distribution.manifest.v2+json"
)

type ManifestsCollector struct {
	Username    string
	AccessToken string
	Writer      *database.RotatingJSONLWriter
	Reader      *database.JSONLReader
	AuthTasks   chan *network.AuthRequestTask
	Redis       *database.RedisClient
	SaveMutex   sync.Mutex
	Context     context.Context
}

func NewManifestsCollector(username, accessToken string, timeout int, cookies map[string]any, writer *database.RotatingJSONLWriter, reader *database.JSONLReader, proxies []string) (*ManifestsCollector, error) {
	authTasks := make(chan *network.AuthRequestTask, 7200)
	for i, proxy := range proxies {
		client1, err := network.NewClient(proxy, timeout, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("error creating first network client: %w", err)
		}
		go client1.AuthClientStart(authTasks, i)
	}

	redis := database.NewRedisClient("localhost:6379", "", 0)

	return &ManifestsCollector{
		Username:    username,
		AccessToken: accessToken,
		Writer:      writer,
		Reader:      reader,
		AuthTasks:   authTasks,
		Redis:       redis,
		SaveMutex:   sync.Mutex{},
		Context:     context.Background(),
	}, nil
}

/*
Liest die Tags aus der JSONL-Datei und ruft für jeden Tag das Manifest ab.
Speichert in Redis, welche Digests bereits verarbeitet wurden, und überspringt diese. (Absicherung für Abbruch)
*/
func (mc *ManifestsCollector) CollectManifests() {
	for mc.Reader.Scanner.Scan() {
		line := mc.Reader.Scanner.Text()
		var record map[string]any
		err := json.Unmarshal([]byte(line), &record)
		if err != nil {
			fmt.Println("Error unmarshalling JSON:", err)
			continue
		}
		repoDigest, _ := record["digest"].(string)
		repoName, _ := record["repo"].(string)

		if repoDigest == "" || repoName == "" {
			fmt.Println("Invalid record:", record)
			continue
		}
		isMember, err := mc.Redis.IsMember(mc.Context, "scanned_digests", repoDigest)
		if err != nil {
			fmt.Printf("Error checking Redis membership for %s: %v\n", repoDigest, err)
			continue
		}
		if isMember {
			continue
		}

		mc.GetAuthToken(repoName, repoDigest)
	}
}

/*
Ruft ein Auth Token für das gegebene Repo ab und startet ProcessManifest als Callback.
Params:
- repo: Name des Repositories
- digest: Digest des Tags
*/
func (mc *ManifestsCollector) GetAuthToken(repo string, digest string) {
	authURL := authBaseURL + repo + ":pull"
	task := &network.AuthRequestTask{
		AuthRequest: network.AuthRequest{
			AuthURL:     authURL,
			Headers:     nil,
			Cookies:     nil,
			Payload:     nil,
			Username:    mc.Username,
			AccessToken: mc.AccessToken,
			Repo:        repo,
			Digest:      digest,
		},
		ProcessResponse: mc.ProcessManifest,
	}
	mc.AuthTasks <- task
}

/*
Verarbeitet die Antwort des Manifest-Requests.
Params:
- resp: HTTP-Antwort des Manifest-Requests
- repo: Name des Repositories
- digest: Digest des Tags
*/
func (mc *ManifestsCollector) ProcessManifest(resp *http.Response, repo string, digest string) {

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Error getting manifest for %s: %s\n", repo, resp.Status)
		resp.Body.Close()
		return
	}
	var manifest struct {
		Layers []types.Layer `json:"layers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		fmt.Printf("Error decoding manifest response for %s: %v\n", repo, err)
		resp.Body.Close()
		return
	}
	resp.Body.Close()
	if err := mc.Save(manifest.Layers, repo, digest); err != nil {
		fmt.Printf("Error saving manifest layers for %s: %v\n", repo, err)
		return
	}
	if added, err := mc.Redis.AddToSet(mc.Context, "scanned_digests", digest); err != nil || added == -1 {
		fmt.Printf("Error adding digest to Redis set: %v\n", err)
		return
	}
}

/*
Schreibt die Layer-Informationen in JSONL-Datei
IO-Blockierung durch Mutex
*/
func (mc *ManifestsCollector) Save(layers []types.Layer, repo string, tagDigest string) error {
	mc.SaveMutex.Lock()
	defer mc.SaveMutex.Unlock()
	for _, layer := range layers {
		if err := mc.Writer.Write(map[string]any{
			"repo":         repo,
			"layer_digest": layer.Digest,
			"size":         layer.Size,
			"tag_digest":   tagDigest,
		}); err != nil {
			return fmt.Errorf("error writing layer to file: %v", err)
		}
	}
	return nil
}
