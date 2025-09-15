/*
Georg Heindl
Paralleler Collector für Docker Image Tags.
Führt Requests durch, um die Tags eines Repositories abzurufen.
Speichert die Tag-Informationen in einer JSONL-Datei.
Params:
- proxies: Liste von Proxy-URLs für die parallelen Clients (falls keine Proxies, dann Liste mit leeren String(s) übergeben)
- timeout: Timeout für HTTP Requests
- cookies: Cookies für die Requests (falls benötigt)
- writer: JSONL Writer zum Speichern der Tag-Informationen
- repofile: Pfad zur JSON-Datei mit Liste der Repositories
*/
package cmd

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/sissl0/DockerAnalysis/internal/network"
	"github.com/sissl0/DockerAnalysis/internal/types"
	"github.com/sissl0/DockerAnalysis/pkg/database"
)

const (
	tagBaseURL = "https://hub.docker.com/v2/repositories/"
)

type TagCollector struct {
	Writer    *database.RotatingJSONLWriter
	saveMutex sync.Mutex
	Tasks     chan *network.RequestTask
	Headers   map[string]any
	Redis     *database.RedisClient
}

func NewTagCollector(proxies []string, timeout int, cookies map[string]any, writer *database.RotatingJSONLWriter) (*TagCollector, error) {
	tasks := make(chan *network.RequestTask, 10)

	for i, proxy := range proxies {
		client, err := network.NewClient(proxy, 20, 180, time.Second*60)
		if err != nil {
			return nil, err
		}
		go client.Start(tasks, i)
	}

	headers := map[string]any{
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
		"Accept":          "application/json",
		"Accept-Language": "en-US,en;q=0.9",
		"Accept-Encoding": "gzip, deflate, br, zstd",
	}

	redis := database.NewRedisClient("localhost:6379", "", 0)
	if err := redis.WaitForRedis(); err != nil {
		return nil, fmt.Errorf("error connecting to Redis: %w", err)
	}
	return &TagCollector{
		Writer:    writer,
		saveMutex: sync.Mutex{},
		Tasks:     tasks,
		Headers:   headers,
		Redis:     redis,
	}, nil
}

/*
Liest Repos aus der JSON-Datei und fragt die Tags für jedes Repo an.
Blockierend, bis Tasks für 300 Sekunden leer sind.
*/
func (collector *TagCollector) Get_tags(repofile string) {
	file, err := os.Open(repofile)
	if err != nil {
		fmt.Printf("error opening repository file: %v\n", err)
		return
	}
	defer file.Close()
	repos := struct {
		Repos []string `json:"repos"`
	}{}
	if err := json.NewDecoder(file).Decode(&repos); err != nil {
		fmt.Printf("Error decoding repository file: %v\n", err)
		return
	}

	semaphore := make(chan struct{}, 3)
	for _, repo := range repos.Repos {
		// Nur Tags downloaden, die auf neue Version hinweisen (nicht nur base image variationen)
		url := fmt.Sprintf("%s%s/tags?platforms=true&page_size=100&page=1&ordering=last_updated&name=", tagBaseURL, repo)
		go func() {
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			collector.ProcessTag(url, repo)
		}()
	}
	for len(collector.Tasks) > 0 {
		time.Sleep(300 * time.Second)
	}
	close(collector.Tasks)
}

/*
Rekursiver Aufruf, um alle Seiten der Tags eines Repos abzufragen.
Keine einheitlichen Versionsnamen, daher werden nur Tags berücksichtigt, deren letzte Änderung
mindestens 7 Tage auseinander liegt.
*/

func (collector *TagCollector) ProcessTag(url string, repo string) {
	isMember, err := collector.Redis.IsMember(context.Background(), "scanned_tags", url)
	if err != nil {
		fmt.Printf("error checking URL %s in Redis: %v\n", url, err)
		return
	}
	if isMember {
		return
	}
	task := &network.RequestTask{
		Request: network.Request{
			URL:     url,
			Headers: collector.Headers,
			Cookies: nil,
			Payload: nil,
		},
		ProcessResponse: func(resp *http.Response) {
			if resp.StatusCode != 200 {
				fmt.Printf("Fehler beim Abrufen der URL %s: %d\n", url, resp.StatusCode)
				return
			}
			var results struct {
				Results []types.TagInfo `json:"results"`
				Count   float64         `json:"count"`
				Next    string          `json:"next"`
			}

			if resp.Header.Get("Content-Encoding") == "gzip" {
				reader, err := gzip.NewReader(resp.Body)
				if err != nil {
					fmt.Printf("Fehler beim Erstellen des GZIP-Readers für %s: %v, %s\n", url, err, resp.Header.Get("Content-Encoding"))
					return
				}
				defer reader.Close()

				if err := json.NewDecoder(reader).Decode(&results); err != nil {
					fmt.Printf("Fehler beim Dekodieren der JSON-Antwort für %s: %v\n", url, err)
					return
				}
			} else {
				if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
					fmt.Printf("Fehler beim Dekodieren der JSON-Antwort für %s: %v\n", url, err)
					return
				}
			}
			defer resp.Body.Close()
			prev_last_pushed := time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
			for _, tagInfo := range results.Results {
				last_pushed, err := time.Parse(time.RFC3339, tagInfo.LastPushed)
				if err != nil {
					fmt.Printf("Error parsing last_pushed time for %s: %v\n", repo, err)
					continue
				}

				if prev_last_pushed.Sub(last_pushed) < time.Hour*24*7 {
					continue
				}
				for _, image := range tagInfo.Images {
					if image.Architecture == "amd64" && image.OS == "linux" {
						if err := collector.Save(image, repo); err != nil {
							fmt.Printf("Error saving image: %v\n", err)
						}
					}
				}
				prev_last_pushed = last_pushed
			}
			if added, err := collector.Redis.AddToSet(context.Background(), "scanned_tags", url); err != nil || added == -1 {
				fmt.Printf("Fehler beim Hinzufügen der URL %s zu Redis: %v\n", url, err)
			}
			if results.Next != "" {
				collector.ProcessTag(results.Next, repo)
				return
			}
		},
	}
	collector.Tasks <- task
}

/*
Schreibt die Tag-Informationen in JSONL-Datei
IO-Blockierung durch Mutex
*/
func (c *TagCollector) Save(image types.Image, repo_name string) error {
	c.saveMutex.Lock()
	defer c.saveMutex.Unlock()
	if err := c.Writer.Write(map[string]any{
		"repo_name":    repo_name,
		"architecture": image.Architecture,
		"os":           image.OS,
		"last_pulled":  image.LastPulled,
		"last_pushed":  image.LastPushed,
		"size":         image.Size,
		"digest":       image.Digest,
		"status":       image.Status,
	}); err != nil {
		return fmt.Errorf("error writing image to file: %w", err)
	}

	return nil
}
