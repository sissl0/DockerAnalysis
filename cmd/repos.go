package cmd

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sissl0/DockerAnalysis/internal/network"
	"github.com/sissl0/DockerAnalysis/ltr"
	"github.com/sissl0/DockerAnalysis/pkg/database"
)

const (
	baseURL      = "https://hub.docker.com/v2/search/repositories/"
	characterSet = "abcdefghijklmnopqrstuvwxyz0123456789" // Character Set ohne -,_ da nicht in usernames erlaubt
)

type Collector struct {
	Writer    *database.RotatingJSONLWriter
	saveMutex sync.Mutex // Mutex für thread-sicheren Zugriff
	Tasks     chan *network.RequestTask
	Headers   map[string]any
	Redis     *database.RedisClient
	ApiClient *ltr.LTRClient
}

func NewCollector(proxies []string, timeout int, cookies map[string]any, writer *database.RotatingJSONLWriter) (*Collector, error) {
	tasks := make(chan *network.RequestTask, 5*len(proxies))

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

	apiClient, err := ltr.NewLTRClient()
	if err != nil {
		fmt.Printf("Error creating LTR client: %v\n", err)
		return nil, err
	}

	return &Collector{
		Writer:    writer,
		saveMutex: sync.Mutex{},
		Tasks:     tasks,
		Headers:   headers,
		Redis:     redis,
		ApiClient: apiClient,
	}, nil
}

func (collector *Collector) GetWeights() {
	ctx := context.Background()
	for _, c1 := range characterSet {
		for _, c2 := range characterSet {
			for _, c3 := range characterSet {
				query := fmt.Sprintf("%c%c%c", c1, c2, c3)
				url := fmt.Sprintf("%s?query=%c%c%c&page=1&page_size=100", baseURL, c1, c2, c3)
				isMember, err := collector.Redis.IsMember(ctx, "scanned_queries", url)
				if err != nil {
					fmt.Printf("Fehler beim Überprüfen der URL %s in Redis: %v\n", url, err)
					continue
				}
				if isMember {
					continue
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
							Results []ltr.Repo `json:"results"`
							Count   float64    `json:"count"`
							Next    string     `json:"next"`
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
						if err := collector.Save(results.Results, query); err != nil {
							fmt.Printf("Fehler beim Speichern der Ergebnisse von %s: %v\n", url, err)
						}
						if added, err := collector.Redis.AddToSet(ctx, "scanned_queries", url); err != nil || added == -1 {
							fmt.Printf("Fehler beim Hinzufügen der URL %s zu Redis: %v\n", url, err)
						}
					},
				}
				collector.Tasks <- task
			}
		}
	}
	for len(collector.Tasks) > 0 {
		time.Sleep(1 * time.Second)
	}
	close(collector.Tasks)
}

func (c *Collector) GetRepos() {
	ctx := context.Background()
	for _, c1 := range characterSet {
		for _, c2 := range characterSet {
			for _, c3 := range characterSet {
				for _, c4 := range characterSet {
					query := fmt.Sprintf("%c%c%c%c", c1, c2, c3, c4)
					url := fmt.Sprintf("%s?query=%s&page=1&page_size=100", baseURL, query)
					go c.ProcessQuery(url, query, ctx)
				}
			}
		}
	}
	for len(c.Tasks) > 0 {
		time.Sleep(1 * time.Second)
	}
	close(c.Tasks)
}

func (c *Collector) ProcessQuery(url string, query string, ctx context.Context) {
	isMember, err := c.Redis.IsMember(ctx, "scanned_queries", url)
	if err != nil {
		fmt.Printf("Fehler beim Überprüfen der URL %s in Redis: %v\n", url, err)
		return
	}
	if isMember {
		// for _, char := range characterSet {
		// 	newQuery := fmt.Sprintf("%s%c", query, char)
		// 	newUrl := fmt.Sprintf("%s?query=%s&page=1&page_size=100", baseURL, newQuery)
		// 	c.ProcessQuery(newUrl, newQuery, ctx)
		// }
		return
	}

	task := &network.RequestTask{
		Request: network.Request{
			URL:     url,
			Headers: c.Headers,
			Cookies: nil,
			Payload: nil,
		},
		ProcessResponse: func(resp *http.Response) {
			if resp.StatusCode != 200 {
				fmt.Printf("Fehler beim Abrufen der URL %s: %d\n", url, resp.StatusCode)
				return
			}
			var results struct {
				Results []ltr.Repo `json:"results"`
				Count   float64    `json:"count"`
				Next    string     `json:"next"`
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
			if err := c.Save(results.Results, query); err != nil {
				fmt.Printf("Fehler beim Speichern der Ergebnisse von %s: %v\n", url, err)
			}
			if added, err := c.Redis.AddToSet(ctx, "scanned_queries", url); err != nil || added == -1 {
				fmt.Printf("Fehler beim Hinzufügen der URL %s zu Redis: %v\n", url, err)
			}

			switch results.Count {
			case 10000:
				// Explore Query
				res, err := c.ApiClient.Predict(query, results.Results)
				if err != nil {
					fmt.Printf("Error during prediction: %v\n", err)
					return
				}
				if !res {
					// Dummy potentially in next page, Scrape next page
					nextPage := results.Next
					if nextPage != "" {
						go c.ProcessQuery(nextPage, query, ctx)
					}
				}
				for _, char := range characterSet {
					newQuery := fmt.Sprintf("%s%c", query, char)
					newUrl := fmt.Sprintf("%s?query=%s&page=1&page_size=100", baseURL, newQuery)
					c.ProcessQuery(newUrl, newQuery, ctx)
				}
			case 0:
				return
			default:
				// Traverse Pages
				nextPage := results.Next
				if nextPage != "" {
					c.ProcessQuery(nextPage, query, ctx)
				}
			}

			// //Search-Logic
			// if results.Count > 0 {
			// 	//Check if standard repo could come after rank 100
			// 	if results.Count > 100 {
			// 		res, err := c.ApiClient.Predict(query, results.Results)
			// 		if err != nil {
			// 			fmt.Printf("Error during prediction: %v\n", err)
			// 			return
			// 		}
			// 		if !res {
			// 			// Dummy potentially in next page, Scrape next page
			// 			nextPage := results.Next
			// 			if nextPage != "" {
			// 				go c.ProcessQuery(nextPage, query, ctx)
			// 			}
			// 		}
			// 	}
			// 	for _, char := range characterSet {
			// 		newQuery := fmt.Sprintf("%s%c", query, char)
			// 		newUrl := fmt.Sprintf("%s?query=%s&page=1&page_size=100", baseURL, newQuery)
			// 		go c.ProcessQuery(newUrl, newQuery, ctx)
			// 	}
			// } else {
			// 	return
			// }

		},
	}

	c.Tasks <- task

}

func (c *Collector) Save(results []ltr.Repo, query string) error {
	c.saveMutex.Lock()         // Sperre den Mutex
	defer c.saveMutex.Unlock() // Gib den Mutex frei, sobald die Methode beendet ist

	breakRecord := map[string]any{
		"repo_name": "#/#",
		"query":     query,
	}
	if err := c.Writer.Write(breakRecord); err != nil {
		return err
	}

	for _, repo := range results {

		record := map[string]any{
			"repo_name":         repo.RepoName,
			"star_count":        repo.StarCount,
			"pull_count":        repo.PullCount,
			"is_official":       repo.IsOfficial,
			"is_automated":      repo.IsAutomated,
			"repo_owner":        repo.RepoOwner,
			"short_description": repo.ShortDescription,
		}
		if err := c.Writer.Write(record); err != nil {
			return err
		}
	}
	return nil
}
