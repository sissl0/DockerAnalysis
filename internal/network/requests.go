package network

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	HttpClient *http.Client
}

func NewClient(proxy string, timeout int) (*Client, error) {
	http_client := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}
	if proxy != "" {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, fmt.Errorf("ungültige Proxy-URL: %v", err)
		}
		http_client.Transport = &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		}
	}
	client := &Client{
		HttpClient: http_client,
	}
	return client, nil
}

func (client *Client) Network_Get(url string, headers map[string]any) (*http.Response, error) {
	// Fetch the data with given Headers
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Erstellen des Requests: %v", err)
	}
	req.Host = req.URL.Host

	for key, value := range headers {
		req.Header.Set(key, fmt.Sprintf("%v", value))
	}

	resp, err := client.HttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Ausführen des Requests: %v", err)
	}
	return resp, nil
}

func (client *Client) Network_Post(url string, payload map[string]any, headers map[string]any, cookies map[string]any) (map[string]any, error) {
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Kodieren des Payloads: %v", err)
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(jsonPayload)))
	if err != nil {
		return nil, fmt.Errorf("fehler beim Erstellen des Requests: %v", err)
	}
	req.Host = req.URL.Host
	for key, value := range headers {
		req.Header.Set(key, fmt.Sprintf("%v", value))
	}

	for key, value := range cookies {
		req.AddCookie(&http.Cookie{
			Name:  key,
			Value: fmt.Sprintf("%v", value),
		})
	}

	resp, err := client.HttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Ausführen des Requests: %v", err)
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode)
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Lesen der Antwort: %v", err)
	}
	var result map[string]any
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Dekodieren der JSON-Antwort: %v, %v", err, string(response))
	}

	return result, nil
}
