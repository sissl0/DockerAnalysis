package network

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	authBaseURL          = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:"
	manifestBaseURL      = "https://registry-1.docker.io/v2/"
	manifestEndpoint     = "/manifests/"
	manifestAcceptHeader = "application/vnd.docker.distribution.manifest.v2+json"
)

type Client struct {
	HttpClient *http.Client
	delay      time.Duration
}

func NewClient(proxy string, timeout int, ratelimit uint16, timelimit time.Duration) (*Client, error) {
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
	delay := time.Duration(0)
	if ratelimit != 0 {
		delay = timelimit / time.Duration(ratelimit)
	}
	fmt.Println(delay)
	client := &Client{
		HttpClient: http_client,
		delay:      delay,
	}
	return client, nil
}

func (client *Client) AuthClientStart(tasks <-chan *AuthRequestTask, num int) {
	for authRequestTask := range tasks {
		sendTime := time.Now()

		resp, err := client.AuthNetwork_Get(
			authRequestTask.AuthRequest.AuthURL,
			authRequestTask.AuthRequest.Headers,
			authRequestTask.AuthRequest.Cookies,
			authRequestTask.AuthRequest.Payload,
			authRequestTask.AuthRequest.Username,
			authRequestTask.AuthRequest.AccessToken,
		)
		if err != nil {
			fmt.Printf("Fehler beim Abrufen der URL %s: %v\n", authRequestTask.AuthRequest.AuthURL, err)
			continue
		}

		var token struct {
			Token string `json:"token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
			fmt.Printf("Error decoding auth token response: %v\n", err)
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		manifestURL := manifestBaseURL + authRequestTask.AuthRequest.Repo + manifestEndpoint + authRequestTask.AuthRequest.Digest
		headers := map[string]any{
			"Authorization": "Bearer " + token.Token,
			"Accept":        manifestAcceptHeader,
		}

		resp, err = client.Network_Get(manifestURL, headers, nil)
		if err != nil {
			fmt.Printf("Error getting manifest for %s: %v\n", manifestURL, err)
			goto sleep
		}
		go authRequestTask.ProcessResponse(resp, authRequestTask.AuthRequest.Repo, authRequestTask.AuthRequest.Digest)

	sleep:
		remaining := ""
		if resp != nil {
			remaining = resp.Header.Get("X-Ratelimit-Remaining")
			if remaining == "0" || resp.StatusCode == http.StatusTooManyRequests {
				reset := resp.Header.Get("X-Ratelimit-Reset")
				resetTime, err := time.Parse(time.RFC1123, reset)
				if err != nil {
					time.Sleep(60 * time.Second)
				} else {
					time.Sleep(time.Until(resetTime))
				}
			}
		}
		if d := client.delay - time.Since(sendTime); d > 0 {
			time.Sleep(d)
		}
	}
}

func (client *Client) AuthNetwork_Get(url string, headers map[string]any, cookies map[string]any, payload map[string]any, username string, accesstoken string) (*http.Response, error) {
	// Fetch the data with given Headers
	req, err := http.NewRequest("GET", url, nil)
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

	if payload != nil {
		query := req.URL.Query()
		for key, value := range payload {
			query.Set(key, fmt.Sprintf("%v", value))
		}
		req.URL.RawQuery = query.Encode()
	}

	if username != "" && accesstoken != "" {
		req.SetBasicAuth(username, accesstoken)
	}

	resp, err := client.HttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Ausführen des Requests: %v", err)
	}

	return resp, nil
}

func (client *Client) Start(tasks <-chan *RequestTask, num int) {
	for requestTask := range tasks {
		sendTime := time.Now()
		resp, err := client.Network_Get(requestTask.Request.URL, requestTask.Request.Headers, requestTask.Request.Cookies)
		if err != nil {
			fmt.Printf("Fehler beim Abrufen der URL %s: %v\n", requestTask.Request.URL, err)
			if d := client.delay - time.Since(sendTime); d > 0 {
				time.Sleep(d)
			}
			continue
		}
		go requestTask.ProcessResponse(resp)

		remaining := resp.Header.Get("X-Ratelimit-Remaining")
		if remaining == "0" || resp.StatusCode == http.StatusTooManyRequests {
			reset := resp.Header.Get("X-Ratelimit-Reset")
			resetTime, err := time.Parse(time.RFC1123, reset)
			if err != nil {
				time.Sleep(60 * time.Second)
			} else {
				time.Sleep(time.Until(resetTime))
			}
		}
		if d := client.delay - time.Since(sendTime); d > 0 {
			time.Sleep(d)
		}
	}
}

func (client *Client) Network_Get(url string, headers map[string]any, cookies map[string]any) (*http.Response, error) {
	// Fetch the data with given Headers
	req, err := http.NewRequest("GET", url, nil)
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

	return resp, nil
}

func (client *Client) Network_Post(url string, payload map[string]any, headers map[string]any, cookies map[string]any) (*http.Response, error) {
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

	return resp, nil
}
