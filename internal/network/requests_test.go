package network_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sissl0/DockerAnalysis/internal/network"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	client, err := network.NewClient("", 10, 0, time.Duration(0))
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, 10*time.Second, client.HttpClient.Timeout)
}

func TestProxyClient(t *testing.T) {
	proxy := "http://localhost:8080"
	clientWithProxy, err := network.NewClient(proxy, 10, 0, time.Duration(0))
	assert.NoError(t, err)
	assert.NotNil(t, clientWithProxy)
	assert.NotNil(t, clientWithProxy.HttpClient.Transport)
}

func TestNetwork_Get(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "success"}`))
	}))
	defer server.Close()

	client, err := network.NewClient("", 10, 0, time.Duration(0))
	assert.NoError(t, err)

	headers := map[string]any{"Content-Type": "application/json"}
	cookies := map[string]any{"session": "test-session"}
	resp, err := client.Network_Get(server.URL, headers, cookies)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestProxyNetwork_Get(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "success"}`))
	}))
	defer server.Close()

	proxy := ""
	assert.NotEmpty(t, proxy)
	client, err := network.NewClient(proxy, 10, 0, time.Duration(0))
	assert.NoError(t, err)

	headers := map[string]any{"Content-Type": "application/json"}
	cookies := map[string]any{"session": "test-session"}
	resp, err := client.Network_Get(server.URL, headers, cookies)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusForbidden)
}

func TestNetwork_Post(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "test-session", r.Cookies()[0].Value)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"message": "created"}`))
	}))
	defer server.Close()

	client, err := network.NewClient("", 10, 0, time.Duration(0))
	assert.NoError(t, err)

	headers := map[string]any{"Content-Type": "application/json"}
	cookies := map[string]any{"session": "test-session"}
	payload := map[string]any{"key": "value"}
	resp, err := client.Network_Post(server.URL, payload, headers, cookies)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestProxyNetwork_Post(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "test-session", r.Cookies()[0].Value)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"message": "created"}`))
	}))
	defer server.Close()

	proxy := ""
	assert.NotEmpty(t, proxy)

	client, err := network.NewClient(proxy, 10, 0, time.Duration(0))
	assert.NoError(t, err)

	headers := map[string]any{"Content-Type": "application/json"}
	cookies := map[string]any{"session": "test-session"}
	payload := map[string]any{"key": "value"}
	resp, err := client.Network_Post(server.URL, payload, headers, cookies)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusForbidden)
}

func TestClient_Start(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "success"}`))
	}))
	defer server.Close()

	client, err := network.NewClient("", 10, 0, 0)
	assert.NoError(t, err)

	tasks := make(chan *network.RequestTask, 1)

	// Füge eine Task hinzu
	tasks <- &network.RequestTask{
		Request: network.Request{
			URL: server.URL,
			Headers: map[string]any{
				"Content-Type": "application/json",
			},
			Cookies: map[string]any{
				"session": "test-session",
			},
		},
		ProcessResponse: func(resp *http.Response) {
			assert.NotNil(t, resp)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		},
	}

	close(tasks)

	// Starte die Verarbeitung der Tasks
	client.Start(tasks, 0)
}
