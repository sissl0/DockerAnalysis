package ltr_test

import (
	"encoding/json"
	"testing"

	"github.com/sissl0/DockerAnalysis/ltr"
	"github.com/stretchr/testify/require"
)

func TestLTRClient_Predict(t *testing.T) {
	client, err := ltr.NewLTRClient()
	require.NoError(t, err)
	resp, err := client.Client.Network_Get("https://hub.docker.com/v2/search/repositories/?query=ab&page=1&page_size=100", nil, nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	var results struct {
		Results []ltr.Repo `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	// Ensure results.Results is not empty before proceeding
	if len(results.Results) == 0 {
		t.Fatalf("No results found in the response")
	}

	client.Predict("aab", results.Results[len(results.Results)-29:])

	t.Fatal("Test not implemented yet")
}
