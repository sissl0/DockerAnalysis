package network

import "net/http"

type Request struct {
	URL     string
	Headers map[string]any
	Cookies map[string]any
	Payload map[string]any
}

type RequestTask struct {
	Request         Request
	ProcessResponse func(*http.Response)
}

type AuthRequest struct {
	AuthURL     string
	Headers     map[string]any
	Cookies     map[string]any
	Payload     map[string]any
	Username    string
	AccessToken string
	Digest      string
	Repo        string
}

type AuthRequestTask struct {
	AuthRequest     AuthRequest
	ProcessResponse func(*http.Response, string, string)
}
