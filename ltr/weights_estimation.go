package ltr

import (
	"strings"
	"unicode"
)

// Relevanzmodell approximieren
func Categorize(query string, reponame string) float64 {

	if !strings.Contains(reponame, query) {
		return 0
	}

	username := ""
	imagename := ""
	parts := strings.Split(reponame, "/")
	if len(parts) > 0 {
		username = parts[0]
	}
	if len(parts) > 1 {
		imagename = parts[1]
	}

	if username == query || imagename == query {
		return 1
	}
	if strings.Contains(reponame, "-"+query+"-") || strings.Contains(reponame, "_"+query+"_") {
		return 0.8
	}
	if strings.HasPrefix(username, query) || strings.HasPrefix(imagename, query) {
		return 0.6
	}
	if isHighlighted(query, username) || isHighlighted(query, imagename) {
		return 0.4
	}
	if strings.Contains(reponame, query) {
		return 0.2
	}

	return 0
}

func isHighlighted(query string, name string) bool {
	q_type := 0
	if isNumeric(query) {
		q_type = 1
	} else if isAlphabetic(query) {
		q_type = 2
	}
	if q_type == 0 {
		return false
	}

	q_idx := strings.Index(name, query)
	if q_idx == -1 {
		return false
	}
	if q_idx > 0 {
		prev := rune(name[q_idx-1])
		if (unicode.IsLetter(prev) && q_type == 2) || unicode.IsNumber(prev) && q_type == 1 {
			return false
		}
	}
	if q_idx+len(query) < len(name) {
		next := rune(name[q_idx+len(query)])
		if (unicode.IsLetter(next) && q_type == 2) || (unicode.IsNumber(next) && q_type == 1) {
			return false
		}
	}
	return true
}

func isNumeric(s string) bool {
	for _, char := range s {
		if !unicode.IsDigit(char) {
			return false
		}
	}
	return true
}

func isAlphabetic(s string) bool {
	for _, char := range s {
		if !unicode.IsLetter(char) {
			return false
		}
	}
	return true
}
