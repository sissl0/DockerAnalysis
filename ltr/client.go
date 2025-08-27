package ltr

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/agnivade/levenshtein"
	"github.com/sissl0/DockerAnalysis/internal/network"
)

var (
	specialChars = []string{"-", "_", ".", "/", ":", ";", "|", "&", "*", "#", "@", "!"}
)

type Repo struct {
	RepoName         string `json:"repo_name"`
	ShortDescription string `json:"short_description"`
	StarCount        int    `json:"star_count"`
	PullCount        int    `json:"pull_count"`
	RepoOwner        string `json:"repo_owner"`
	IsOfficial       bool   `json:"is_official"`
	IsAutomated      bool   `json:"is_automated"`
}

type LTRClient struct {
	Client  *network.Client
	headers map[string]any
}

func NewLTRClient() (*LTRClient, error) {
	client, err := network.NewClient("", 10, 0, time.Duration(0))
	if err != nil {
		fmt.Printf("Error creating network client: %v\n", err)
		return nil, err
	}
	return &LTRClient{
		Client: client,
		headers: map[string]any{
			"accept": "application/json",
		},
	}, nil
}
func (ltrcli *LTRClient) Predict(query string, repos []Repo) (bool, error) {
	//Dummy Repo
	dummyRepo := Repo{
		RepoName:         query,
		ShortDescription: "",
		StarCount:        0,
		PullCount:        0,
		RepoOwner:        "",
		IsOfficial:       false,
		IsAutomated:      false,
	}
	dummyRank_org := len(repos)
	repos = append(repos, dummyRepo)
	payload := scaleRepos(query, repos)

	resp, err := ltrcli.Client.Network_Post("http://localhost:8000/predict", payload, ltrcli.headers, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	var results []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return false, fmt.Errorf("error decoding JSON response: %v", err)
	}
	// Sort results by score in descending order
	sort.Slice(results, func(i, j int) bool {
		return results[i]["score"].(float64) > results[j]["score"].(float64)
	})

	spearmanCorr, dummyRank := spearmanCorrelation(results, dummyRank_org)
	if dummyRank <= int(math.Floor(spearmanCorr*float64(len(results)))) {
		return true, nil
	} else {
		return false, nil
	}
}
func spearmanCorrelation(results []map[string]any, dummyRank_org int) (float64, int) {
	var rankDiffSum float64
	var validCount float64
	var dummyRank int = len(results) + 1
	for i, result := range results {
		predictedRank := int(result["rank"].(float64))
		actualRank := i
		if predictedRank == dummyRank_org {
			dummyRank = actualRank
			continue
		}
		rankDiff := predictedRank - actualRank
		rankDiffSum += float64(rankDiff * rankDiff)
		validCount++
	}

	spearmanCorrelation := math.Max(1-(6*rankDiffSum)/(validCount*(validCount*validCount-1)), 0)
	return spearmanCorrelation, dummyRank
}

func scaleRepos(query string, repos []Repo) map[string]any {
	scaledRepos := make([]map[string]any, len(repos))
	for i, repo := range repos {
		queryLower := strings.ToLower(query)
		repoParts := strings.Split(strings.ToLower(repo.RepoName), "/")
		repo.RepoOwner = repoParts[0]
		if len(repoParts) > 1 {
			repo.RepoName = repoParts[1]
		} else {
			repo.RepoName = ""
		}
		repo.ShortDescription = strings.ToLower(repo.ShortDescription)

		scaled_repo := map[string]any{
			"repo_name":               repo.RepoName,
			"rank":                    i, // Placeholder for rank
			"star_count":              math.Log1p(float64(repo.StarCount)),
			"pull_count":              math.Log1p(float64(repo.PullCount)),
			"is_official":             boolToInt(repo.IsOfficial),
			"is_automated":            boolToInt(repo.IsAutomated),
			"significant_levenshtein": get_significant_levenshtein(queryLower, repo),
			"significant_position":    get_significant_position(queryLower, repo),
			"significant_category":    get_significant_category(queryLower, repo),
			"significant_jaccard":     get_significant_jaccard(queryLower, repo),
			"is_standalone":           get_is_standalone(queryLower, repo),
			"query_find":              get_query_find(queryLower, repo),
		}
		scaledRepos[i] = scaled_repo
	}
	return map[string]any{
		"repos": scaledRepos,
	}
}

func get_significant_position(query string, repo Repo) float64 {
	repo_name_position := get_relative_position(query, repo.RepoName)
	repo_owner_position := get_relative_position(query, repo.RepoOwner)
	short_description_position := get_relative_position(query, repo.ShortDescription)

	return math.Max(repo_name_position, math.Max(repo_owner_position, short_description_position))
}

func get_relative_position(query string, anchor string) float64 {
	if len(anchor) == 0 {
		return 0.0
	}
	position := strings.Index(anchor, query)
	if position == -1 {
		return 0.0
	}
	return 1 - float64(position)/float64(len(anchor))
}

func get_query_find(query string, repo Repo) int {
	query_find := 0
	if strings.Contains(repo.RepoName, query) {
		query_find += 1
	}
	if strings.Contains(repo.RepoOwner, query) {
		query_find += 2
	}
	if strings.Contains(repo.ShortDescription, query) {
		query_find += 4
	}

	return query_find
}

func get_is_standalone(query string, repo Repo) int {
	isStandalone := 0
	if query == repo.RepoName || query == repo.RepoOwner || query == repo.ShortDescription {
		isStandalone = 1
	}
	return isStandalone
}

func get_significant_levenshtein(query string, repo Repo) float64 {
	max_len := math.Max(float64(len(query)), float64(len(repo.RepoName)))
	if max_len == 0 {
		return 1.0
	}
	imps := make([]float64, 0, 3)
	if strings.Contains(repo.RepoName, query) {
		repo_name_levenshtein := 1 - float64(levenshtein.ComputeDistance(query, repo.RepoName))/max_len
		imps = append(imps, repo_name_levenshtein)
	}
	max_len = math.Max(float64(len(query)), float64(len(repo.RepoOwner)))
	if strings.Contains(repo.RepoOwner, query) {
		repo_owner_levenshtein := 1 - float64(levenshtein.ComputeDistance(query, repo.RepoOwner))/max_len
		imps = append(imps, repo_owner_levenshtein)
	}
	max_len = math.Max(float64(len(query)), float64(len(repo.ShortDescription)))
	if strings.Contains(repo.ShortDescription, query) {
		short_description_levenshtein := 1 - float64(levenshtein.ComputeDistance(query, repo.ShortDescription))/max_len
		imps = append(imps, short_description_levenshtein)
	}

	if len(imps) == 0 {
		return 0.0
	}
	sort.Float64s(imps)
	return imps[len(imps)-1]
}

func get_significant_jaccard(query string, repo Repo) float64 {
	jaccard_repo_name := jaccardChars(query, repo.RepoName)
	jaccard_repo_owner := jaccardChars(query, repo.RepoOwner)
	jaccard_short_description := jaccardChars(query, repo.ShortDescription)

	return math.Max(jaccard_repo_name, math.Max(jaccard_repo_owner, jaccard_short_description))
}

func get_significant_text_share(query string, repo Repo) float64 {
	repo_name_share := 0.0
	if strings.Contains(repo.RepoName, query) {
		repo_name_share = float64(len(query)) / float64(len(repo.RepoName))
	}
	repo_owner_share := 0.0
	if strings.Contains(repo.RepoOwner, query) {
		repo_owner_share = float64(len(query)) / float64(len(repo.RepoOwner))
	}

	short_description_share := 0.0
	if strings.Contains(repo.ShortDescription, query) {
		short_description_share = float64(len(query)) / float64(len(repo.ShortDescription))
	}

	return math.Max(repo_name_share, math.Max(repo_owner_share, short_description_share))
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func jaccardChars(a, b string) float64 {
	setA := make(map[rune]bool)
	setB := make(map[rune]bool)

	for _, char := range a {
		setA[char] = true
	}
	for _, char := range b {
		setB[char] = true
	}

	intersection := 0
	union := make(map[rune]bool)

	for char := range setA {
		union[char] = true
		if setB[char] {
			intersection++
		}
	}
	for char := range setB {
		union[char] = true
	}

	if len(union) == 0 {
		return 0.0
	}
	return float64(intersection) / float64(len(union))
}

func get_significant_category(query string, repo Repo) float64 {
	repo_name_category := get_repo_name_category(query, repo.RepoName)
	repo_owner_category := get_repo_owner_category(query, repo.RepoOwner)
	short_description_category := get_short_descr_category(query, repo.ShortDescription)

	return math.Max(repo_name_category, math.Max(repo_owner_category, short_description_category))
}

func get_repo_name_category(query string, repo_name string) float64 {
	category := 0.0
	if query == repo_name {
		category = 1.0
	} else if strings.Contains(repo_name, "-"+query+"-") || strings.Contains(repo_name, "_"+query+"_") {
		category = 0.75
	} else if isHighlighted(query, repo_name) {
		category = 0.5
	} else if strings.Contains(repo_name, query) {
		category = 0.25
	}

	return category
}

func get_repo_owner_category(query string, repo_owner string) float64 {
	category := 0.0
	if query == repo_owner {
		category = 1.0
	} else if isHighlighted(query, repo_owner) {
		category = 0.66
	} else if strings.Contains(repo_owner, query) {
		category = 0.33
	}
	return category
}

func highlightScoreShortDescription(query string, description string) int {
	if description == "" || query == "" {
		return 0
	}

	score := 0

	for _, char := range specialChars {
		escapedChar := regexp.QuoteMeta(char)
		escapedQuery := regexp.QuoteMeta(query)

		// Both sides have the same special character (possibly with spaces)
		patternBoth := fmt.Sprintf(`%s\s*%s\s*%s`, escapedChar, escapedQuery, escapedChar)
		matchedBoth, _ := regexp.MatchString(patternBoth, description)
		if matchedBoth {
			return 2
		}

		// Only left of the special character
		patternLeft := fmt.Sprintf(`%s\s*%s`, escapedChar, escapedQuery)
		// Only right of the special character
		patternRight := fmt.Sprintf(`%s\s*%s`, escapedQuery, escapedChar)

		matchedLeft, _ := regexp.MatchString(patternLeft, description)
		matchedRight, _ := regexp.MatchString(patternRight, description)

		if matchedLeft || matchedRight {
			score = int(math.Max(float64(score), 1))
		}
	}

	return score
}

func get_short_descr_category(query string, short_description string) float64 {
	sHighlight := highlightScoreShortDescription(query, short_description)

	category := 0.0
	if query == short_description {
		category = 1.0
	} else if strings.Contains(" "+short_description+" ", " "+query+" ") {
		category = 0.833
	} else if sHighlight == 2 {
		category = 0.66
	} else if sHighlight == 1 {
		category = 0.5
	} else if isHighlighted(query, short_description) {
		category = 0.333
	} else if strings.Contains(short_description, query) {
		category = 0.166
	}

	return category
}
