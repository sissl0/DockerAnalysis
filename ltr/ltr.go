/*
Georg Heindl
Skaliert die Features und exportiert sie in eine CSV-Datei für Learning to Rank.
*/

package ltr

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/sissl0/DockerAnalysis/pkg/database"
)

const (
	characterSet = "abcdefghijklmnopqrstuvwxyz0123456789"
)

func Run() {
	results, err := LoadData("repos/repos__0.jsonl")
	if err != nil {
		fmt.Println("Error loading data:", err)
		return
	}
	//err = ExportToCSV(results, "repos/learningRepos_descr.csv")
	err = scaled_export_to_csv(results, "repos/learningRepos_descr_go_scaled.csv")
	//err = logResDataset(results, "repos/logResDS.csv")
	if err != nil {
		fmt.Println("Error exporting to CSV:", err)
		return
	}
}

func LoadData(filename string) (map[string][]map[string]any, error) {
	reader, err := database.NewJSONLReader(filename)
	redis := database.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		fmt.Println("Error opening JSONL reader:", err)
		return nil, err
	}

	ctx := context.Background()
	var total_queries uint64
	var total_repos uint64
	var unique_repos uint64
	var curr_query string
	results := make(map[string][]map[string]any)
	for reader.Scanner.Scan() {
		line := reader.Scanner.Text()
		var record map[string]any
		err := json.Unmarshal([]byte(line), &record)
		if err != nil {
			fmt.Println("Error unmarshalling JSON:", err)
			continue
		}
		if record["repo_name"] == "#/#" {
			total_queries++
			curr_query = record["query"].(string)
			results[curr_query] = make([]map[string]any, 0)
			continue
		}

		total_repos++

		repo_name, ok := record["repo_name"].(string)
		if !ok {
			fmt.Println("Invalid repo_name format:", record)
			continue
		}
		isMember, err := redis.IsMember(ctx, "scanned_repos", repo_name)
		if err != nil {
			fmt.Println("Error checking membership in Redis:", err)
			return nil, err
		}
		if !isMember {
			unique_repos++
			results[curr_query] = append(results[curr_query], record)
			if added, err := redis.AddToSet(ctx, "scanned_repos", record["repo_name"].(string)); err != nil || added == -1 {
				fmt.Println("Error adding member to Redis:", err)
				return nil, err
			}
		}

		if len(results[curr_query]) > 100 {
			fmt.Println("Warning: More than 100 repos for query:", curr_query)
		}
	}
	fmt.Println("Total queries:", total_queries)
	fmt.Println("Total repos:", total_repos)
	fmt.Println("Unique repos:", unique_repos)
	return results, nil
}

/*
Deprecated
*/
func logResDataset(results map[string][]map[string]any, filepath string) error {

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Schreibe Header
	writer.Write([]string{"label", "parent_entry_num", "avg_lehenshtein", "avg_text_share"})

	// Schreibe Daten
	for query, repos := range results {
		if len(repos) == 0 {
			continue
		}
		label := 0
		for char := range characterSet {
			child_query := fmt.Sprintf("%s%c", query, char)
			if _, exists := results[child_query]; !exists {
				continue
			}
			if len(results[child_query]) > 0 {
				allReposMatch := true
				for _, childRepo := range results[child_query] {
					childRepoName := childRepo["repo_name"].(string)
					found := false
					for _, parentRepo := range repos {
						if parentRepo["repo_name"].(string) == childRepoName {
							found = true
							break
						}
					}
					if !found {
						allReposMatch = false
						break
					}
				}
				if !allReposMatch {
					label = 1
					break
				}

			}
		}

		totalLevenshtein := 0.0
		totalTextShare := 0.0
		for _, repo := range repos {
			repoParts := strings.Split(strings.ToLower(repo["repo_name"].(string)), "/")
			repo_owner := repoParts[0]
			repo_name := ""
			if len(repoParts) > 1 {
				repo_name = repoParts[1]
			}
			repo := Repo{
				RepoName:         repo_name,
				StarCount:        int(repo["star_count"].(float64)),
				PullCount:        int(repo["pull_count"].(float64)),
				IsOfficial:       repo["is_official"].(bool),
				IsAutomated:      repo["is_automated"].(bool),
				RepoOwner:        repo_owner,
				ShortDescription: repo["short_description"].(string),
			}
			lehenshtein := get_significant_levenshtein(strings.ToLower(query), repo)
			textShare := get_significant_text_share(strings.ToLower(query), repo)
			totalLevenshtein += lehenshtein
			totalTextShare += textShare
		}

		writer.Write([]string{
			fmt.Sprintf("%d", label), // Label: 0 for child has no entries, 1 for child has entries
			fmt.Sprintf("%d", len(repos)),
			fmt.Sprintf("%.7f", totalLevenshtein/float64(len(repos))),
			fmt.Sprintf("%.7f", totalTextShare/float64(len(repos))),
		})
	}

	return nil

}

/*
Exportiert die Features skaliert in eine CSV-Datei für Learning to Rank.
Skalierung:
- log1p scaled star_count
- log1p scaled pull_count
- binary is_official
- binary is_automated
- max levenshtein similarity der Variable(repo_name, repo_owner, short_description) mit Query
- max relative position der Variable(repo_name, repo_owner, short_description) mit Query
- max Kategorie der Variable(repo_name, repo_owner, short_description) mit Query
- max Jaccard Similarity der Variable(repo_name, repo_owner, short_description) mit Query
- binary Query == Variable(repo_name, repo_owner, short_description)
- Query in welcher Variable(repo_name, repo_owner, short_description)
*/
func scaled_export_to_csv(results map[string][]map[string]any, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Schreibe Header
	writer.Write([]string{"query", "rank", "repo_name", "star_count", "pull_count", "is_official", "is_automated", "repo_owner", "short_description",
		"significant_levenshtein", "significant_position", "significant_category", "significant_jaccard", "is_standalone", "query_find"})

	// Schreibe Daten
	for query, repos := range results {
		for idx, repo := range repos {
			queryLower := strings.ToLower(query)
			repoParts := strings.Split(strings.ToLower(repo["repo_name"].(string)), "/")
			repo_owner := repoParts[0]
			repo_name := ""
			if len(repoParts) > 1 {
				repo_name = repoParts[1]
			}

			repo := Repo{
				RepoName:         repo_name,
				StarCount:        int(repo["star_count"].(float64)),
				PullCount:        int(repo["pull_count"].(float64)),
				IsOfficial:       repo["is_official"].(bool),
				IsAutomated:      repo["is_automated"].(bool),
				RepoOwner:        repo_owner,
				ShortDescription: repo["short_description"].(string),
			}

			writer.Write([]string{
				query,
				fmt.Sprintf("%d", idx+1),
				repo.RepoName,
				fmt.Sprintf("%.7f", math.Log1p(float64(repo.StarCount))),
				fmt.Sprintf("%.7f", math.Log1p(float64(repo.PullCount))),
				fmt.Sprintf("%d", boolToInt(repo.IsOfficial)),
				fmt.Sprintf("%d", boolToInt(repo.IsAutomated)),
				repo.RepoOwner,
				repo.ShortDescription,
				fmt.Sprintf("%.7f", get_significant_levenshtein(queryLower, repo)),
				fmt.Sprintf("%.7f", get_significant_position(queryLower, repo)),
				fmt.Sprintf("%.7f", get_significant_category(queryLower, repo)),
				fmt.Sprintf("%.7f", get_significant_jaccard(queryLower, repo)),
				fmt.Sprintf("%d", get_is_standalone(queryLower, repo)),
				fmt.Sprintf("%d", get_query_find(queryLower, repo)),
			})
		}
	}

	return nil
}

/*
Deprecated
*/
func ExportToCSV(results map[string][]map[string]any, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Schreibe Header
	writer.Write([]string{"query", "rank", "repo_name_cat", "star_count", "pull_count", "is_official", "is_automated", "repo_owner", "short_description"})

	// Schreibe Daten
	for query, repos := range results {
		for idx, repo := range repos {
			writer.Write([]string{
				query,
				fmt.Sprintf("%d", idx+1),
				repo["repo_name"].(string),
				fmt.Sprintf("%.0f", repo["star_count"].(float64)),
				fmt.Sprintf("%.0f", repo["pull_count"].(float64)),
				fmt.Sprintf("%t", repo["is_official"].(bool)),
				fmt.Sprintf("%t", repo["is_automated"].(bool)),
				repo["repo_owner"].(string),
				repo["short_description"].(string),
			})
		}
	}

	return nil
}
