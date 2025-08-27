package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/sissl0/DockerAnalysis/pkg/database"
)

func GetRepoList(filename string, target_file string) error {
	reader, err := database.NewJSONLReader(filename)
	redis := database.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		fmt.Println("Error opening JSONL reader:", err)
		return err
	}

	ctx := context.Background()

	var unique_repos = make(map[string][]string)
	unique_repos["repos"] = make([]string, 100000)

	for reader.Scanner.Scan() {
		line := reader.Scanner.Text()
		var record map[string]any
		err := json.Unmarshal([]byte(line), &record)
		if err != nil {
			fmt.Println("Error unmarshalling JSON:", err)
			continue
		}
		if record["repo_name"] == "#/#" {
			continue
		}

		repo_name, ok := record["repo_name"].(string)
		if !ok {
			fmt.Println("Invalid repo_name format:", record)
			continue
		}
		isMember, err := redis.IsMember(ctx, "unique_repos", repo_name)
		if err != nil {
			fmt.Println("Error checking membership in Redis:", err)
			return err
		}
		if !isMember {
			unique_repos["repos"] = append(unique_repos["repos"], repo_name)
			if added, err := redis.AddToSet(ctx, "scanned_repos", record["repo_name"].(string)); err != nil || added == -1 {
				fmt.Println("Error adding member to Redis:", err)
				return err
			}
		}
	}
	file, err := json.MarshalIndent(unique_repos, "", "  ")
	if err != nil {
		fmt.Println("Error marshalling unique_repos to JSON:", err)
		return err
	}

	err = os.WriteFile(target_file, file, 0644)
	if err != nil {
		fmt.Println("Error writing JSON to file:", err)
		return err
	}

	return nil
}
