package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/sissl0/DockerAnalysis/analysis"
	"github.com/sissl0/DockerAnalysis/cmd"
	"github.com/sissl0/DockerAnalysis/internal/utils"
	"github.com/sissl0/DockerAnalysis/ltr"
	"github.com/sissl0/DockerAnalysis/pkg/database"
)

func weightcollection() {
	var proxies []string

	file, err := os.Open("data/proxylist.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	data := struct {
		Proxies []string `json:"proxies"`
	}{}

	err = decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	proxies = data.Proxies
	writer, err := database.NewRotatingJSONLWriter("repos", "learningRepos_descr", 500000000, 0)
	if err != nil {
		panic(err)
	}
	defer writer.Close()
	collector, err := cmd.NewCollector(proxies, 10, nil, writer)
	if err != nil {
		panic(err)
	}

	collector.GetWeights()
}

func get_repo_list(repopath string) ([]string, error) {
	repos, err := ltr.LoadData(repopath)
	if err != nil {
		return nil, fmt.Errorf("error loading data from %s: %w", repopath, err)
	}
	repo_list := make([]string, 0, len(repos))
	for _, repo_data := range repos {
		for _, repo := range repo_data {
			repo_name := repo["repo_name"].(string)
			if repo_name != "#/#" {
				repo_list = append(repo_list, repo_name)
			}
		}
	}
	return repo_list, nil

}

func get_repo_lists(filename string) error {
	total_repo_list := make([]string, 0)
	repo_list, err := get_repo_list("repos/learningRepos_descr.jsonl")
	if err != nil {
		panic("Error getting repo list: " + err.Error())
	}
	total_repo_list = append(total_repo_list, repo_list...)
	for i := 0; i < 66; i++ {
		repo_path := fmt.Sprintf("repos/repos__%d.jsonl", i)
		repo_list, err := get_repo_list(repo_path)
		if err != nil {
			panic("Error getting repo list: " + err.Error())
		}
		total_repo_list = append(total_repo_list, repo_list...)
	}
	repo_list_json := struct {
		Repos []string `json:"repos"`
	}{
		Repos: total_repo_list,
	}
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file %s: %w", filename, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	err = encoder.Encode(repo_list_json)
	if err != nil {
		return fmt.Errorf("error encoding JSON to file %s: %w", filename, err)
	}
	fmt.Println(len(total_repo_list))
	return nil
}

func tagcollection(repo_list string) {
	var proxies []string

	file, err := os.Open("data/proxylist.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	data := struct {
		Proxies []string `json:"proxies"`
	}{}

	err = decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	proxies = data.Proxies
	writer, err := database.NewRotatingJSONLWriter("tags", "tags_", 500000000, 0)
	if err != nil {
		panic(err)
	}
	defer writer.Close()
	collector, err := cmd.NewTagCollector(proxies, 10, nil, writer)
	if err != nil {
		panic(err)
	}
	collector.Get_tags(repo_list)
}

func repocollection() {
	var proxies []string

	file, err := os.Open("data/proxylist.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	data := struct {
		Proxies []string `json:"proxies"`
	}{}

	err = decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	proxies = data.Proxies
	writer, err := database.NewRotatingJSONLWriter("repos", "repos_", 500000000, 0)
	if err != nil {
		panic(err)
	}
	defer writer.Close()
	collector, err := cmd.NewCollector(proxies, 10, nil, writer)
	if err != nil {
		panic(err)
	}

	collector.GetRepos()
}

func TestLTRClient_Predict() {
	client, err := ltr.NewLTRClient()
	if err != nil {
		panic("Error creating LTR client: " + err.Error())
	}
	query := "cars"
	url := fmt.Sprintf("https://hub.docker.com/v2/search/repositories/?query=%s&page=1&page_size=100", query)
	resp, err := client.Client.Network_Get(url, nil, nil)
	if err != nil {
		panic("Error making GET request: " + err.Error())
	}
	defer resp.Body.Close()
	var results struct {
		Results []ltr.Repo `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		panic("Failed to decode response: " + err.Error())
	}
	//for i, repo := range results.Results[len(results.Results)-9:] {
	//	fmt.Println(i, repo)
	//}
	res, err := client.Predict(query, results.Results)
	//res, err := client.Predict(query, results.Results[:9])
	if err != nil {
		fmt.Printf("Error during prediction: %v\n", err)
		return
	}
	fmt.Println(res)
}

func layercollection(repo_digest_list string, username string, accessToken string) {
	var proxies []string

	file, err := os.Open("data/proxylist.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	data := struct {
		Proxies []string `json:"proxies"`
	}{}

	err = decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	proxies = data.Proxies

	writer, err := database.NewRotatingJSONLWriter("layers", "layers_", 500000000, 226)
	if err != nil {
		panic(err)
	}
	defer writer.Close()

	reader, err := database.NewJSONLReader(repo_digest_list)
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	collector, err := cmd.NewManifestsCollector(username, accessToken, 20, nil, writer, reader, proxies)
	if err != nil {
		panic(err)
	}

	collector.CollectManifests()
}

func main() {

	switch os.Args[1] {
	case "weightcollection":
		weightcollection()
	case "learnweights":
		ltr.Run()
	case "test_client":
		TestLTRClient_Predict()
	case "repocollection":
		repocollection()
	case "get_repo_list":
		get_repo_lists("data/repo_list.json")
	case "tagcollection":
		tagcollection("data/repo_list.json")
	case "get_tag_list":
		utils.LoadTags("data/tag_list.jsonl")

	case "layercollection":
		if len(os.Args) < 5 {
			fmt.Println("Usage: layercollection <repo_digest_list.json> <username> <accessToken>")
			return
		}
		layercollection(os.Args[2], os.Args[3], os.Args[4])

	case "load_layers":
		if len(os.Args) < 4 {
			fmt.Println("Usage: load_layers <layerfilepath> <maxFiles> <outputfile>")
			return
		}
		layerfilepath := os.Args[2]
		maxFiles, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Println("Error: maxFiles must be a valid integer")
			return
		}
		outputfile := os.Args[4]
		if err := utils.LoadLayers(layerfilepath, maxFiles, outputfile); err != nil {
			fmt.Println("Error loading layers:", err)
			return
		}

	case "runtime":
		if len(os.Args) < 4 {
			fmt.Println("Usage: runtime <Layer File> <maxStorage in GB>")
			return
		}

		reader, err := database.NewJSONLReader(os.Args[2])
		if err != nil {
			fmt.Println("Error opening layer file:", err)
			return
		}

		defer reader.Close()

		maxStorageGB, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Println("Error: maxStorage must be a valid integer")
			return
		}

		runtimeHandler := cmd.NewRuntimeHandler(context.Background(), reader, "runtime/results/", int64(maxStorageGB)*1e+9, runtime.NumCPU(), 30)
		err = runtimeHandler.Run()
		if err != nil {
			fmt.Println("Error running runtime handler:", err)
			return
		}

	case "get_sample":
		if len(os.Args) < 4 {
			fmt.Println("Usage: get_sample <unique_layer_file> <sample_file>")
			return
		}
		if err := utils.CreateSample(os.Args[2], os.Args[3]); err != nil {
			fmt.Println("Error getting sample:", err)
			return
		}

	case "load_to_ps":
		if len(os.Args) < 7 {
			fmt.Println("Usage: load_to_ps <repo_file> <tag_file> <layer_file> <layer_data_file> <secrets_file>")
			return
		}
		connStr := ""
		analysis.Run(connStr, os.Args[2], os.Args[3], os.Args[4], os.Args[5], os.Args[6])
	case "fix_layer_tag_con":
		connStr := "postgres://postgres:mypassword@localhost:5500/postgres?sslmode=disable"
		db, err := sql.Open("pgx", connStr)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		// Pool-Parameter (optional)
		db.SetMaxOpenConns(16)
		db.SetMaxIdleConns(16)
		db.SetConnMaxLifetime(30 * time.Minute)

		// Verbindung testen
		if err := db.Ping(); err != nil {
			log.Fatal(err)
		}
		analysis.FixLayerTagConnections(db)
	default:
		panic("Unknown command: " + os.Args[1])
	}

}
