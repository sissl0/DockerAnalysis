package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/opencontainers/go-digest"
	"github.com/regclient/regclient"
	"github.com/regclient/regclient/types/descriptor"
	"github.com/regclient/regclient/types/ref"
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

func countRepoPlace(reponame string, repofile string) {
	file, err := os.Open(repofile)
	if err != nil {
		fmt.Printf("error opening repository file: %v\n", err)
		return
	}
	defer file.Close()
	repos := struct {
		Repos []string `json:"repos"`
	}{}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&repos)
	if err != nil {
		fmt.Printf("error decoding repository file: %v\n", err)
		return
	}
	name_idx := 0
	for i, _ := range repos.Repos {
		if repos.Repos[i] == reponame {
			name_idx = i
			break
		}
	}
	fmt.Println("Progress of", reponame, "is", name_idx+1, "of", len(repos.Repos), "repos")
}

func countTagPlace(reponame string, tagfile string) {
	name_idx := 0
	reader, err := database.NewJSONLReader(tagfile)
	if err != nil {
		fmt.Printf("error opening tag file: %v\n", err)
		return
	}
	defer reader.Close()
	for reader.Scanner.Scan() {
		line := reader.Scanner.Text()
		var record map[string]any
		err := json.Unmarshal([]byte(line), &record)
		if err != nil {
			fmt.Printf("error unmarshalling tag record: %v\n", err)
			continue
		}
		repo_name, ok := record["repo"].(string)
		if !ok {
			fmt.Printf("error getting repo_name from tag record: %v\n", record)
			continue
		}
		if repo_name == reponame {
			break
		}
		name_idx++
	}
	fmt.Println("Progress of", reponame, "is", name_idx+1, "of", 88978092, "tags")
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

func regctl_test(repo string, digest_ string, size int64, outputpath string) {
	ctx := context.Background()

	// Registry-Client erstellen
	rc := regclient.New()

	// Referenz parsen (repo@digest)
	r, err := ref.New(repo + "@" + digest_)
	if err != nil {
		panic(err)
	}

	desc := descriptor.Descriptor{
		Digest:    digest.Digest(digest_),
		MediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
		Size:      size,
	}

	// Blob holen
	reader, err := rc.BlobGet(ctx, r, desc)
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	//os.Mkdir(outputpath+digest_, 0755)
	// In Datei schreiben
	tarFileName := fmt.Sprintf("%s/%s.tar", outputpath, digest_)
	out, err := os.Create(tarFileName)
	if err != nil {
		panic(err)
	}
	defer out.Close()

	_, err = io.Copy(out, reader)
	if err != nil {
		panic(err)
	}

	archivPath := fmt.Sprintf("%s/%s/", outputpath, digest_)
	err = utils.ExtractTar(tarFileName, archivPath)
	if err != nil {
		panic("Error extracting tar: " + err.Error())
	}

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
	case "count_repo_place":
		countRepoPlace(os.Args[2], "data/repo_list.json")
	case "layercollection":
		if len(os.Args) < 5 {
			fmt.Println("Usage: layercollection <repo_digest_list.json> <username> <accessToken>")
			return
		}
		layercollection(os.Args[2], os.Args[3], os.Args[4])
	case "count_tag_place":
		countTagPlace(os.Args[2], "data/tag_list.jsonl")
	case "regctl_test":
		if len(os.Args) < 5 {
			fmt.Println("Usage: regctl_test <repo> <digest> <size>")
			return
		}
		size, err := strconv.ParseInt(os.Args[4], 10, 64)
		if err != nil {
			fmt.Println("Error: size must be a valid integer")
			return
		}
		regctl_test(os.Args[2], os.Args[3], size, "runtime")
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
	case "gitleaks_test":
		if len(os.Args) < 4 {
			fmt.Println("Usage: gitleaks_test <source> <outputfile> <size>")
			return
		}
		size, err := strconv.Atoi(os.Args[4])
		if err != nil {
			fmt.Println("Error: size must be a valid integer")
			return
		}
		if err := utils.GitleaksScan(os.Args[2], os.Args[3], size); err != nil {
			fmt.Println("Error running gitleaks scan:", err)
			return
		}
	default:
		panic("Unknown command: " + os.Args[1])
	}

}
