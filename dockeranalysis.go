package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/regclient/regclient"
	"github.com/regclient/regclient/types/descriptor"
	"github.com/regclient/regclient/types/ref"
	"github.com/sissl0/DockerAnalysis/cmd"
	"github.com/sissl0/DockerAnalysis/internal/network"
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
	r, err := ref.New(repo)
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

	fmt.Println(reader.RawHeaders())
	fmt.Println(reader.Response().Status)
	//os.Mkdir(outputpath+digest_, 0755)
	// In Datei schreiben

	archivPath := fmt.Sprintf("%s/%s/", outputpath, digest_)
	_, err = utils.ExtractTar(reader, archivPath)
	if err != nil {
		fmt.Printf("Error extracting tar: %s\n", err.Error())
	}
}

func getBlob(repo string, digest_ string) error {
	client, err := network.NewClient("", 20, 0, 0)
	if err != nil {
		return fmt.Errorf("error creating network client: %w", err)
	}
	resp, err := client.Network_Get(fmt.Sprintf("https://registry-1.docker.io/v2/%s/blobs/%s", repo, digest_), nil, nil)
	if err != nil {
		return fmt.Errorf("error getting blob: %w", err)
	}
	defer resp.Body.Close()
	for key, values := range resp.Header {
		for _, value := range values {
			fmt.Printf("%s: %s\n", key, value)
		}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading blob body: %w", err)
	}
	fmt.Println(string(body))
	return nil
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
		if len(os.Args) < 2 {
			fmt.Println("Usage: regctl_test <layer_file>")
			return
		}
		reader, err := database.NewJSONLReader(os.Args[2])
		if err != nil {
			fmt.Println("Error opening layer file:", err)
			return
		}
		defer reader.Close()
		cnt := 0
		for reader.Scanner.Scan() {
			cnt++
			line := reader.Scanner.Text()
			var record map[string]any
			err := json.Unmarshal([]byte(line), &record)
			if err != nil {
				fmt.Println("Error unmarshalling JSON:", err)
				continue
			}
			repo := record["repo"].(string)
			digest_ := record["layer_digest"].(string)
			size := int64(record["size"].(float64))
			go regctl_test(repo, digest_, size, "runtime")
			if cnt > 25 {
				break
			}
		}
		time.Sleep(600 * time.Second)
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
		if len(os.Args) < 3 {
			fmt.Println("Usage: gitleaks_test <source> <digest>")
			return
		}

		fileInfoWriter, err := database.NewRotatingJSONLWriter("runtime/results/fileinfos/", "fileinfo_", 500000000, 0)
		if err != nil {
			fmt.Println("Error creating fileInfoWriter: ", err)
		}
		secretsWriter, err := database.NewRotatingJSONLWriter("runtime/results/secrets/", "secrets_", 500000000, 0)
		if err != nil {
			fmt.Println("Error creating secretsWriter: ", err)
		}
		defer fileInfoWriter.Close()
		defer secretsWriter.Close()
		// if err := utils.GitleaksScan(os.Args[2], secretsWriter, &sync.Mutex{}, fileInfoWriter, &sync.Mutex{}, os.Args[3], 20000); err != nil {
		// 	fmt.Println("Error running gitleaks scan:", err)
		// 	return
		// }
	case "get_blob":
		if len(os.Args) < 4 {
			fmt.Println("Usage: get_blob <repo> <digest>")
			return
		}
		if err := getBlob(os.Args[2], os.Args[3]); err != nil {
			fmt.Println("Error getting blob:", err)
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
	case "test_extract":
		regctl_test("zaphodbeeblebrox/binarypy0", "sha256:00001d0ba60bc4f8a3ddc66d3e4558ccba776b4deb9e2ce3f4191a23242f221a", 51792133, "runtime_test")
	default:
		panic("Unknown command: " + os.Args[1])
	}

}
