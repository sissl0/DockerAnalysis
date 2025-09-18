# DockerAnalysis

Werkzeuge zum Sammeln, Verarbeiten und Analysieren von Docker Hub Daten (Repos, Tags, Layer/Manifeste) sowie Import nach PostgreSQL und Auswertung von Secrets.

## Voraussetzungen
- Linux
- Go 1.20+
- PostgreSQL 14+ Citus
- Optional: Python 3.10+ für Notebooks
- Redis

Standard-DSN im Code (analysis.Run): `postgres://postgres:mypassword@localhost:5500/postgres?sslmode=disable`

## Build
```bash
go build -o dockeranalysis
```

## Proxies
Datei: `data/proxylist.json`
```json
{ "proxies": ["http://user:pass@host:port", "http://host:port"] }
```
Leere Liste ist erlaubt.

## Befehle

- Repos sammeln
  ```bash
  go run dockeranalysis.go repocollection
  ```
  Ausgabe: rotierende JSONL unter `repos/` (Prefix `repos_`)

- Repo-Liste bauen (aus vorhandenen Dateien)
  ```bash
  go run dockeranalysis.go get_repo_list
  ```
  Ausgabe: `data/repo_list.json` mit Feld `Repos`.

- Tags sammeln (benötigt `data/repo_list.json`)
  ```bash
  go run dockeranalysis.go tagcollection
  ```
  Ausgabe: rotierende JSONL unter `tags/` (Prefix `tags_`)

- Layer/Manifeste sammeln (benötigt Liste der Repo+Digest-Paare)
  ```bash
  go run dockeranalysis.go layercollection <repo_digest_list.json> <dockerhub_user> <accessToken>
  ```
  Ausgabe: rotierende JSONL unter `layers/` (Prefix `layers_`)

- Layer-Dateien vorverarbeiten (Sampling/Begrenzung)
  ```bash
  go run dockeranalysis.go load_layers <layerfilepath> <maxFiles> <outputfile>
  ```

- Runtime-Extraktion (lädt/image-layers lokal, begrenzt Storage)
  ```bash
  go run dockeranalysis.go runtime <layer_file.jsonl> <maxStorageGB>
  ```
  Ausgabe: `runtime/results/`

- Sample aus Unique-Layern ziehen
  ```bash
  go run dockeranalysis.go get_sample <unique_layer_file.jsonl> <sample_output.jsonl>
  ```

- Import nach PostgreSQL
  ```bash
  go run dockeranalysis.go load_to_ps <repos.jsonl> <tags.jsonl> <layers.jsonl> <layer_data.jsonl> <secrets_fragments.jsonl>
  ```
- Precompute für Analyse
  ```bash
  go run dockeranalysis.go precompute_tags <tag_file>
  go run dockeranalysis.go precompute_repos <unique_repos.jsonl>
  ```
  Importiert:
  - repos, tags, layer_blobs, repo_layers
  - layer_data (file_count, max_depth, uncompressed_size)
  - secret_fragments (Details je fragment_hash)
  - layer_secret_fragments (Mapping Layer ↔ fragment_hash)



## Typische Pipeline
1) Repos sammeln → `repocollection`
2) Repo-Liste erstellen → `get_repo_list`
3) Tags sammeln → `tagcollection`
4) Layer/Manifeste sammeln → `layercollection`
5) Dateiinfos/Secrets `runtime`
6) In Postgres importieren → `load_to_ps`
7) Analysieren → `scripts/python_scripts/Analyse/`


## SQL Datenbank
- `repos(id, repo_name, …)`
- `tags(digest, repo_name, size, position, …)`
- `layer_blobs(id, digest BYTEA, size)`
- `repo_layers(repo_id, layer_id, position)`
- `layer_data(layer_id PK, file_count, max_depth, uncompressed_size)`
- `secret_fragments(fragment_hash PK, file, file_type, file_size, origin, secret, start_line)`
- `layer_secret_fragments(layer_id, fragment_hash, PK(layer_id, fragment_hash))`


