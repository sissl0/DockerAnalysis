package analysis

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	// neu
	_ "github.com/jackc/pgx/v5/stdlib" // pgx als database/sql Treiber
)

type Repo struct {
	IsAutomated      bool   `json:"is_automated"`
	IsOfficial       bool   `json:"is_official"`
	PullCount        int64  `json:"pull_count"`
	RepoName         string `json:"repo_name"`
	RepoOwner        string `json:"repo_owner"`
	ShortDescription string `json:"short_description"`
	StarCount        int64  `json:"star_count"`
}

type OptionalTime struct {
	time.Time
	Valid bool
}

type Tag struct {
	Architecture string       `json:"architecture"`
	Digest       string       `json:"digest"`
	LastPulled   OptionalTime `json:"last_pulled"` // geändert: OptionalTime
	LastPushed   OptionalTime `json:"last_pushed"` // geändert: OptionalTime
	OS           string       `json:"os"`
	RepoName     string       `json:"repo_name"`
	Size         int64        `json:"size"`
	Status       string       `json:"status"`
}

type Layer struct {
	LayerDigest string `json:"layer_digest"`
	Repo        string `json:"repo"`
	Size        int64  `json:"size"`
	Position    int    `json:"-"` // neu: feste Position pro Zeile
}

// Neue Strukturen für zusätzliche Layer-Daten & Secrets
type layerDataJSON struct {
	Digest           string   `json:"digest"`
	FileCount        int      `json:"file_count"`
	MaxDepth         int      `json:"max_depth"`
	UncompressedSize int64    `json:"uncompressed_size"`
	Secrets          []string `json:"secrets"` // fragment_hash-Liste (kann leer sein)
}

type layerSecretsJSON struct {
	FragmentHash string             `json:"fragment_hash"`
	Secrets      fragmentSecretInfo `json:"secrets"`
}

type fragmentSecretInfo struct {
	File      string `json:"file"`
	FileType  string `json:"file_type"`
	FileSize  int64  `json:"file_size"`
	Origin    string `json:"origin"`
	Secret    string `json:"secret"`
	StartLine int    `json:"start_line"`
}

func Run(connStr string, reposFile, tagsFile, layersFile, layerDataFile, secretsFile string) {

	// Default-DSN, wenn nichts übergeben wurde
	if strings.TrimSpace(connStr) == "" {
		connStr = "postgres://postgres:mypassword@localhost:5500/postgres?sslmode=disable"
	}

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

	if err := importData(db, reposFile, tagsFile, layersFile, layerDataFile, secretsFile); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Daten erfolgreich importiert!")
}

func importData(db *sql.DB, reposFile, tagsFile, layersFile, layerDataFile, secretsFile string) error {
	// vorhandene Importe (auskommentiert bei Bedarf)
	// if err := importRepos(db, reposFile); err != nil { return err }
	// if err := importTags(db, tagsFile); err != nil { return err }
	//if err := importLayers(db, layersFile); err != nil {
	//	return err
	//}
	// Zusätzliche Dateien:
	if err := importLayerData(db, layerDataFile); err != nil {
		return err
	}
	if err := importLayerSecrets(db, secretsFile); err != nil {
		return err
	}
	return nil
}

func importRepos(db *sql.DB, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	batch := make([]*Repo, 0, 1000)
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		var repo Repo
		if err := json.Unmarshal(scanner.Bytes(), &repo); err != nil {
			log.Printf("skip repo line %d: json error: %v", lineNo, err)
			continue
		}
		batch = append(batch, &repo)
		if len(batch) >= 1000 {
			if err := insertReposBatch(db, batch); err != nil {
				log.Printf("insert repos batch error: %v (continuing)", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := insertReposBatch(db, batch); err != nil {
			log.Printf("insert repos last batch error: %v (continuing)", err)
		}
	}
	return scanner.Err()
}

func insertReposBatch(db *sql.DB, repos []*Repo) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, repo := range repos {
		if _, err := tx.ExecContext(ctx, "SAVEPOINT sp"); err != nil {
			log.Printf("savepoint error (repo %s): %v", repo.RepoName, err)
			continue
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO repos (repo_name, repo_owner, is_automated, is_official, pull_count, star_count, short_description)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (repo_name) DO UPDATE SET
               repo_owner = EXCLUDED.repo_owner,
               is_automated = EXCLUDED.is_automated,
               is_official = EXCLUDED.is_official,
               pull_count = EXCLUDED.pull_count,
               star_count = EXCLUDED.star_count,
               short_description = EXCLUDED.short_description`,
			repo.RepoName, repo.RepoOwner, repo.IsAutomated, repo.IsOfficial,
			repo.PullCount, repo.StarCount, repo.ShortDescription,
		)
		if err != nil {
			log.Printf("skip repo %s: insert error: %v", repo.RepoName, err)
			_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp")
			_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
			continue
		}
		_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
	}
	return tx.Commit()
}

func importTags(db *sql.DB, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	positionCounter := make(map[string]int)
	batch := make([]*Tag, 0, 1000)
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		var tag Tag
		if err := json.Unmarshal(scanner.Bytes(), &tag); err != nil {
			log.Printf("skip tag line %d: json error: %v", lineNo, err)
			continue
		}
		positionCounter[tag.RepoName]++
		batch = append(batch, &tag)
		if len(batch) >= 1000 {
			if err := insertTagsBatch(db, batch, positionCounter); err != nil {
				log.Printf("insert tags batch error: %v (continuing)", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := insertTagsBatch(db, batch, positionCounter); err != nil {
			log.Printf("insert tags last batch error: %v (continuing)", err)
		}
	}
	return scanner.Err()
}

func insertTagsBatch(db *sql.DB, tags []*Tag, positions map[string]int) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, _ = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = off`)

	if err := ensureRepos(ctx, tx, uniqueReposFromTags(tags)); err != nil {
		log.Printf("ensure repos for tags failed: %v", err)
	}

	// SAVEPOINT vor Bulk-Insert
	if _, err := tx.ExecContext(ctx, "SAVEPOINT bulk"); err != nil {
		return fmt.Errorf("savepoint bulk: %w", err)
	}
	if berr := bulkInsertTags(ctx, tx, tags, positions); berr == nil {
		_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT bulk")
		return tx.Commit()
	} else {
		log.Printf("bulk insert tags failed (%d rows), falling back to row-by-row: %v", len(tags), berr)
	}
	// Bulk-Fehler: zum SAVEPOINT zurückrollen, dann Row-by-Row
	_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT bulk")
	_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT bulk")

	for _, tag := range tags {
		position := positions[tag.RepoName]
		if _, err := tx.ExecContext(ctx, "SAVEPOINT sp"); err != nil {
			log.Printf("savepoint error (tag %s): %v", tag.Digest, err)
			continue
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO tags (repo_name, digest, architecture, os, size, status, last_pulled, last_pushed, position)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (repo_name, digest) DO UPDATE SET
               architecture = EXCLUDED.architecture,
               os = EXCLUDED.os,
               size = EXCLUDED.size,
               status = EXCLUDED.status,
               last_pulled = EXCLUDED.last_pulled,
               last_pushed = EXCLUDED.last_pushed`,
			tag.RepoName, tag.Digest, tag.Architecture, tag.OS, tag.Size,
			tag.Status, tag.LastPulled.Ptr(), tag.LastPushed.Ptr(), position,
		)
		if err != nil {
			log.Printf("skip tag %s,%s: insert error: %v", tag.Digest, tag.RepoName, err)
			_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp")
			_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
			continue
		}
		_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
	}
	return tx.Commit()
}

func importLayers(db *sql.DB, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)

	// Positionen ab 0 neu zählen (kein Resume aus DB, kein Start-Offset)
	positionCounter := make(map[string]int)

	batch := make([]*Layer, 0, 1000)
	var lineNo int64

	for scanner.Scan() {
		lineNo++
		var layer Layer
		if err := json.Unmarshal(scanner.Bytes(), &layer); err != nil {
			log.Printf("skip layer line %d: json error: %v", lineNo, err)
			continue
		}
		positionCounter[layer.Repo]++
		layer.Position = positionCounter[layer.Repo] // hier festhalten
		batch = append(batch, &layer)
		if len(batch) >= 1000 {
			if err := insertLayersBatch(db, batch, positionCounter); err != nil {
				log.Printf("insert layers batch error: %v (continuing)", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := insertLayersBatch(db, batch, positionCounter); err != nil {
			log.Printf("insert layers last batch error: %v (continuing)", err)
		}
	}
	return scanner.Err()
}

func insertLayersBatch(db *sql.DB, layers []*Layer, positions map[string]int) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, _ = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = off`)

	// Repos anlegen und IDs auflösen
	if err := ensureRepos(ctx, tx, uniqueReposFromLayers(layers)); err != nil {
		log.Printf("ensure repos for layers failed: %v", err)
	}
	repoIDs, err := fetchRepoIDs(ctx, tx, uniqueReposFromLayers(layers))
	if err != nil {
		return fmt.Errorf("fetch repo ids: %w", err)
	}

	// SAVEPOINT vor Bulk-Insert
	if _, err := tx.ExecContext(ctx, "SAVEPOINT bulk"); err != nil {
		return fmt.Errorf("savepoint bulk: %w", err)
	}
	if berr := bulkInsertLayers(ctx, tx, layers, positions, repoIDs); berr == nil {
		_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT bulk")
		return tx.Commit()
	} else {
		log.Printf("bulk insert layers failed (%d rows), falling back to row-by-row: %v", len(layers), berr)
	}

	// Bulk-Fehler: Rollback und Row-by-Row
	_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT bulk")
	_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT bulk")

	for _, layer := range layers {
		position := layer.Position // statt: positions[layer.Repo]
		if _, err := tx.ExecContext(ctx, "SAVEPOINT sp"); err != nil {
			log.Printf("savepoint error (layer %s): %v", layer.LayerDigest, err)
			continue
		}

		// blob upsert
		dbuf, derr := digestToBytes(layer.LayerDigest)
		if derr != nil {
			log.Printf("skip layer (bad digest) %s: %v", layer.LayerDigest, derr)
			_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp")
			_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO layer_blobs (digest, size) VALUES ($1,$2)
             ON CONFLICT (digest) DO NOTHING`,
			dbuf, layer.Size,
		); err != nil {
			log.Printf("skip layer %s: blob upsert error: %v", layer.LayerDigest, err)
			_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp")
			_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
			continue
		}

		// IDs holen
		var layerID int64
		if err := tx.QueryRowContext(ctx, `SELECT id FROM layer_blobs WHERE digest=$1`, dbuf).Scan(&layerID); err != nil {
			log.Printf("skip layer %s: fetch layer_id error: %v", layer.LayerDigest, err)
			_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp")
			_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
			continue
		}
		repoID, ok := repoIDs[layer.Repo]
		if !ok {
			log.Printf("skip layer %s: unknown repo %s", layer.LayerDigest, layer.Repo)
			_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp")
			_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
			continue
		}

		// Mapping eintragen
		_, err := tx.ExecContext(ctx,
			`INSERT INTO repo_layers (repo_id, layer_id, position)
             VALUES ($1,$2,$3)
             ON CONFLICT (repo_id, position) DO NOTHING`,
			repoID, layerID, position,
		)
		if err != nil {
			log.Printf("skip layer %s: map error: %v", layer.LayerDigest, err)
			_, _ = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT sp")
			_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
			continue
		}
		_, _ = tx.ExecContext(ctx, "RELEASE SAVEPOINT sp")
	}
	return tx.Commit()
}

// Import von layer_data JSONL (Datei enthält Zeilen vom Typ layerDataJSON)
func importLayerData(db *sql.DB, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), 32<<20)

	const batchSize = 1000
	type row struct {
		digest           []byte
		fileCt           int
		depth            int
		uncompressedSize int64
		frags            []string
	}
	batch := make([]row, 0, batchSize)

	flush := func(rows []row) error {
		if len(rows) == 0 {
			return nil
		}
		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// unique Digests -> IDs
		dset := make(map[string][]byte, len(rows))
		for _, r := range rows {
			dset[string(r.digest)] = r.digest
		}
		digests := make([][]byte, 0, len(dset))
		for _, d := range dset {
			digests = append(digests, d)
		}
		idMap, err := fetchLayerIDs(ctx, tx, digests)
		if err != nil {
			return err
		}

		// layer_data upsert
		{
			sb := strings.Builder{}
			args := make([]any, 0, len(rows)*4)
			written := 0
			sb.WriteString(`INSERT INTO layer_data (layer_id, file_count, max_depth, uncompressed_size) VALUES `)
			for _, r := range rows {
				lid, ok := idMap[string(r.digest)]
				if !ok {
					log.Printf("layer_data: skip unknown digest")
					continue
				}
				if written > 0 {
					sb.WriteByte(',')
				}
				base := written*4 + 1
				sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d)", base, base+1, base+2, base+3))
				args = append(args, lid, r.fileCt, r.depth, r.uncompressedSize)
				written++
			}
			if written > 0 {
				sb.WriteString(` ON CONFLICT (layer_id) DO UPDATE SET
                  file_count = EXCLUDED.file_count,
                  max_depth = EXCLUDED.max_depth,
                  uncompressed_size = EXCLUDED.uncompressed_size`)
				if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
					return err
				}
			}
		}

		// secret_fragments: nur Hash anlegen (Details später), dann mapping layer_secret_fragments
		// 2.1) alle Fragment-Hashes sammeln
		type pair struct {
			lid int64
			fh  string
		}
		pairs := make([]pair, 0, 1024)
		fhSet := make(map[string]struct{}, 1024)
		for _, r := range rows {
			lid, ok := idMap[string(r.digest)]
			if !ok {
				continue
			}
			for _, fh := range r.frags {
				if fh == "" {
					continue
				}
				pairs = append(pairs, pair{lid: lid, fh: fh})
				fhSet[fh] = struct{}{}
			}
		}
		// 2.2) Fragment-Hashes in secret_fragments upserten (nur Schlüssel)
		if len(fhSet) > 0 {
			sb := strings.Builder{}
			args := make([]any, 0, len(fhSet))
			i := 0
			sb.WriteString(`INSERT INTO secret_fragments (fragment_hash) VALUES `)
			for fh := range fhSet {
				if i > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(fmt.Sprintf("($%d)", i+1))
				args = append(args, fh)
				i++
			}
			sb.WriteString(` ON CONFLICT (fragment_hash) DO NOTHING`)
			if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
				return err
			}
		}
		// 2.3) Zuordnung (layer_id, fragment_hash)
		if len(pairs) > 0 {
			const chunk = 20000
			for i := 0; i < len(pairs); i += chunk {
				end := i + chunk
				if end > len(pairs) {
					end = len(pairs)
				}
				sb := strings.Builder{}
				args := make([]any, 0, (end-i)*2)
				sb.WriteString(`INSERT INTO layer_secret_fragments (layer_id, fragment_hash) VALUES `)
				for j := i; j < end; j++ {
					if j > i {
						sb.WriteByte(',')
					}
					base := (j - i) * 2
					sb.WriteString(fmt.Sprintf("($%d,$%d)", base+1, base+2))
					args = append(args, pairs[j].lid, pairs[j].fh)
				}
				sb.WriteString(` ON CONFLICT (layer_id, fragment_hash) DO NOTHING`)
				if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
					return err
				}
			}
		}

		return tx.Commit()
	}

	for scanner.Scan() {
		var jd layerDataJSON
		if err := json.Unmarshal(scanner.Bytes(), &jd); err != nil {
			log.Printf("layer_data: skip malformed: %v", err)
			continue
		}
		if jd.Digest == "" {
			continue
		}
		b, err := digestToBytes(jd.Digest)
		if err != nil {
			log.Printf("layer_data: bad digest %s: %v", jd.Digest, err)
			continue
		}
		batch = append(batch, row{
			digest:           b,
			fileCt:           jd.FileCount,
			depth:            jd.MaxDepth,
			uncompressedSize: jd.UncompressedSize,
			frags:            jd.Secrets, // fragment hashes
		})
		if len(batch) >= batchSize {
			if err := flush(batch); err != nil {
				log.Printf("layer_data: flush error: %v", err)
			}
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return flush(batch)
}

func importLayerSecrets(db *sql.DB, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// sehr große Zeilen (Arrays) erlauben
	scanner.Buffer(make([]byte, 0, 1<<20), 512<<20)

	const batchSize = 2000
	const rowsPerStmt = 8000 // 8000*7 = 56k Parameter < 65535
	type row struct {
		hash string
		s    fragmentSecretInfo
	}
	batch := make([]row, 0, batchSize)

	flushChunk := func(ctx context.Context, tx *sql.Tx, rows []row) error {
		// in Statements von max rowsPerStmt splitten, um Param-Limit zu vermeiden
		for off := 0; off < len(rows); {
			end := off + rowsPerStmt
			if end > len(rows) {
				end = len(rows)
			}
			ch := rows[off:end]
			// Statement bauen
			var sb strings.Builder
			args := make([]any, 0, len(ch)*7)
			i := 0
			sb.WriteString(`INSERT INTO secret_fragments (fragment_hash, file, file_type, file_size, origin, secret, start_line) VALUES `)
			for _, r := range ch {
				if r.hash == "" {
					continue
				}
				if i > 0 {
					sb.WriteByte(',')
				}
				base := i*7 + 1
				sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", base, base+1, base+2, base+3, base+4, base+5, base+6))
				args = append(args,
					r.hash,
					cleanText(r.s.File),
					cleanText(r.s.FileType),
					r.s.FileSize,
					cleanText(r.s.Origin),
					cleanText(r.s.Secret),
					r.s.StartLine,
				)
				i++
			}
			if i > 0 {
				sb.WriteString(` ON CONFLICT (fragment_hash) DO UPDATE SET
               file = EXCLUDED.file,
               file_type = EXCLUDED.file_type,
               file_size = EXCLUDED.file_size,
               origin = EXCLUDED.origin,
               secret = EXCLUDED.secret,
               start_line = EXCLUDED.start_line`)
				if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
					return err
				}
			}
			off = end
		}
		return nil
	}

	flush := func(rows []row) error {
		if len(rows) == 0 {
			return nil
		}
		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if err := flushChunk(ctx, tx, rows); err != nil {
			return err
		}
		return tx.Commit()
	}

	for scanner.Scan() {
		line := scanner.Bytes()

		// Variante 1: einzelnes Objekt
		var one layerSecretsJSON
		if err := json.Unmarshal(line, &one); err == nil && one.FragmentHash != "" {
			batch = append(batch, row{hash: one.FragmentHash, s: one.Secrets})
		} else {
			// Variante 2: Array von Objekten in einer Zeile
			var many []layerSecretsJSON
			if err := json.Unmarshal(line, &many); err == nil {
				for _, m := range many {
					if m.FragmentHash == "" {
						continue
					}
					batch = append(batch, row{hash: m.FragmentHash, s: m.Secrets})
					// bei großen Arrays innerhalb der Zeile chunkweise flushen
					if len(batch) >= batchSize {
						if err := flush(batch); err != nil {
							log.Printf("layer_secrets: flush error: %v", err)
						}
						batch = batch[:0]
					}
				}
			} else {
				log.Printf("layer_secrets: skip malformed line")
				continue
			}
		}

		if len(batch) >= batchSize {
			if err := flush(batch); err != nil {
				log.Printf("layer_secrets: flush error: %v", err)
			}
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return flush(batch)
}

// Entfernt NUL-Bytes und repariert ungültiges UTF‑8 (Postgres TEXT verbietet 0x00)
func cleanText(s string) string {
	if s == "" {
		return s
	}
	s = strings.ReplaceAll(s, "\x00", "")
	s = strings.ToValidUTF8(s, "")
	return s
}

// Bulk-Insert Layers auf layer_blobs + repo_layers
func bulkInsertLayers(ctx context.Context, tx *sql.Tx, layers []*Layer, positions map[string]int, repoIDs map[string]int64) error {
	if len(layers) == 0 {
		return nil
	}

	// 1) Digests deduplizieren und in layer_blobs upserten
	type blob struct {
		d    []byte
		size int64
	}
	uniq := make(map[string]blob, len(layers))
	for _, l := range layers {
		if l == nil || l.LayerDigest == "" {
			continue
		}
		dbuf, err := digestToBytes(l.LayerDigest)
		if err != nil {
			return fmt.Errorf("digest parse: %w", err)
		}
		k := string(dbuf) // key by raw bytes
		if _, ok := uniq[k]; !ok {
			uniq[k] = blob{d: dbuf, size: l.Size}
		}
	}
	if len(uniq) > 0 {
		const cols = 2
		args := make([]any, 0, len(uniq)*cols)
		sb := strings.Builder{}
		sb.WriteString(`INSERT INTO layer_blobs (digest, size) VALUES `)
		i := 0
		for _, b := range uniq {
			if i > 0 {
				sb.WriteByte(',')
			}
			base := i*cols + 1
			sb.WriteString(fmt.Sprintf("($%d,$%d)", base, base+1))
			args = append(args, b.d, b.size)
			i++
		}
		sb.WriteString(` ON CONFLICT (digest) DO NOTHING`)
		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return err
		}
	}

	// 2) IDs zu Digests auflösen
	digests := make([][]byte, 0, len(uniq))
	for _, b := range uniq {
		digests = append(digests, b.d)
	}
	layerIDMap, err := fetchLayerIDs(ctx, tx, digests)
	if err != nil {
		return err
	}

	// 3) repo_layers befüllen, Dups je (repo_id, position) im Batch vermeiden
	const cols2 = 3
	args2 := make([]any, 0, len(layers)*cols2)
	sb2 := strings.Builder{}
	sb2.WriteString(`INSERT INTO repo_layers (repo_id, layer_id, position) VALUES `)
	type key struct {
		rid int64
		pos int
	}
	seen := make(map[key]struct{}, len(layers))
	row := 0
	for _, l := range layers {
		if l == nil || l.Repo == "" || l.LayerDigest == "" {
			continue
		}
		rid, ok := repoIDs[l.Repo]
		if !ok {
			return fmt.Errorf("unknown repo: %s", l.Repo)
		}
		dbuf, err := digestToBytes(l.LayerDigest)
		if err != nil {
			return fmt.Errorf("digest parse: %w", err)
		}
		lid, ok := layerIDMap[string(dbuf)]
		if !ok {
			return fmt.Errorf("missing layer_id for digest")
		}
		pos := l.Position // statt: positions[l.Repo]
		k := key{rid: rid, pos: pos}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}

		if row > 0 {
			sb2.WriteByte(',')
		}
		base := row*cols2 + 1
		sb2.WriteString(fmt.Sprintf("($%d,$%d,$%d)", base, base+1, base+2))
		args2 = append(args2, rid, lid, pos)
		row++
	}
	if row == 0 {
		return nil
	}
	sb2.WriteString(` ON CONFLICT (repo_id, position) DO NOTHING`)
	_, err = tx.ExecContext(ctx, sb2.String(), args2...)
	return err
}

// ensureRepos: fügt fehlende Repos (einmal pro Batch) ein
func ensureRepos(ctx context.Context, tx *sql.Tx, repos []string) error {
	if len(repos) == 0 {
		return nil
	}
	// Multi-Row INSERT DO NOTHING
	vals := make([]any, 0, len(repos))
	sb := strings.Builder{}
	sb.WriteString(`INSERT INTO repos (repo_name) VALUES `)
	for i, r := range repos {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("($%d)", i+1))
		vals = append(vals, r)
	}
	sb.WriteString(` ON CONFLICT (repo_name) DO NOTHING`)
	_, err := tx.ExecContext(ctx, sb.String(), vals...)
	return err
}

func uniqueReposFromTags(tags []*Tag) []string {
	seen := make(map[string]struct{}, len(tags))
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		if t == nil || t.RepoName == "" {
			continue
		}
		if _, ok := seen[t.RepoName]; !ok {
			seen[t.RepoName] = struct{}{}
			out = append(out, t.RepoName)
		}
	}
	return out
}

func uniqueReposFromLayers(layers []*Layer) []string {
	seen := make(map[string]struct{}, len(layers))
	out := make([]string, 0, len(layers))
	for _, l := range layers {
		if l == nil || l.Repo == "" {
			continue
		}
		if _, ok := seen[l.Repo]; !ok {
			seen[l.Repo] = struct{}{}
			out = append(out, l.Repo)
		}
	}
	return out
}

// Bulk-Insert Tags mit ON CONFLICT; überschreibt position NICHT (Reihenfolge bleibt)
func bulkInsertTags(ctx context.Context, tx *sql.Tx, tags []*Tag, positions map[string]int) error {
	if len(tags) == 0 {
		return nil
	}
	// Dedupliziere innerhalb des Batches nach (repo_name, digest)
	type urow struct {
		t   *Tag
		pos int
	}
	uniq := make(map[string]urow, len(tags))
	order := make([]string, 0, len(tags))
	for _, t := range tags {
		if t == nil {
			continue
		}
		k := t.RepoName + "\x00" + t.Digest
		p := positions[t.RepoName]
		if u, ok := uniq[k]; !ok {
			uniq[k] = urow{t: t, pos: p}
			order = append(order, k)
		} else {
			// früheste Position behalten
			if p < u.pos {
				u.pos = p
			}
			// übrige Felder mit den letzten Werten überschreiben
			u.t.Architecture = t.Architecture
			u.t.OS = t.OS
			u.t.Size = t.Size
			u.t.Status = t.Status
			u.t.LastPulled = t.LastPulled
			u.t.LastPushed = t.LastPushed
			uniq[k] = u
		}
	}

	const cols = 9
	args := make([]any, 0, len(uniq)*cols)
	sb := strings.Builder{}
	sb.WriteString(`INSERT INTO tags (repo_name, digest, architecture, os, size, status, last_pulled, last_pushed, position) VALUES `)
	for i, k := range order {
		u := uniq[k]
		if i > 0 {
			sb.WriteByte(',')
		}
		base := i*cols + 1
		sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8))
		args = append(args,
			u.t.RepoName, u.t.Digest, u.t.Architecture, u.t.OS, u.t.Size, u.t.Status, u.t.LastPulled.Ptr(), u.t.LastPushed.Ptr(), u.pos,
		)
	}
	sb.WriteString(` ON CONFLICT (repo_name, digest) DO UPDATE SET
        architecture = EXCLUDED.architecture,
        os = EXCLUDED.os,
        size = EXCLUDED.size,
        status = EXCLUDED.status,
        last_pulled = EXCLUDED.last_pulled,
        last_pushed = EXCLUDED.last_pushed`)
	_, err := tx.ExecContext(ctx, sb.String(), args...)
	return err
}

func (ot *OptionalTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "" || s == "null" {
		ot.Time = time.Time{}
		ot.Valid = false
		return nil
	}
	// RFC3339(+Nano) versuchen
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		ot.Time = t
		ot.Valid = true
		return nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		ot.Time = t
		ot.Valid = true
		return nil
	}
	return fmt.Errorf("invalid time format: %q", s)
}

// Ptr: für INSERT, gibt nil bei ungültig zurück (-> SQL NULL)
func (ot OptionalTime) Ptr() any {
	if !ot.Valid {
		return nil
	}
	return ot.Time
}

// Hilfen: IDs laden und Digest parsen
func fetchRepoIDs(ctx context.Context, tx *sql.Tx, repos []string) (map[string]int64, error) {
	if len(repos) == 0 {
		return map[string]int64{}, nil
	}
	args := make([]any, len(repos))
	sb := strings.Builder{}
	sb.WriteString(`SELECT repo_name, id FROM repos WHERE repo_name IN (`)
	for i := range repos {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("$%d", i+1))
		args[i] = repos[i]
	}
	sb.WriteString(")")
	rows, err := tx.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]int64, len(repos))
	for rows.Next() {
		var name string
		var id int64
		if err := rows.Scan(&name, &id); err != nil {
			return nil, err
		}
		out[name] = id
	}
	return out, rows.Err()
}

func fetchLayerIDs(ctx context.Context, tx *sql.Tx, digests [][]byte) (map[string]int64, error) {
	if len(digests) == 0 {
		return map[string]int64{}, nil
	}
	args := make([]any, len(digests))
	sb := strings.Builder{}
	sb.WriteString(`SELECT digest, id FROM layer_blobs WHERE digest IN (`)
	for i := range digests {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("$%d", i+1))
		args[i] = digests[i]
	}
	sb.WriteString(")")
	rows, err := tx.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]int64, len(digests))
	for rows.Next() {
		var d []byte
		var id int64
		if err := rows.Scan(&d, &id); err != nil {
			return nil, err
		}
		out[string(d)] = id
	}
	return out, rows.Err()
}

func digestToBytes(s string) ([]byte, error) {
	// akzeptiert "sha256:<hex>" oder "<hex>"
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[i+1:]
	}
	s = strings.TrimSpace(s)
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd hex len")
	}
	return hex.DecodeString(s)
}

func loadLayerPositions(ctx context.Context, db *sql.DB) (map[string]int, error) {
	rows, err := db.QueryContext(ctx, `SELECT r.repo_name, COALESCE(MAX(rl.position),0)
                                      FROM repo_layers rl
                                      JOIN repos r ON r.id = rl.repo_id
                                      GROUP BY r.repo_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int, 1024)
	for rows.Next() {
		var repo string
		var maxpos int
		if err := rows.Scan(&repo, &maxpos); err != nil {
			return nil, err
		}
		out[repo] = maxpos
	}
	return out, rows.Err()
}

func ensureSecretTables(db *sql.DB) error {
	ctx := context.Background()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS layer_data (
           layer_id BIGINT PRIMARY KEY REFERENCES layer_blobs(id) ON DELETE CASCADE,
           file_count INT,
           max_depth INT,
           uncompressed_size BIGINT
         )`,
		`CREATE TABLE IF NOT EXISTS secret_fragments (
           fragment_hash TEXT PRIMARY KEY,
           file TEXT,
           file_type TEXT,
           file_size BIGINT,
           origin TEXT,
           secret TEXT,
           start_line INT
         )`,
		`CREATE TABLE IF NOT EXISTS layer_secret_fragments (
           layer_id BIGINT REFERENCES layer_blobs(id) ON DELETE CASCADE,
           fragment_hash TEXT REFERENCES secret_fragments(fragment_hash) ON DELETE CASCADE,
           PRIMARY KEY (layer_id, fragment_hash)
         )`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}
