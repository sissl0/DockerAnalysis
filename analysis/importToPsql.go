package analysis

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	pq "github.com/lib/pq" // was: _ "github.com/lib/pq"
)

// ---------- Input JSON line models ----------

type digestAnalysisLine struct {
	Digest    string   `json:"digest"`
	Size      int64    `json:"size"`
	Repos     []string `json:"repos"`
	RepoCount int      `json:"repo_count"`
}

type repoLine struct {
	IsAutomated      bool   `json:"is_automated"`
	IsOfficial       bool   `json:"is_official"`
	PullCount        int64  `json:"pull_count"`
	RepoName         string `json:"repo_name"`
	RepoOwner        string `json:"repo_owner"`
	ShortDescription string `json:"short_description"`
	StarCount        int64  `json:"star_count"`
}

type tagLine struct {
	Architecture string  `json:"architecture"`
	Digest       string  `json:"digest"`
	LastPulled   string  `json:"last_pulled"`
	LastPushed   string  `json:"last_pushed"`
	OS           string  `json:"os"`
	RepoName     string  `json:"repo_name"`
	Size         *int64  `json:"size"`
	Status       *string `json:"status"`
}

type layerLineFull struct {
	Digest           string `json:"digest"`
	FileCount        *int32 `json:"file_count"`
	MaxDepth         *int32 `json:"max_depth"`
	UncompressedSize *int64 `json:"uncompressed_size"`
	// Optional, kann fehlen
	Secrets []string `json:"secrets"`
}

type secretOcc struct {
	FragmentHash string `json:"fragment_hash"`
	Secrets      struct {
		File      string `json:"file"`
		FileType  string `json:"file_type"`
		FileSize  *int64 `json:"file_size"`
		Origin    string `json:"origin"`
		Secret    string `json:"secret"`
		StartLine int32  `json:"start_line"`
	} `json:"secrets"`
}

// ---------- Internal state ----------

type digestState struct {
	Digest       string
	CompressedSz int64 // aus digest_analysis.size
	Repos        map[string]struct{}
}

type repoInfo struct {
	// normalized name as key
	ID               int64
	IsAutomated      bool
	IsOfficial       bool
	PullCount        int64
	RepoName         string
	RepoOwner        string
	ShortDescription string
	StarCount        int64
}

// ---------- Public entry point ----------

// ImportSelectedToPostgres importiert nur die Einträge, deren Layer-Digest in digest_analysis.jsonl vorkommt.
// Erwartete Dateien:
//   - digestAnalysisPath: JSONL mit {digest,size,repos,repo_count}
//   - uniqueReposPath: JSONL mit Repository-Metadaten
//   - combinedTagsPath: JSONL mit Tag-Metadaten (muss digest + repo_name enthalten)
//   - combinedLayersPath: JSONL mit Layer-Metadaten (digest, file_count, max_depth, uncompressed_size[, secrets])
//   - combinedSecretsPath: JSONL Zeile je Layer: null oder Liste von Secret-Vorkommen
func ImportSelectedToPostgres(
	ctx context.Context,
	postgresURL string,
	digestAnalysisPath string,
	uniqueReposPath string,
	combinedTagsPath string,
	combinedLayersPath string,
	combinedSecretsPath string,
	batchSize int,
) error {
	if batchSize <= 0 {
		batchSize = 5000
	}

	db, err := sql.Open("postgres", postgresURL)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	// 1) Auswahl laden
	selection, selectedReposCanon, err := loadDigestSelection(ctx, digestAnalysisPath)
	if err != nil {
		return fmt.Errorf("loadDigestSelection: %w", err)
	}
	// Whitelist inkl. Aliase (library/<name> <-> <name>)
	repoWhitelist := buildRepoWhitelist(selectedReposCanon)
	fmt.Fprintf(os.Stderr, "[selection] digests=%d repos=%d (whitelist=%d)\n", len(selection), len(selectedReposCanon), len(repoWhitelist))

	// 2) Repositories importieren (nur Whitelist)
	repoIDs, err := importRepositories(ctx, db, uniqueReposPath, repoWhitelist, batchSize)
	if err != nil {
		return fmt.Errorf("importRepositories: %w", err)
	}

	// Fehlende (nur kanonische Namen) minimal anlegen
	if err := ensureMissingRepos(ctx, db, selectedReposCanon, repoIDs, batchSize); err != nil {
		return fmt.Errorf("ensureMissingRepos: %w", err)
	}

	// 3) Layers importieren und fragment_hash -> layer_id Map bauen
	layerIDs, fragToLayerIDs, err := importLayers(ctx, db, combinedLayersPath, selection, batchSize)
	if err != nil {
		return fmt.Errorf("importLayers: %w", err)
	}
	// minimal fehlende Layer anlegen
	if err := ensureMissingLayers(ctx, db, selection, layerIDs, batchSize); err != nil {
		return fmt.Errorf("ensureMissingLayers: %w", err)
	}

	// 4) Repo<->Layer Verknüpfungen aus digest_analysis
	if err := linkReposToLayers(ctx, db, selection, repoIDs, layerIDs, batchSize); err != nil {
		return fmt.Errorf("linkReposToLayers: %w", err)
	}

	// 5) Tags importieren (Repo-Whitelist)
	if err := importTags(ctx, db, combinedTagsPath, repoWhitelist, repoIDs, batchSize); err != nil {
		return fmt.Errorf("importTags: %w", err)
	}

	// 6) Secrets importieren (per fragment_hash -> layer_id)
	if err := importSecrets(ctx, db, combinedSecretsPath, fragToLayerIDs, batchSize); err != nil {
		return fmt.Errorf("importSecrets: %w", err)
	}

	fmt.Fprintln(os.Stderr, "Import finished successfully.")
	return nil
}

// Secrets-only Pipeline (überspringt Repos/Tags/Layers-Insert)
func ImportOnlySecrets(
	ctx context.Context,
	postgresURL string,
	digestAnalysisPath string,
	combinedLayersPath string,
	combinedSecretsPath string,
	batchSize int,
) error {
	if batchSize <= 0 {
		batchSize = 5000
	}
	db, err := sql.Open("postgres", postgresURL)
	if err != nil {
		return err
	}
	defer db.Close()

	selection, _, err := loadDigestSelection(ctx, digestAnalysisPath)
	if err != nil {
		return fmt.Errorf("loadDigestSelection: %w", err)
	}

	// Layer-IDs aus DB holen (bereits vorhanden)
	layerIDs, err := fetchLayerIDsFromDB(ctx, db, selection, batchSize)
	if err != nil {
		return fmt.Errorf("fetchLayerIDsFromDB: %w", err)
	}
	// fragment_hash -> []layer_id, aus Datei aufgebaut
	fragToLayerIDs, err := buildFragToLayerIDsFromFile(ctx, combinedLayersPath, selection, layerIDs)
	if err != nil {
		return fmt.Errorf("buildFragToLayerIDsFromFile: %w", err)
	}

	// Secrets importieren
	if err := importSecrets(ctx, db, combinedSecretsPath, fragToLayerIDs, batchSize); err != nil {
		return fmt.Errorf("importSecrets: %w", err)
	}
	return nil
}

// ---------- Step 1: selection ----------

func loadDigestSelection(ctx context.Context, path string) (map[string]*digestState, map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 4*1024*1024)
	selected := make(map[string]*digestState, 1_000_000)
	selectedRepos := make(map[string]struct{}, 200_000)

	var lines uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		b, e := r.ReadBytes('\n')
		if len(b) > 0 {
			lines++
			b = trimNewline(b)
			if len(b) > 0 {
				var dl digestAnalysisLine
				if jsonErr := json.Unmarshal(b, &dl); jsonErr == nil && dl.Digest != "" {
					st := &digestState{
						Digest:       strings.TrimSpace(dl.Digest),
						CompressedSz: dl.Size,
						Repos:        map[string]struct{}{},
					}
					for _, repo := range dl.Repos {
						repo = strings.TrimSpace(repo)
						if repo == "" {
							continue
						}
						st.Repos[repo] = struct{}{}
						selectedRepos[repo] = struct{}{}
					}
					selected[st.Digest] = st
				}
			}
			if lines%1_000_000 == 0 {
				fmt.Fprintf(os.Stderr, "[selection] lines=%d digests=%d repos=%d\n", lines, len(selected), len(selectedRepos))
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, nil, e
		}
	}
	if len(selected) == 0 {
		return nil, nil, errors.New("digest selection is empty")
	}
	return selected, selectedRepos, nil
}

// ---------- Step 2: repositories ----------

func importRepositories(ctx context.Context, db *sql.DB, path string, repoWhitelist map[string]struct{}, batchSize int) (map[string]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	type repoRow struct {
		repo repoLine
	}
	rows := make([]repoRow, 0, batchSize)
	repoIDs := make(map[string]int64, len(repoWhitelist))

	flush := func(tx *sql.Tx, batch []repoRow) error {
		if len(batch) == 0 {
			return nil
		}
		stmt, err := tx.PrepareContext(ctx, `
            INSERT INTO repositories (repo_name, repo_owner, short_description, is_automated, is_official, pull_count, star_count)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (repo_name) DO UPDATE
            SET repo_owner=EXCLUDED.repo_owner,
                short_description=EXCLUDED.short_description,
                is_automated=EXCLUDED.is_automated,
                is_official=EXCLUDED.is_official,
                pull_count=EXCLUDED.pull_count,
                star_count=EXCLUDED.star_count
            RETURNING id, repo_name`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, rr := range batch {
			r := rr.repo
			var id int64
			if err := stmt.QueryRowContext(ctx,
				r.RepoName, r.RepoOwner, r.ShortDescription, r.IsAutomated, r.IsOfficial, r.PullCount, r.StarCount,
			).Scan(&id, &r.RepoName); err != nil {
				return err
			}
			repoIDs[normRepoName(r.RepoName)] = id
		}
		return nil
	}

	r := bufio.NewReaderSize(f, 4*1024*1024)
	var lines uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, e := r.ReadBytes('\n')
		if len(b) > 0 {
			lines++
			b = trimNewline(b)
			if len(b) > 0 {
				var rl repoLine
				if jsonErr := json.Unmarshal(b, &rl); jsonErr == nil && rl.RepoName != "" {
					// Filter via Whitelist (inkl. Aliase)
					include := false
					for _, k := range expandRepoAliases(rl.RepoName) {
						if _, ok := repoWhitelist[k]; ok {
							include = true
							break
						}
					}
					if include {
						rows = append(rows, repoRow{repo: rl})
						if len(rows) >= batchSize {
							if err := withTx(ctx, db, func(tx *sql.Tx) error {
								return flush(tx, rows)
							}); err != nil {
								return nil, err
							}
							rows = rows[:0]
						}
					}
				}
			}
			if lines%1_000_000 == 0 {
				fmt.Fprintf(os.Stderr, "[repos] lines=%d inserted=%d\n", lines, len(repoIDs))
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
	}
	if len(rows) > 0 {
		if err := withTx(ctx, db, func(tx *sql.Tx) error {
			return flush(tx, rows)
		}); err != nil {
			return nil, err
		}
	}
	return repoIDs, nil
}

func ensureMissingRepos(ctx context.Context, db *sql.DB, selectedRepos map[string]struct{}, repoIDs map[string]int64, batchSize int) error {
	missing := make([]string, 0)
	for r := range selectedRepos {
		if _, ok := repoIDs[r]; !ok {
			missing = append(missing, r)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	fmt.Fprintf(os.Stderr, "[repos] creating minimal for %d missing\n", len(missing))

	for i := 0; i < len(missing); i += batchSize {
		j := i + batchSize
		if j > len(missing) {
			j = len(missing)
		}
		batch := missing[i:j]
		if err := withTx(ctx, db, func(tx *sql.Tx) error {
			stmt, err := tx.PrepareContext(ctx, `
                INSERT INTO repositories (repo_name)
                VALUES ($1)
                ON CONFLICT (repo_name) DO UPDATE SET repo_name=EXCLUDED.repo_name
                RETURNING id, repo_name`)
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, name := range batch {
				var id int64
				var rn string
				if err := stmt.QueryRowContext(ctx, name).Scan(&id, &rn); err != nil {
					return err
				}
				repoIDs[rn] = id
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// ---------- Step 3: layers ----------

func importLayers(ctx context.Context, db *sql.DB, path string, selection map[string]*digestState, batchSize int) (map[string]int64, map[string][]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	type layerRow struct {
		digest           string
		compressedSize   *int64
		uncompressedSize *int64
		fileCount        *int32
		maxDepth         *int32
	}
	rows := make([]layerRow, 0, batchSize)
	layerIDs := make(map[string]int64, len(selection))
	found := make(map[string]struct{}, len(selection))

	// Sammle fragment_hashes je Layer-Digest
	digestToFragments := make(map[string][]string, len(selection))

	flush := func(tx *sql.Tx, batch []layerRow) error {
		if len(batch) == 0 {
			return nil
		}
		stmt, err := tx.PrepareContext(ctx, `
            INSERT INTO layers (digest, compressed_size, uncompressed_size, file_count, max_depth)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (digest) DO UPDATE
            SET compressed_size = COALESCE(EXCLUDED.compressed_size, layers.compressed_size),
                uncompressed_size = COALESCE(EXCLUDED.uncompressed_size, layers.uncompressed_size),
                file_count = COALESCE(EXCLUDED.file_count, layers.file_count),
                max_depth = COALESCE(EXCLUDED.max_depth, layers.max_depth)
            RETURNING id, digest`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, lr := range batch {
			var id int64
			var digest string
			if err := stmt.QueryRowContext(ctx,
				lr.digest, lr.compressedSize, lr.uncompressedSize, lr.fileCount, lr.maxDepth,
			).Scan(&id, &digest); err != nil {
				return err
			}
			layerIDs[digest] = id
			found[digest] = struct{}{}
		}
		return nil
	}

	r := bufio.NewReaderSize(f, 4*1024*1024)
	var lines uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		b, e := r.ReadBytes('\n')
		if len(b) > 0 {
			lines++
			b = trimNewline(b)
			if len(b) > 0 {
				var ll layerLineFull
				if jsonErr := json.Unmarshal(b, &ll); jsonErr == nil && ll.Digest != "" {
					if st, ok := selection[ll.Digest]; ok {
						if len(ll.Secrets) > 0 {
							// kopieren um späteren Mutationen vorzubeugen
							frags := make([]string, len(ll.Secrets))
							copy(frags, ll.Secrets)
							digestToFragments[ll.Digest] = frags
						}
						cs := st.CompressedSz
						rows = append(rows, layerRow{
							digest:           ll.Digest,
							compressedSize:   &cs,
							uncompressedSize: ll.UncompressedSize,
							fileCount:        ll.FileCount,
							maxDepth:         ll.MaxDepth,
						})
						if len(rows) >= batchSize {
							if err := withTx(ctx, db, func(tx *sql.Tx) error { return flush(tx, rows) }); err != nil {
								return nil, nil, err
							}
							rows = rows[:0]
						}
					}
				}
			}
			if lines%1_000_000 == 0 {
				fmt.Fprintf(os.Stderr, "[layers] lines=%d inserted=%d\n", lines, len(layerIDs))
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, nil, e
		}
	}
	if len(rows) > 0 {
		if err := withTx(ctx, db, func(tx *sql.Tx) error { return flush(tx, rows) }); err != nil {
			return nil, nil, err
		}
	}

	// fragment_hash -> []layer_id aufbauen (nur für selektierte Layer)
	fragToLayerIDs := make(map[string][]int64, len(digestToFragments))
	for d, frags := range digestToFragments {
		if lid, ok := layerIDs[d]; ok {
			for _, fh := range frags {
				fragToLayerIDs[fh] = append(fragToLayerIDs[fh], lid)
			}
		}
	}

	return layerIDs, fragToLayerIDs, nil
}

func ensureMissingLayers(ctx context.Context, db *sql.DB, selection map[string]*digestState, layerIDs map[string]int64, batchSize int) error {
	missing := make([]string, 0)
	for d, st := range selection {
		if _, ok := layerIDs[d]; !ok {
			missing = append(missing, d)
			_ = st // keep
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	fmt.Fprintf(os.Stderr, "[layers] creating minimal for %d missing\n", len(missing))

	for i := 0; i < len(missing); i += batchSize {
		j := i + batchSize
		if j > len(missing) {
			j = len(missing)
		}
		batch := missing[i:j]
		if err := withTx(ctx, db, func(tx *sql.Tx) error {
			stmt, err := tx.PrepareContext(ctx, `
                INSERT INTO layers (digest, compressed_size)
                VALUES ($1,$2)
                ON CONFLICT (digest) DO UPDATE
                SET compressed_size = COALESCE(EXCLUDED.compressed_size, layers.compressed_size)
                RETURNING id, digest`)
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, d := range batch {
				cs := selection[d].CompressedSz
				var id int64
				var dd string
				if err := stmt.QueryRowContext(ctx, d, cs).Scan(&id, &dd); err != nil {
					return err
				}
				layerIDs[dd] = id
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// ---------- Step 4: repo_layers ----------

func linkReposToLayers(ctx context.Context, db *sql.DB, selection map[string]*digestState, repoIDs map[string]int64, layerIDs map[string]int64, batchSize int) error {
	type rl struct{ repoID, layerID int64 }
	buf := make([]rl, 0, batchSize)

	flush := func(tx *sql.Tx, batch []rl) error {
		if len(batch) == 0 {
			return nil
		}
		stmt, err := tx.PrepareContext(ctx, `
            INSERT INTO repo_layers (repo_id, layer_id)
            VALUES ($1,$2)
            ON CONFLICT DO NOTHING`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, x := range batch {
			if _, err := stmt.ExecContext(ctx, x.repoID, x.layerID); err != nil {
				return err
			}
		}
		return nil
	}

	for d, st := range selection {
		lid, ok := layerIDs[d]
		if !ok {
			return fmt.Errorf("missing layer id for digest %s", d)
		}
		for r := range st.Repos {
			rid, rok := findRepoID(repoIDs, r)
			if !rok {
				return fmt.Errorf("missing repo id for repo %s", r)
			}
			buf = append(buf, rl{repoID: rid, layerID: lid})
			if len(buf) >= batchSize {
				if err := withTx(ctx, db, func(tx *sql.Tx) error { return flush(tx, buf) }); err != nil {
					return err
				}
				buf = buf[:0]
			}
		}
	}
	if len(buf) > 0 {
		if err := withTx(ctx, db, func(tx *sql.Tx) error { return flush(tx, buf) }); err != nil {
			return err
		}
	}
	return nil
}

var tagDigestRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`) // lower-case sha256

// ---------- Step 5: tags ----------

func importTags(ctx context.Context, db *sql.DB, path string, repoWhitelist map[string]struct{}, repoIDs map[string]int64, batchSize int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	type row struct {
		repoID int64
		t      tagLine
	}
	rows := make([]row, 0, batchSize)

	flush := func(tx *sql.Tx, batch []row) error {
		if len(batch) == 0 {
			return nil
		}
		stmt, err := tx.PrepareContext(ctx, `
            INSERT INTO tags (repo_id, digest, architecture, os, size, status, last_pulled, last_pushed)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (digest) DO UPDATE
            SET repo_id=EXCLUDED.repo_id,
                architecture=EXCLUDED.architecture,
                os=EXCLUDED.os,
                size=EXCLUDED.size,
                status=EXCLUDED.status,
                last_pulled=EXCLUDED.last_pulled,
                last_pushed=EXCLUDED.last_pushed`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, rr := range batch {
			lp, _ := parseTime(rr.t.LastPulled)
			lps, _ := parseTime(rr.t.LastPushed)
			var status sql.NullString
			if rr.t.Status != nil && *rr.t.Status != "" {
				status = sql.NullString{String: *rr.t.Status, Valid: true}
			}
			var size sql.NullInt64
			if rr.t.Size != nil {
				size = sql.NullInt64{Int64: *rr.t.Size, Valid: true}
			}
			if _, err := stmt.ExecContext(ctx,
				rr.repoID, rr.t.Digest, n2s(rr.t.Architecture), n2s(rr.t.OS), size, status, lp, lps,
			); err != nil {
				return err
			}
		}
		return nil
	}

	// Filter nur per Repo-Whitelist; Repo-ID via findRepoID auflösen (inkl. Aliase)
	r := bufio.NewReaderSize(f, 4*1024*1024)
	var lines uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, e := r.ReadBytes('\n')
		if len(b) > 0 {
			lines++
			b = trimNewline(b)
			if len(b) > 0 {
				var tl tagLine
				if jsonErr := json.Unmarshal(b, &tl); jsonErr == nil && tl.RepoName != "" {
					var whitelisted bool
					for _, k := range expandRepoAliases(tl.RepoName) {
						if _, ok := repoWhitelist[k]; ok {
							whitelisted = true
							break
						}
					}
					if whitelisted {
						// normalize/validate digest to satisfy DB CHECK
						dig := strings.ToLower(strings.TrimSpace(tl.Digest))
						if !tagDigestRe.MatchString(dig) {
							// skip invalid tag digests (prevents CHECK violation)
							continue
						}
						tl.Digest = dig

						if rid, ok := findRepoID(repoIDs, tl.RepoName); ok {
							rows = append(rows, row{repoID: rid, t: tl})
							if len(rows) >= batchSize {
								if err := withTx(ctx, db, func(tx *sql.Tx) error { return flush(tx, rows) }); err != nil {
									return err
								}
								rows = rows[:0]
							}
						}
					}
				}
			}
			if lines%1_000_000 == 0 {
				fmt.Fprintf(os.Stderr, "[tags] lines=%d\n", lines)
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
	}
	if len(rows) > 0 {
		if err := withTx(ctx, db, func(tx *sql.Tx) error { return flush(tx, rows) }); err != nil {
			return err
		}
	}
	return nil
}

// ---------- Step 6: secrets ----------

// Hinweis: Wir verknüpfen Secrets per fragment_hash -> layer_id Mapping
func importSecrets(ctx context.Context, db *sql.DB, path string, fragToLayerIDs map[string][]int64, batchSize int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	type occRow struct {
		layerID    int64
		fragment   string
		origin     string
		secretText string
		file       string
		fileType   string
		fileSize   *int64
		startLine  int32
	}
	rows := make([]occRow, 0, batchSize)

	flush := func(tx *sql.Tx, batch []occRow) error {
		if len(batch) == 0 {
			return nil
		}
		// upsert secret, then insert occurrence
		upsertSecret, err := tx.PrepareContext(ctx, `
            INSERT INTO secrets (fragment_hash, origin, secret)
            VALUES ($1,$2,$3)
            ON CONFLICT (fragment_hash) DO UPDATE
            SET origin = COALESCE(EXCLUDED.origin, secrets.origin),
                secret = COALESCE(EXCLUDED.secret, secrets.secret)
            RETURNING id`)
		if err != nil {
			return err
		}
		defer upsertSecret.Close()

		insertOcc, err := tx.PrepareContext(ctx, `
            INSERT INTO layer_secret_occurrences (layer_id, secret_id, file, file_type, file_size, start_line)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT DO NOTHING`)
		if err != nil {
			return err
		}
		defer insertOcc.Close()

		// cache for secret_id to reduce roundtrips inside the same tx
		secretIDCache := make(map[string]int64, 2*len(batch))
		for _, rr := range batch {
			sid, ok := secretIDCache[rr.fragment]
			if !ok {
				if err := upsertSecret.QueryRowContext(ctx, rr.fragment, rr.origin, rr.secretText).Scan(&sid); err != nil {
					return err
				}
				secretIDCache[rr.fragment] = sid
			}
			if _, err := insertOcc.ExecContext(ctx,
				rr.layerID, sid, rr.file, rr.fileType, sqlNullInt(rr.fileSize), rr.startLine,
			); err != nil {
				return err
			}
		}
		return nil
	}

	r := bufio.NewReaderSize(f, 4*1024*1024)
	var lines uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, e := r.ReadBytes('\n')
		if len(b) > 0 {
			lines++
			b = trimNewline(b)
			if len(b) > 0 && string(b) != "null" {
				var list []secretOcc
				if jsonErr := json.Unmarshal(b, &list); jsonErr == nil {
					for _, it := range list {
						fh := strings.TrimSpace(it.FragmentHash)
						if fh == "" {
							continue
						}
						layerIDs := fragToLayerIDs[fh]
						if len(layerIDs) == 0 {
							continue
						}
						// NUL-Bytes entfernen (Postgres verbietet 0x00 in TEXT)
						origin := stripNulls(it.Secrets.Origin)
						secretText := stripNulls(it.Secrets.Secret)
						file := stripNulls(it.Secrets.File) // NOT NULL
						fileType := stripNulls(it.Secrets.FileType)

						for _, lid := range layerIDs {
							rows = append(rows, occRow{
								layerID:    lid,
								fragment:   fh,
								origin:     origin,
								secretText: secretText,
								file:       file,
								fileType:   fileType,
								fileSize:   it.Secrets.FileSize,
								startLine:  it.Secrets.StartLine,
							})
							if len(rows) >= batchSize {
								if err := withTx(ctx, db, func(tx *sql.Tx) error { return flush(tx, rows) }); err != nil {
									return err
								}
								rows = rows[:0]
							}
						}
					}
				}
			}
			if lines%1_000_000 == 0 {
				fmt.Fprintf(os.Stderr, "[secrets] lines=%d\n", lines)
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
	}
	if len(rows) > 0 {
		if err := withTx(ctx, db, func(tx *sql.Tx) error { return flush(tx, rows) }); err != nil {
			return err
		}
	}
	return nil
}

// ---------- helpers ----------

func withTx(ctx context.Context, db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func trimNewline(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
		if len(b) > 0 && b[len(b)-1] == '\r' {
			b = b[:len(b)-1]
		}
	}
	return b
}

func parseTime(s string) (sql.NullTime, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return sql.NullTime{}, nil
	}
	// Try multiple layouts
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05Z07:00",
	}
	var t time.Time
	var err error
	for _, l := range layouts {
		t, err = time.Parse(l, s)
		if err == nil {
			return sql.NullTime{Time: t, Valid: true}, nil
		}
	}
	return sql.NullTime{}, err
}

func n2s(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func sqlNullInt(p *int64) sql.NullInt64 {
	if p == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *p, Valid: true}
}

func normRepoName(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func expandRepoAliases(name string) []string {
	n := normRepoName(name)
	// library/<name> <-> <name> (Docker Hub offizielle images)
	if strings.HasPrefix(n, "library/") {
		return []string{n, strings.TrimPrefix(n, "library/")}
	}
	if !strings.Contains(n, "/") {
		return []string{n, "library/" + n}
	}
	return []string{n}
}

func buildRepoWhitelist(selectedReposCanon map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(selectedReposCanon)*2)
	for r := range selectedReposCanon {
		for _, k := range expandRepoAliases(r) {
			out[k] = struct{}{}
		}
	}
	return out
}

func findRepoID(repoIDs map[string]int64, name string) (int64, bool) {
	n := normRepoName(name)
	if id, ok := repoIDs[n]; ok {
		return id, true
	}
	// Aliase versuchen
	if strings.HasPrefix(n, "library/") {
		if id, ok := repoIDs[strings.TrimPrefix(n, "library/")]; ok {
			return id, true
		}
	} else if !strings.Contains(n, "/") {
		if id, ok := repoIDs["library/"+n]; ok {
			return id, true
		}
	}
	return 0, false
}

func stripNulls(s string) string {
	// Entfernt alle NUL-Bytes (0x00), die Postgres-INSERTs brechen würden
	if strings.IndexByte(s, 0) == -1 {
		return s
	}
	return strings.Map(func(r rune) rune {
		if r == 0 {
			return -1
		}
		return r
	}, s)
}

func fetchLayerIDsFromDB(ctx context.Context, db *sql.DB, selection map[string]*digestState, batchSize int) (map[string]int64, error) {
	// Digests in Chunks abfragen
	digests := make([]string, 0, len(selection))
	for d := range selection {
		digests = append(digests, d)
	}
	layerIDs := make(map[string]int64, len(digests))

	for i := 0; i < len(digests); i += batchSize {
		j := i + batchSize
		if j > len(digests) {
			j = len(digests)
		}
		chunk := digests[i:j]
		rows, err := db.QueryContext(ctx,
			`SELECT id, digest FROM layers WHERE digest = ANY($1)`,
			pq.Array(chunk),
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var d string
			if err := rows.Scan(&id, &d); err != nil {
				_ = rows.Close()
				return nil, err
			}
			layerIDs[d] = id
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	if len(layerIDs) == 0 {
		return nil, errors.New("no layer IDs found in DB for selection")
	}
	return layerIDs, nil
}

func buildFragToLayerIDsFromFile(ctx context.Context, path string, selection map[string]*digestState, layerIDs map[string]int64) (map[string][]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 4*1024*1024)
	fragToLayerIDs := make(map[string][]int64, 1<<12)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, e := r.ReadBytes('\n')
		if len(b) > 0 {
			b = trimNewline(b)
			if len(b) > 0 {
				var ll layerLineFull
				if jsonErr := json.Unmarshal(b, &ll); jsonErr == nil && ll.Digest != "" {
					if _, ok := selection[ll.Digest]; ok && len(ll.Secrets) > 0 {
						if lid, ok := layerIDs[ll.Digest]; ok {
							for _, fh := range ll.Secrets {
								fragToLayerIDs[fh] = append(fragToLayerIDs[fh], lid)
							}
						}
					}
				}
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
	}
	return fragToLayerIDs, nil
}
