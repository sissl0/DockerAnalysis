package analysis

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

type FullTagStats struct {
	LastPushedMonth map[string]uint64
	StatusCounts    map[string]uint64
	LastPushedEpoch []int64 // seconds
	SizeSample      []int64 // bytes
}

type tagRec struct {
	LastPushed string  `json:"last_pushed"`
	Size       *int64  `json:"size"`
	Status     *string `json:"status"`
}

type FullRepoStats struct {
	PullCountSample  []int64
	StarCountSample  []int64
	IsOfficialCounts map[string]uint64
}

type repoRec struct {
	PullCount  *int64 `json:"pull_count"`
	StarCount  *int64 `json:"star_count"`
	IsOfficial bool   `json:"is_official"`
}

// Reservoir-Sampling (Algorithm R)
func reservoirAdd[T any](buf []T, k int, n int, v T) ([]T, int) {
	if n < k {
		return append(buf, v), n + 1
	}
	// fast LCG rand; ausreichend für Reservoir
	x := uint64((2862933555777941757*uint64(n) + 3037000493) & 0xFFFFFFFFFFFFFFFF)
	j := int(x % uint64(n+1))
	if j < k {
		buf[j] = v
	}
	return buf, n + 1
}

func monthKey(t time.Time) string {
	y, m, _ := t.UTC().Date()
	return fmt.Sprintf("%04d-%02d", y, int(m))
}

func parseRFC3339(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	// try common layouts
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05Z07:00"}
	for _, l := range layouts {
		if tt, err := time.Parse(l, s); err == nil {
			return tt, true
		}
	}
	return time.Time{}, false
}

func PrecomputeFullTagStats(ctx context.Context, tagsJSONL string, reservoir int) (*FullTagStats, error) {
	f, err := os.Open(tagsJSONL)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stats := &FullTagStats{
		LastPushedMonth: make(map[string]uint64, 4096),
		StatusCounts:    make(map[string]uint64, 128),
		LastPushedEpoch: make([]int64, 0, reservoir),
		SizeSample:      make([]int64, 0, reservoir),
	}

	r := bufio.NewReaderSize(f, 4*1024*1024)
	var nLP, nSZ int
	var lines uint64

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, e := r.ReadBytes('\n')
		if len(b) > 0 {
			lines++
			// trim newline
			if b[len(b)-1] == '\n' {
				b = b[:len(b)-1]
				if len(b) > 0 && b[len(b)-1] == '\r' {
					b = b[:len(b)-1]
				}
			}
			if len(b) > 0 {
				var t tagRec
				if err := json.Unmarshal(b, &t); err == nil {
					// last_pushed
					if tt, ok := parseRFC3339(t.LastPushed); ok {
						stats.LastPushedMonth[monthKey(tt)]++
						sec := tt.Unix()
						stats.LastPushedEpoch, nLP = reservoirAdd(stats.LastPushedEpoch, reservoir, nLP, sec)
					}
					// size
					if t.Size != nil && *t.Size >= 0 {
						stats.SizeSample, nSZ = reservoirAdd(stats.SizeSample, reservoir, nSZ, *t.Size)
					}
					// status
					if t.Status != nil {
						s := *t.Status
						if s == "" {
							s = "unknown"
						}
						stats.StatusCounts[s]++
					} else {
						stats.StatusCounts["unknown"]++
					}
				}
			}
			if lines%5_000_000 == 0 {
				fmt.Fprintf(os.Stderr, "[precompute] lines=%d\n", lines)
			}
		}
		if e != nil {
			if e.Error() == "EOF" {
				break
			}
			if e != nil {
				return nil, e
			}
		}
	}
	return stats, nil
}

func PrecomputeFullRepoStats(ctx context.Context, reposJSONL string, reservoir int) (*FullRepoStats, error) {
	f, err := os.Open(reposJSONL)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stats := &FullRepoStats{
		PullCountSample:  make([]int64, 0, reservoir),
		StarCountSample:  make([]int64, 0, reservoir),
		IsOfficialCounts: make(map[string]uint64, 4),
	}

	r := bufio.NewReaderSize(f, 4*1024*1024)
	var nPull, nStar int
	var lines uint64

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, e := r.ReadBytes('\n')
		if len(b) > 0 {
			lines++
			// trim newline
			if b[len(b)-1] == '\n' {
				b = b[:len(b)-1]
				if len(b) > 0 && b[len(b)-1] == '\r' {
					b = b[:len(b)-1]
				}
			}
			if len(b) > 0 {
				var rec repoRec
				if err := json.Unmarshal(b, &rec); err == nil {
					// pull_count
					if rec.PullCount != nil && *rec.PullCount >= 0 {
						stats.PullCountSample, nPull = reservoirAdd(stats.PullCountSample, reservoir, nPull, *rec.PullCount)
					}
					// star_count (optional, falls du es auswerten willst)
					if rec.StarCount != nil && *rec.StarCount >= 0 {
						stats.StarCountSample, nStar = reservoirAdd(stats.StarCountSample, reservoir, nStar, *rec.StarCount)
					}
					// is_official
					if rec.IsOfficial {
						stats.IsOfficialCounts["official"]++
					} else {
						stats.IsOfficialCounts["unofficial"]++
					}
				}
			}
			if lines%5_000_000 == 0 {
				fmt.Fprintf(os.Stderr, "[precompute_repos] lines=%d\n", lines)
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
	}
	return stats, nil
}

func WriteHistogramCSV(m map[string]uint64, path string) error {
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()
	w := csv.NewWriter(out)
	defer w.Flush()
	_ = w.Write([]string{"key", "count"})
	for k, v := range m {
		_ = w.Write([]string{k, strconv.FormatUint(v, 10)})
	}
	return w.Error()
}

func WriteSeriesCSV(vals []int64, path string) error {
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()
	w := csv.NewWriter(out)
	defer w.Flush()
	_ = w.Write([]string{"value"})
	for _, v := range vals {
		_ = w.Write([]string{strconv.FormatInt(v, 10)})
	}
	return w.Error()
}
