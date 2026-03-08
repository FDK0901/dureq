package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

// DailyStats holds processed/failed counts for a single date.
type DailyStats struct {
	Date      string `json:"date"`
	Processed int64  `json:"processed"`
	Failed    int64  `json:"failed"`
}

// IncrDailyStat atomically increments the processed or failed counter for today.
func (s *RedisStore) IncrDailyStat(ctx context.Context, field string) {
	date := time.Now().Format("2006-01-02")
	key := DailyStatsKey(s.prefix, date)
	cmds := make(rueidis.Commands, 0, 2)
	cmds = append(cmds, s.rdb.B().Hincrby().Key(key).Field(field).Increment(1).Build())
	// Set TTL of 91 days so stats auto-expire.
	cmds = append(cmds, s.rdb.B().Expire().Key(key).Seconds(int64((91 * 24 * time.Hour).Seconds())).Build())
	s.rdb.DoMulti(ctx, cmds...)
}

// GetDailyStats returns daily stats for the last N days.
func (s *RedisStore) GetDailyStats(ctx context.Context, days int) ([]DailyStats, error) {
	result := make([]DailyStats, 0, days)
	now := time.Now()

	for i := days - 1; i >= 0; i-- {
		date := now.AddDate(0, 0, -i).Format("2006-01-02")
		key := DailyStatsKey(s.prefix, date)

		vals, err := s.rdb.Do(ctx, s.rdb.B().Hgetall().Key(key).Build()).AsStrMap()
		if err != nil {
			return nil, fmt.Errorf("get daily stats for %s: %w", date, err)
		}

		entry := DailyStats{Date: date}
		if v, ok := vals["processed"]; ok {
			entry.Processed, _ = strconv.ParseInt(v, 10, 64)
		}
		if v, ok := vals["failed"]; ok {
			entry.Failed, _ = strconv.ParseInt(v, 10, 64)
		}
		result = append(result, entry)
	}
	return result, nil
}

// ParseRedisInfo parses raw Redis INFO output into a map of sections.
func ParseRedisInfo(raw string) map[string]map[string]string {
	sections := make(map[string]map[string]string)
	currentSection := "general"

	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "# ") {
			currentSection = strings.ToLower(strings.TrimPrefix(line, "# "))
			if sections[currentSection] == nil {
				sections[currentSection] = make(map[string]string)
			}
			continue
		}
		if idx := strings.Index(line, ":"); idx > 0 {
			if sections[currentSection] == nil {
				sections[currentSection] = make(map[string]string)
			}
			sections[currentSection][line[:idx]] = line[idx+1:]
		}
	}
	return sections
}
