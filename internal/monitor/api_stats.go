package monitor

import (
	"net/http"
	"strconv"

	"github.com/FDK0901/dureq/internal/store"
)

// DailyStatsEntry represents aggregated stats for one day.
type DailyStatsEntry struct {
	Date      string `json:"date"`
	Processed int64  `json:"processed"`
	Failed    int64  `json:"failed"`
}

func (a *API) getDailyStats(w http.ResponseWriter, r *http.Request) {
	days := 7
	if d := r.URL.Query().Get("days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed > 0 && parsed <= 90 {
			days = parsed
		}
	}

	entries, err := a.store.GetDailyStats(r.Context(), days)
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	result := make([]DailyStatsEntry, 0, len(entries))
	for _, e := range entries {
		result = append(result, DailyStatsEntry{
			Date:      e.Date,
			Processed: e.Processed,
			Failed:    e.Failed,
		})
	}
	a.jsonOK(w, result)
}

func (a *API) getRedisInfo(w http.ResponseWriter, r *http.Request) {
	section := r.URL.Query().Get("section")
	if section == "" {
		section = "all"
	}

	info, err := a.store.Client().Do(r.Context(), a.store.Client().B().Info().Section(section).Build()).ToString()
	if err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}

	// Parse Redis INFO into sections.
	sections := store.ParseRedisInfo(info)
	a.jsonOK(w, sections)
}
