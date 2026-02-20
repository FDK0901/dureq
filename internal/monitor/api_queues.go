package monitor

import (
	"net/http"

	"github.com/FDK0901/dureq/internal/store"
)

// QueueInfo describes the state of a single queue/tier.
type QueueInfo struct {
	Name       string `json:"name"`
	Weight     int    `json:"weight"`
	FetchBatch int    `json:"fetch_batch"`
	Paused     bool   `json:"paused"`
	Size       int64  `json:"size"`
}

func (a *API) listQueues(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tiers := a.store.Config().Tiers
	paused, _ := a.store.ListPausedQueues(ctx)
	pausedSet := make(map[string]bool, len(paused))
	for _, p := range paused {
		pausedSet[p] = true
	}

	queues := make([]QueueInfo, 0, len(tiers))
	for _, tier := range tiers {
		size, _ := a.store.Client().Do(ctx, a.store.Client().B().Xlen().Key(store.WorkStreamKey(a.store.Prefix(), tier.Name)).Build()).AsInt64()
		queues = append(queues, QueueInfo{
			Name:       tier.Name,
			Weight:     tier.Weight,
			FetchBatch: tier.FetchBatch,
			Paused:     pausedSet[tier.Name],
			Size:       size,
		})
	}
	a.jsonOK(w, queues)
}

func (a *API) pauseQueue(w http.ResponseWriter, r *http.Request) {
	tierName := r.PathValue("tierName")
	if err := a.store.PauseQueue(r.Context(), tierName); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, map[string]string{"status": "paused", "queue": tierName})
}

func (a *API) resumeQueue(w http.ResponseWriter, r *http.Request) {
	tierName := r.PathValue("tierName")
	if err := a.store.UnpauseQueue(r.Context(), tierName); err != nil {
		a.jsonError(w, err, http.StatusInternalServerError)
		return
	}
	a.jsonOK(w, map[string]string{"status": "resumed", "queue": tierName})
}
