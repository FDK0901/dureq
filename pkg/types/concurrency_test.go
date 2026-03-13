package types

import (
	"encoding/json"
	"testing"
)

func TestConcurrencyKey_JSONRoundTrip(t *testing.T) {
	keys := []ConcurrencyKey{
		{Key: "tenant:acme", MaxConcurrency: 1},
		{Key: "resource:stripe-api", MaxConcurrency: 5},
	}

	data, err := json.Marshal(keys)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded []ConcurrencyKey
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(decoded) != 2 {
		t.Fatalf("expected 2, got %d", len(decoded))
	}
	if decoded[0].Key != "tenant:acme" || decoded[0].MaxConcurrency != 1 {
		t.Errorf("first key mismatch: %+v", decoded[0])
	}
	if decoded[1].Key != "resource:stripe-api" || decoded[1].MaxConcurrency != 5 {
		t.Errorf("second key mismatch: %+v", decoded[1])
	}
}

func TestConcurrencyKey_JobSerialization(t *testing.T) {
	job := &Job{
		ID:       "job-1",
		TaskType: "test",
		ConcurrencyKeys: []ConcurrencyKey{
			{Key: "tenant:acme", MaxConcurrency: 1},
		},
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}

	var decoded Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}

	if len(decoded.ConcurrencyKeys) != 1 {
		t.Fatalf("expected 1 concurrency key, got %d", len(decoded.ConcurrencyKeys))
	}
	if decoded.ConcurrencyKeys[0].Key != "tenant:acme" {
		t.Errorf("expected tenant:acme, got %s", decoded.ConcurrencyKeys[0].Key)
	}
}

func TestConcurrencyKey_WorkMessageSerialization(t *testing.T) {
	msg := WorkMessage{
		RunID:    "run-1",
		JobID:    "job-1",
		TaskType: "test",
		ConcurrencyKeys: []ConcurrencyKey{
			{Key: "tenant:acme", MaxConcurrency: 1},
			{Key: "resource:db", MaxConcurrency: 10},
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded WorkMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(decoded.ConcurrencyKeys) != 2 {
		t.Fatalf("expected 2, got %d", len(decoded.ConcurrencyKeys))
	}
}

func TestConcurrencyKey_OmittedWhenEmpty(t *testing.T) {
	job := &Job{
		ID:       "job-1",
		TaskType: "test",
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// ConcurrencyKeys should not appear in JSON when nil.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if _, exists := raw["concurrency_keys"]; exists {
		t.Error("concurrency_keys should be omitted when empty")
	}
}
