package archival

import (
	"testing"
	"time"
)

func TestConfig_DefaultRetentionPeriod(t *testing.T) {
	// Negative value triggers the 7-day default.
	c := New(Config{RetentionPeriod: -1})
	expected := 7 * 24 * time.Hour
	if c.retentionPeriod != expected {
		t.Fatalf("expected default RetentionPeriod %v, got %v", expected, c.retentionPeriod)
	}
}

func TestConfig_ZeroRetentionDisablesArchival(t *testing.T) {
	c := New(Config{})
	if c.retentionPeriod != 0 {
		t.Fatalf("expected RetentionPeriod 0 (disabled), got %v", c.retentionPeriod)
	}
}

func TestConfig_DefaultScanInterval(t *testing.T) {
	c := New(Config{})
	expected := 1 * time.Hour
	if c.scanInterval != expected {
		t.Fatalf("expected default ScanInterval %v, got %v", expected, c.scanInterval)
	}
}

func TestConfig_DefaultBatchSize(t *testing.T) {
	c := New(Config{})
	if c.batchSize != 100 {
		t.Fatalf("expected default BatchSize 100, got %d", c.batchSize)
	}
}

func TestConfig_CustomValues(t *testing.T) {
	c := New(Config{
		RetentionPeriod: 48 * time.Hour,
		ScanInterval:    30 * time.Minute,
		BatchSize:       50,
	})

	if c.retentionPeriod != 48*time.Hour {
		t.Fatalf("expected RetentionPeriod 48h, got %v", c.retentionPeriod)
	}
	if c.scanInterval != 30*time.Minute {
		t.Fatalf("expected ScanInterval 30m, got %v", c.scanInterval)
	}
	if c.batchSize != 50 {
		t.Fatalf("expected BatchSize 50, got %d", c.batchSize)
	}
}

func TestConfig_DefaultLogger(t *testing.T) {
	c := New(Config{})
	if c.logger == nil {
		t.Fatal("expected default logger to be set, got nil")
	}
}
