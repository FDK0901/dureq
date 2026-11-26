package config

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// The keys of the monitoring API are read from the file, as they are written in docs/dureqd.md.
func TestGetConfig_ReadsTheKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(`dureqd:
  allowedOrigins:
    - "https://durequi.example.com"
  allowedHosts:
    - "dureq.internal"
    - "localhost:8080"
  maxNameScan: 5000
  requestTimeoutMs: 15000
  runHistoryRetentionHours: 24
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DUREQ_CONFIG_PATH", path)
	cfg, err := GetConfig()
	if err != nil {
		t.Fatal(err)
	}
	d := cfg.DureqdConfig
	if !slices.Equal(d.AllowedOrigins, []string{"https://durequi.example.com"}) ||
		!slices.Equal(d.AllowedHosts, []string{"dureq.internal", "localhost:8080"}) ||
		d.MaxNameScan != 5000 || d.RequestTimeoutMs != 15000 || d.RunHistoryRetentionHours != 24 {
		t.Errorf("read %+v", d)
	}
}
