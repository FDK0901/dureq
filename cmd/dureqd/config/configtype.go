package config

type Config struct {
	DureqdConfig DureqdConfig `koanf:"dureqd"`
	RedisConfig  RedisConfig  `koanf:"redis"`
}

type DureqdConfig struct {
	NodeID         string   `koanf:"nodeId"`
	ApiAddress     string   `koanf:"apiAddress"`
	AllowedOrigins []string `koanf:"allowedOrigins"`
	AllowedHosts   []string `koanf:"allowedHosts"` // hosts that the HTTP API answers; empty = all
	GrpcAddress    string   `koanf:"grpcAddress"`
	Concurrency    int      `koanf:"concurrency"`
	Prefix         string   `koanf:"prefix"`
	Mode           string   `koanf:"mode"`           // "full", "queue", "scheduler", "workflow", "monitor" or comma-separated combo
	Handlers       []string `koanf:"handlers"`        // enabled handler task types; empty = all
	DrainTimeoutMs int      `koanf:"drainTimeoutMs"`  // max time (ms) to wait for in-flight jobs during shutdown; 0 = 30s default

	// RunHistoryRetentionHours is how long (h) finished runs are kept in the run history; 0 = until removed by hand
	RunHistoryRetentionHours int `koanf:"runHistoryRetentionHours"`

	// MaxNameScan is how many workflows or batches a list filtered by name looks at; 0 = 100000
	MaxNameScan int `koanf:"maxNameScan"`

	// RequestTimeoutMs is how long (ms) a request of the HTTP API may take, but a WebSocket; one that takes
	// longer is answered 503. 0 = 30000, negative = no limit
	RequestTimeoutMs int `koanf:"requestTimeoutMs"`
}

type RedisConfig struct {
	URL          string   `koanf:"url"`
	ClusterAddrs []string `koanf:"clusterAddrs"`
	Username     string   `koanf:"username"`
	Password     string   `koanf:"password"`
	DB           int      `koanf:"db"`
	PoolSize     int      `koanf:"poolSize"`
}
