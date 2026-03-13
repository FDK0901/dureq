package config

type Config struct {
	DureqdConfig DureqdConfig `koanf:"dureqd"`
	RedisConfig  RedisConfig  `koanf:"redis"`
}

type DureqdConfig struct {
	NodeID      string   `koanf:"nodeId"`
	ApiAddress  string   `koanf:"apiAddress"`
	GrpcAddress string   `koanf:"grpcAddress"`
	Concurrency int      `koanf:"concurrency"`
	Prefix      string   `koanf:"prefix"`
	Mode        string   `koanf:"mode"` // "full", "queue", "scheduler", "workflow", "monitor" or comma-separated combo
	Handlers    []string `koanf:"handlers"` // enabled handler task types; empty = all
}

type RedisConfig struct {
	URL          string   `koanf:"url"`
	ClusterAddrs []string `koanf:"clusterAddrs"`
	Username     string   `koanf:"username"`
	Password     string   `koanf:"password"`
	DB           int      `koanf:"db"`
	PoolSize     int      `koanf:"poolSize"`
}
