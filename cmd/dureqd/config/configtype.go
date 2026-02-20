package config

type Config struct {
	DureqdConfig DureqdConfig `koanf:"dureqd"`
	RedisConfig  RedisConfig  `koanf:"redis"`
}

type DureqdConfig struct {
	NodeID      string `koanf:"nodeId"`
	ApiAddress  string `koanf:"apiAddress"`
	GrpcAddress string `koanf:"grpcAddress"`
	Concurrency int    `koanf:"concurrency"`
	Prefix      string `koanf:"prefix"`
}

type RedisConfig struct {
	URL      string `koanf:"url"`
	Password string `koanf:"password"`
	DB       int    `koanf:"db"`
	PoolSize int    `koanf:"poolSize"`
}
