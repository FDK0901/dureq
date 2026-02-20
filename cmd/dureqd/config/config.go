package config

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

var (
	configLock      = &sync.Mutex{}
	koanfSingleton  = koanf.New(".")
	configSingleton *Config
)

// GetConfig는 설정을 로드하고 반환합니다
func GetConfig() (*Config, error) {
	configLock.Lock()
	defer configLock.Unlock()

	if configSingleton != nil {
		return configSingleton, nil
	}

	configPath := getConfigPath()

	fp := file.Provider(configPath)
	if err := koanfSingleton.Load(fp, yaml.Parser()); err != nil {
		return nil, err
	}

	configSingleton = &Config{}
	if err := koanfSingleton.Unmarshal("", configSingleton); err != nil {
		return nil, err
	}

	// 파일 변경 감지 (hot reload)
	fp.Watch(func(event interface{}, err error) {
		if err != nil {
			return
		}
		configLock.Lock()
		defer configLock.Unlock()

		koanfSingleton = koanf.New(".")
		if err := koanfSingleton.Load(fp, yaml.Parser()); err != nil {
			return
		}
		if err := koanfSingleton.Unmarshal("", configSingleton); err != nil {
			return
		}
	})

	return configSingleton, nil
}

func getConfigPath() string {
	// 환경변수로 설정 경로 지정 가능
	if envPath := os.Getenv("DUREQ_CONFIG_PATH"); envPath != "" {
		return envPath
	}

	// 기본 경로
	basePath := "cmd/dureqd"

	return filepath.Join(basePath, "config.yaml")
}
