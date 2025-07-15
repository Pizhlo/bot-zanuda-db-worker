package config

import (
	"fmt"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// ModelConfig представляет конфигурацию моделей
type ModelConfig struct {
	Models map[string]Model `yaml:"models" validate:"required"`
}

// Model представляет модель данных
type Model struct {
	Operations map[string]Operation `yaml:"operations" validate:"required"` // храним операции по имени
}

// LoadModelConfig загружает конфигурацию моделей из файла
func LoadModelConfig(path string) (*ModelConfig, error) {
	logrus.Infof("loading model config from: %s", path)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading model config: %w", err)
	}

	var config ModelConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error unmarshaling model config: %w", err)
	}

	// Бизнес-валидация операций
	for modelName, model := range config.Models {
		for opName, op := range model.Operations {
			if err := op.Validate(); err != nil {
				return nil, fmt.Errorf("model %s operation %s: %w", modelName, opName, err)
			}
		}
	}

	validate := validator.New()

	if err := validate.Struct(config); err != nil {
		return nil, fmt.Errorf("error validating model config: %w", err)
	}

	logrus.Infof("loaded %d model(s)", len(config.Models))

	return &config, nil
}
