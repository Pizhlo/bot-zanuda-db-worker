package redis

import (
	"context"
	"db-worker/internal/config"
	"db-worker/internal/storage/redis"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
)

// Service - сервис для работы с Redis.
type Service struct {
	cfg    *config.Redis
	client redisClient

	mu sync.Mutex
}

// redisClient - интерфейс для работы с Redis.
// Его реализуют клиент и кластерный клиент.
type redisClient interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
}

// Option определяет опции для Service.
type Option func(*Service)

// WithCfg сохраняет конфигурацию Redis.
func WithCfg(cfg *config.Redis) Option {
	return func(s *Service) {
		s.cfg = cfg
	}
}

// New создает новый экземпляр Service для работы с Redis.
func New(opts ...Option) (*Service, error) {
	s := &Service{}

	for _, opt := range opts {
		opt(s)
	}

	if s.cfg == nil {
		return nil, fmt.Errorf("cfg is required")
	}

	return s, nil
}

// Connect соединяется с Redis в зависимости от типа конфигурации: single - один узел, cluster - кластер.
func (s *Service) Connect(ctx context.Context) error {
	s.mu.Lock()

	if s.client != nil {
		return nil
	}

	s.mu.Unlock()

	var err error

	switch s.cfg.Type {
	case config.RedisTypeSingle:
		s.mu.Lock()
		s.client, err = redis.NewSingleClient(s.cfg)
		s.mu.Unlock()

		if err != nil {
			return fmt.Errorf("error creating redis client (single): %w", err)
		}
	case config.RedisTypeCluster:
		s.mu.Lock()
		s.client, err = redis.NewClusterClient(s.cfg)
		s.mu.Unlock()

		if err != nil {
			return fmt.Errorf("error creating redis client (cluster): %w", err)
		}
	default:
		return fmt.Errorf("unknown redis type: %s", s.cfg.Type)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.client.Connect(ctx); err != nil {
		return fmt.Errorf("error connecting to redis: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"type":  s.cfg.Type,
		"host":  s.cfg.Host,
		"port":  s.cfg.Port,
		"addrs": s.cfg.Addrs,
	}).Info("successfully connected redis")

	return nil
}

// Stop закрывает соединение с Redis.
func (s *Service) Stop(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"type":  s.cfg.Type,
		"host":  s.cfg.Host,
		"port":  s.cfg.Port,
		"addrs": s.cfg.Addrs,
	}).Info("stopping redis")

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client == nil { // нет соединения, значит не нужно закрывать
		return nil
	}

	return s.client.Close(ctx)
}
