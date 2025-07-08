package transaction

import (
	"context"
	interfaces "db-worker/internal/service/message/interface"
)

func (s *Repo) CreateRequest(ctx context.Context, msg interfaces.Message) error {
	return nil
}
