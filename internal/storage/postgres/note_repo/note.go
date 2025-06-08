package postgres

import (
	"context"
	"fmt"
	"time"

	"db-worker/internal/model"
	interfaces "db-worker/internal/service/message/interface"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

func (db *Repo) SaveNotes(ctx context.Context, id string, notes []interfaces.Message) error {
	logrus.Debugf("postgres: saving notes. id: %s", id)

	ctx, cancel := context.WithTimeout(ctx, time.Duration(db.insertTimeout)*time.Millisecond)
	defer cancel()

	tx, err := db.getTx(id)
	if err != nil {
		return fmt.Errorf("error on get tx: %w", err)
	}

	query := `
		INSERT INTO notes.notes (user_id, text, space_id)
		VALUES ((select id from users.users where tg_id = $1), $2, $3)
		RETURNING id
	`

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		// rollback вызовется в uow

		return fmt.Errorf("error on prepare statement: %w. ID: %s", err, id)
	}
	defer stmt.Close()

	for i, msg := range notes {
		id := uuid.UUID{}

		note := msg.Model().(model.CreateNoteRequest)
		err = stmt.QueryRowContext(ctx, note.UserID, note.Text, note.SpaceID).Scan(&id)
		if err != nil {
			return fmt.Errorf("error on execute statement: %w. ID: %s", err, id)
		}

		note.ID = id

		notes[i] = note
	}

	// if err = tx.Commit(); err != nil {
	// 	return fmt.Errorf("error on commit tx: %w", err)
	// }

	logrus.Debugf("postgres: notes saved. id: %s", id)

	return nil
}
