package postgres

// import (
// 	"context"
// 	"fmt"
// 	"time"

// 	"db-worker/internal/model"
// 	interfaces "db-worker/internal/service/message/interface"

// 	"github.com/google/uuid"
// 	"github.com/sirupsen/logrus"
// )

// func (db *Repo) SaveNotes(ctx context.Context, id string, notes []interfaces.Message) error {
// 	logrus.Debugf("postgres: saving %d note(s). id: %s", len(notes), id)

// 	ctx, cancel := context.WithTimeout(ctx, time.Duration(db.insertTimeout)*time.Millisecond)
// 	defer cancel()

// 	tx, err := db.getTx(id)
// 	if err != nil {
// 		return fmt.Errorf("create notes: error on get tx: %w", err)
// 	}

// 	query := `
// 		INSERT INTO notes.notes (user_id, text, space_id)
// 		VALUES ((select id from users.users where tg_id = $1), $2, $3)
// 		RETURNING id
// 	`

// 	stmt, err := tx.PrepareContext(ctx, query)
// 	if err != nil {
// 		// rollback вызовется в uow

// 		return fmt.Errorf("create notes: error on prepare statement: %w. ID: %s", err, id)
// 	}
// 	defer stmt.Close()

// 	for i, msg := range notes {
// 		id := uuid.UUID{}

// 		note := msg.Model().(model.CreateNoteRequest)
// 		err = stmt.QueryRowContext(ctx, note.UserID, note.Text, note.SpaceID).Scan(&id)
// 		if err != nil {
// 			return fmt.Errorf("create notes: error on execute statement: %w. ID: %s", err, id)
// 		}

// 		note.ID = id

// 		notes[i] = note
// 	}

// 	logrus.Debugf("postgres: %d note(s) saved. id: %s", len(notes), id)

// 	return nil
// }

// func (db *Repo) UpdateNotes(ctx context.Context, id string, notes []interfaces.Message) error {
// 	logrus.Debugf("postgres: updating %d note(s). id: %s", len(notes), id)

// 	ctx, cancel := context.WithTimeout(ctx, time.Duration(db.insertTimeout)*time.Millisecond)
// 	defer cancel()

// 	tx, err := db.getTx(id)
// 	if err != nil {
// 		return fmt.Errorf("update notes:error on get tx: %w", err)
// 	}

// 	query := `
// 		UPDATE notes.notes
// 		SET text = $1,
// 		updated = extract(epoch from current_timestamp)::BIGINT
// 		WHERE id = $2
// 	`

// 	stmt, err := tx.PrepareContext(ctx, query)
// 	if err != nil {
// 		// rollback вызовется в uow

// 		return fmt.Errorf("update notes: error on prepare statement: %w. ID: %s", err, id)
// 	}
// 	defer stmt.Close()

// 	for _, msg := range notes {
// 		note := msg.Model().(model.UpdateNoteRequest)
// 		_, err = stmt.ExecContext(ctx, note.Text, note.ID)
// 		if err != nil {
// 			return fmt.Errorf("update notes: error on execute statement: %w. ID: %s", err, id)
// 		}
// 	}

// 	logrus.Debugf("postgres: %d note(s) updated. id: %s", len(notes), id)

// 	return nil
// }
