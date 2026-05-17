package nats

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

type PersistentQueue struct {
	db     *sql.DB
	worker func(ctx context.Context, stream, subject string, payload []byte) error
}

type EventRow struct {
	ID        int64
	CreatedAt time.Time
	Stream    string
	Subject   string
	Payload   []byte
}

func NewPersistentQueue(dbPath string, worker func(ctx context.Context, stream, subject string, payload []byte) error) (*PersistentQueue, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Create events table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			stream TEXT NOT NULL,
			subject TEXT NOT NULL,
			payload BLOB NOT NULL
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create events table: %w", err)
	}

	// Create index on created_at for FIFO ordering
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at)`)
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	pq := &PersistentQueue{
		db:     db,
		worker: worker,
	}

	// Start the worker goroutine
	go pq.runWorker()

	return pq, nil
}

func (pq *PersistentQueue) Close() error {
	return pq.db.Close()
}

func (pq *PersistentQueue) Add(stream, subject string, payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := pq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "INSERT INTO events (stream, subject, payload) VALUES (?, ?, ?)", stream, subject, payload)
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logrus.Debugf("pqueue: added event to queue (stream=%s, subject=%s)", stream, subject)
	return nil
}

func (pq *PersistentQueue) runWorker() {
	for {
		// Get the oldest event
		event, err := pq.getNextEvent()
		if err != nil {
			logrus.Errorf("pqueue: failed to get next event: %s", err)
			time.Sleep(10 * time.Second)
			continue
		}

		if event == nil {
			// No events in queue, wait and retry
			time.Sleep(1 * time.Second)
			continue
		}

		// Process the event
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = pq.worker(ctx, event.Stream, event.Subject, event.Payload)
		cancel()

		if err != nil {
			logrus.Errorf("pqueue: worker failed for event %d (stream=%s, subject=%s): %s", event.ID, event.Stream, event.Subject, err)
			// Wait 10 seconds before retrying
			time.Sleep(10 * time.Second)
			continue
		}

		// Remove the event from the queue
		err = pq.removeEvent(event.ID)
		if err != nil {
			logrus.Errorf("pqueue: failed to remove event %d: %s", event.ID, err)
			// Even if removal fails, continue to next event
			continue
		}

		logrus.Debugf("pqueue: successfully processed and removed event %d", event.ID)
	}
}

func (pq *PersistentQueue) getNextEvent() (*EventRow, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var event EventRow
	row := pq.db.QueryRowContext(ctx, "SELECT id, created_at, stream, subject, payload FROM events ORDER BY created_at ASC LIMIT 1")

	err := row.Scan(&event.ID, &event.CreatedAt, &event.Stream, &event.Subject, &event.Payload)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan event: %w", err)
	}

	return &event, nil
}

func (pq *PersistentQueue) removeEvent(id int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pq.db.ExecContext(ctx, "DELETE FROM events WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete event: %w", err)
	}

	return nil
}

func (pq *PersistentQueue) Size() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := pq.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count events: %w", err)
	}

	return count, nil
}
