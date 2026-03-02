package scheduler

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
)

func TestSchedulerAddAndRun(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := NewStandard(logger)

	var called atomic.Int32
	rule := config.Rule{
		Name:     "test-rule",
		Schedule: "* * * * *", // every minute
	}

	err := s.AddRule(rule, func(ctx context.Context, r config.Rule) error {
		called.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("AddRule() error: %v", err)
	}

	// Just verify it was added without error.
	// We don't wait for actual execution since it would take a minute.
}

func TestSchedulerInvalidSchedule(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := NewStandard(logger)

	rule := config.Rule{
		Name:     "bad-rule",
		Schedule: "not-a-cron-expression",
	}

	err := s.AddRule(rule, func(ctx context.Context, r config.Rule) error {
		return nil
	})
	if err == nil {
		t.Error("AddRule() should return error for invalid schedule")
	}
}

func TestSchedulerEmptySchedule(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := NewStandard(logger)

	rule := config.Rule{
		Name: "no-schedule",
	}

	err := s.AddRule(rule, func(ctx context.Context, r config.Rule) error {
		return nil
	})
	if err == nil {
		t.Error("AddRule() should return error for empty schedule")
	}
}

func TestSchedulerStartStop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := NewStandard(logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	// Give the scheduler a moment to start.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler did not stop within timeout")
	}
}
