package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/robfig/cron/v3"
)

// RunFunc is called when a rule is due to run.
type RunFunc func(ctx context.Context, rule config.Rule) error

// Scheduler manages cron-based scheduling of archive rules.
type Scheduler struct {
	cron   *cron.Cron
	log    *log.Logger
	mu     sync.Mutex
	cancel context.CancelFunc
}

// New creates a new Scheduler.
func New(logger *log.Logger) *Scheduler {
	if logger == nil {
		logger = log.Default()
	}
	return &Scheduler{
		cron: cron.New(cron.WithSeconds()),
		log:  logger,
	}
}

// NewStandard creates a scheduler with standard (5-field) cron expressions.
func NewStandard(logger *log.Logger) *Scheduler {
	if logger == nil {
		logger = log.Default()
	}
	return &Scheduler{
		cron: cron.New(),
		log:  logger,
	}
}

// AddRule adds a rule to the scheduler with its cron schedule.
func (s *Scheduler) AddRule(rule config.Rule, fn RunFunc) error {
	if rule.Schedule == "" {
		return fmt.Errorf("rule %q has no schedule", rule.Name)
	}

	ruleCopy := rule
	_, err := s.cron.AddFunc(ruleCopy.Schedule, func() {
		s.mu.Lock()
		cancel := s.cancel
		s.mu.Unlock()

		ctx := context.Background()
		if cancel != nil {
			var cancelCtx context.CancelFunc
			ctx, cancelCtx = context.WithCancel(ctx)
			defer cancelCtx()
			// Store the cancel function so shutdown can cancel running jobs.
			s.mu.Lock()
			s.cancel = cancelCtx
			s.mu.Unlock()
		}

		s.log.Printf("scheduler: running rule %q", ruleCopy.Name)
		if err := fn(ctx, ruleCopy); err != nil {
			s.log.Printf("scheduler: rule %q failed: %v", ruleCopy.Name, err)
		} else {
			s.log.Printf("scheduler: rule %q completed", ruleCopy.Name)
		}
	})
	if err != nil {
		return fmt.Errorf("adding schedule for rule %q (%s): %w", rule.Name, rule.Schedule, err)
	}

	s.log.Printf("scheduler: registered rule %q with schedule %q", rule.Name, rule.Schedule)
	return nil
}

// Start begins the scheduler. It blocks until the context is cancelled.
func (s *Scheduler) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()

	s.cron.Start()
	s.log.Printf("scheduler: started")

	<-ctx.Done()
	s.log.Printf("scheduler: shutting down...")

	stopCtx := s.cron.Stop()
	select {
	case <-stopCtx.Done():
		s.log.Printf("scheduler: all jobs finished")
	case <-time.After(30 * time.Second):
		s.log.Printf("scheduler: shutdown timed out after 30s")
	}

	return nil
}

// Stop stops the scheduler.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
}
