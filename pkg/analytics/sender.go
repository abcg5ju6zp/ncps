package analytics

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/kalbasit/ncps/pkg/analytics/queue"
)

const (
	// sendAttemptTimeout bounds a single OTLP delivery attempt.
	sendAttemptTimeout = 15 * time.Second

	backoffInitial = 1 * time.Second
	backoffMax     = 5 * time.Minute
	backoffFactor  = 2.0
	backoffJitter  = 0.2
)

// senders runs one FIFO delivery loop per kind against a shared durable
// queue.
type senders struct {
	spool          *queue.Queue
	sender         batchSender
	logger         zerolog.Logger
	backoffInitial time.Duration
	backoffMax     time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// senderConfig tunes delivery backoff. Zero values fall back to package
// defaults.
type senderConfig struct {
	backoffInitial time.Duration
	backoffMax     time.Duration
}

func startSendersWith(
	parent context.Context,
	spool *queue.Queue,
	sender batchSender,
	cfg senderConfig,
) *senders {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))

	if cfg.backoffInitial <= 0 {
		cfg.backoffInitial = backoffInitial
	}

	if cfg.backoffMax <= 0 {
		cfg.backoffMax = backoffMax
	}

	s := &senders{
		spool:          spool,
		sender:         sender,
		logger:         *zerolog.Ctx(parent),
		backoffInitial: cfg.backoffInitial,
		backoffMax:     cfg.backoffMax,
		ctx:            ctx,
		cancel:         cancel,
	}

	for _, kind := range queue.AllKinds() {
		s.wg.Add(1)

		go s.run(kind)
	}

	return s
}

// stop signals the loops to exit and waits for them. In-flight batches are
// requeued so the next start retries them.
func (s *senders) stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *senders) run(kind queue.Kind) {
	defer s.wg.Done()

	for {
		if err := s.ctx.Err(); err != nil {
			return
		}

		lease, err := s.spool.Next(s.ctx, kind)
		if err != nil {
			// Queue closed with nothing left, or the loop was stopped.
			return
		}

		if !s.deliver(lease) {
			return
		}
	}
}

// deliver sends one leased batch and applies the ack/fail/quarantine policy.
// It returns false when the loop must stop.
func (s *senders) deliver(lease *queue.Lease) bool {
	ctx, cancel := context.WithTimeout(s.ctx, sendAttemptTimeout)
	defer cancel()

	err := s.sender.Send(ctx, lease.Kind, lease.Payload)

	switch {
	case err == nil:
		if err := lease.Ack(); err != nil {
			s.logger.Error().Err(err).Str("batch", lease.ID).
				Msg("analytics: accepted batch could not be removed from queue, retrying")

			return s.backoff(lease)
		}

		s.logger.Debug().Str("batch", lease.ID).
			Msg("analytics: batch acknowledged by collector")

		return true

	case isUnprocessable(err):
		// Persisted data is structurally invalid; never resend it.
		lease.Quarantine(err)

		return true

	case s.ctx.Err() != nil:
		// Stopping mid-send: requeue and exit without sleeping.
		_ = lease.Fail()

		return false

	default:
		s.logger.Warn().
			Err(err).
			Str("batch", lease.ID).
			Str("kind", string(lease.Kind)).
			Int("attempts", lease.Attempts).
			Msg("analytics: batch delivery failed; keeping batch and backing off")

		return s.backoff(lease)
	}
}

// backoff requeues the lease and waits for a bounded, jittered delay before
// the next attempt. Ordering is preserved because the single per-kind loop
// stays head-of-line blocked on this batch.
func (s *senders) backoff(lease *queue.Lease) bool {
	_ = lease.Fail()

	delay := s.backoffDelay(lease.Attempts)

	timer := time.NewTimer(delay)

	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *senders) backoffDelay(attempts int) time.Duration {
	initial := float64(s.backoffInitial)
	if initial <= 0 {
		initial = float64(backoffInitial)
	}

	maxDelay := s.backoffMax
	if maxDelay <= 0 {
		maxDelay = backoffMax
	}

	if attempts < 1 {
		attempts = 1
	}

	delay := initial
	for range attempts - 1 {
		delay *= backoffFactor
	}

	if delay > float64(maxDelay) {
		delay = float64(maxDelay)
	}

	spread := delay * backoffJitter
	//nolint:gosec // backoff jitter is not security sensitive; math/rand/v2 is sufficient.
	delay += (rand.Float64()*2 - 1) * spread

	if delay < 0 {
		delay = 0
	}

	return time.Duration(delay)
}
