package internal

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/limited"
	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
)

func (s *Backend) ttl(ctx context.Context) {
	run := func(ctx context.Context, key []byte, revision int64, timeout time.Duration) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(timeout):
			deleteLease := func() error {
				// retry until we succeed or context is cancelled, if another node has already deleted the key the delete will not return an error and we will exit the retry loop
				rev, deleted, err := s.Delete(ctx, key, revision)
				if err != nil {
					logrus.Errorf("failed to delete %s for TTL: %v, retrying", key, err)
					return err
				}
				if deleted {
					logrus.Debugf("deleted %s for TTL, revRet=%d", key, rev)
				}
				return nil
			}

			b := backoff.NewExponentialBackOff()
			b.InitialInterval = 100 * time.Millisecond
			b.RandomizationFactor = 0.5
			b.Multiplier = 1.5
			b.MaxInterval = 5 * time.Second
			b.MaxElapsedTime = 0 // retry indefinitely
			b.Reset()

			err := backoff.Retry(deleteLease, backoff.WithContext(b, ctx))
			if err != nil {
				logrus.Errorf("failed to delete %s for TTL after retries: %v", key, err)
			}
		}
	}

	schedule := func(key []byte, revision, lease int64, jitter bool) {
		timeout := time.Duration(lease) * time.Second
		if jitter {
			// add some jitter to avoid ttl expiry and deletion on all nodes at the same time with the goal of
			// one of them succeeding if the DB is under high load (busy)
			timeout += time.Duration(rand.Intn(1000)) * time.Millisecond // Jitter up to 1 second as api-server does not expect sub-second precision
		}
		go run(ctx, key, revision, timeout)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ttlBackoff := s.Config.PollInterval
		wait := func() bool {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(ttlBackoff):
			}
			if ttlBackoff < 30*time.Second {
				ttlBackoff *= 2
			}
			return true
		}

		// watchFromRevision is the next revision to watch from; Watch's
		// startRevision is inclusive. 0 means existing leases must be listed first.
		var watchFromRevision int64
		for ctx.Err() == nil {
			if watchFromRevision == 0 {
				listRevision, err := s.scheduleExistingTTL(ctx, schedule)
				if err != nil {
					logrus.Errorf("failed to read old events for ttl: %v", err)
					if !wait() {
						return
					}
					continue
				}
				watchFromRevision = listRevision + 1
			}

			next, err := s.watchTTL(ctx, watchFromRevision, schedule)
			if ctx.Err() != nil {
				return
			}
			var compacted *limited.CompactedError
			if errors.As(err, &compacted) {
				logrus.Warnf("ttl watch revision %d is compacted, listing leases again", watchFromRevision)
				watchFromRevision = 0
				continue
			}
			logrus.WithError(err).Warnf("ttl watch ended, restarting from revision %d", next)
			if next > watchFromRevision {
				ttlBackoff = s.Config.PollInterval
			}
			watchFromRevision = next
			if !wait() {
				return
			}
		}
	}()
}

// scheduleExistingTTL schedules expiry of all leased keys and returns the
// revision they were listed at.
func (s *Backend) scheduleExistingTTL(ctx context.Context, schedule func(key []byte, revision, lease int64, jitter bool)) (int64, error) {
	listRevision, err := s.Driver.CurrentRevision(ctx)
	if err != nil {
		return 0, err
	}
	rows, err := s.Driver.ListTTL(ctx, listRevision)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var (
		key             []byte
		revision, lease int64
	)
	for rows.Next() {
		if err := rows.Scan(&revision, &key, &lease); err != nil {
			return 0, err
		}
		schedule(key, revision, lease, false)
	}
	return listRevision, rows.Err()
}

// watchTTL schedules expiry of leased keys written from revision on until the
// watch ends, and returns the revision to resume from. Its watcher group is
// removed on return, also when Watch fails.
func (s *Backend) watchTTL(ctx context.Context, revision int64, schedule func(key []byte, revision, lease int64, jitter bool)) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, err := s.WatcherGroup(ctx)
	if err != nil {
		return revision, err
	}
	if err := group.Watch(1, []byte{0}, []byte{255}, revision); err != nil {
		return revision, err
	}
	for update := range group.Updates() {
		revision = update.Revision() + 1
		for _, watcher := range update.Watchers() {
			for _, event := range watcher.Events {
				if event.Kv.Lease > 0 {
					schedule([]byte(event.Kv.Key), event.Kv.ModRevision, event.Kv.Lease, true)
				}
			}
		}
	}
	if err := group.Err(); err != nil {
		return revision, err
	}
	return revision, errors.New("watcher group closed")
}
