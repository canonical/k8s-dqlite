package internal

import (
	"context"
	"math/rand"
	"time"

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

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		startRevision, err := s.Driver.CurrentRevision(ctx)
		if err != nil {
			logrus.Errorf("failed to read old events for ttl: %v", err)
			return
		}

		rows, err := s.Driver.ListTTL(ctx, startRevision)
		if err != nil {
			logrus.Errorf("failed to read old events for ttl: %v", err)
			return
		}

		var (
			key             []byte
			revision, lease int64
		)
		for rows.Next() {
			if err := rows.Scan(&revision, &key, &lease); err != nil {
				logrus.Errorf("failed to read old events for ttl: %v", err)
				return
			}
			go run(ctx, key, revision, time.Duration(lease)*time.Second)
		}

		// watchFromRevision tracks where to re-subscribe from after a watch group
		// is unexpectedly closed. Initialized to startRevision and advanced as
		// updates are received so restarts replay only unseen events.
		watchFromRevision := startRevision
		ttlBackoff := s.Config.PollInterval

		for ctx.Err() == nil {
			group, err := s.WatcherGroup(ctx)
			if err != nil {
				logrus.Errorf("failed to create watch group for ttl: %v", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(ttlBackoff):
				}
				if ttlBackoff < 30*time.Second {
					ttlBackoff *= 2
				}
				continue
			}

			if err := group.Watch(1, []byte{0}, []byte{255}, watchFromRevision); err != nil {
				logrus.WithError(err).Warning("ttl watch failed")
				select {
				case <-ctx.Done():
					return
				case <-time.After(ttlBackoff):
				}
				if ttlBackoff < 30*time.Second {
					ttlBackoff *= 2
				}
				continue
			}

			// Reset backoff after a successful subscription.
			ttlBackoff = s.Config.PollInterval

			for update := range group.Updates() {
				watchFromRevision = update.Revision()
				for _, watcher := range update.Watchers() {
					for _, event := range watcher.Events {
						if event.Kv.Lease > 0 {
							// add some jitter to avoid ttl expiry and deletion on all nodes at the same time with the goal of
							// one of them succeeding if the DB is under high load (busy)
							jitterDelay := time.Duration(rand.Intn(1000)) * time.Millisecond // Jitter up to 1 second as api-server does not expect sub-second precision
							go run(ctx, []byte(event.Kv.Key), event.Kv.ModRevision, time.Duration(event.Kv.Lease)*time.Second+jitterDelay)
						}
					}
				}
			}

			if ctx.Err() != nil {
				return
			}

			// group.Updates() closed unexpectedly; log and re-subscribe from last revision.
			if groupErr := group.Err(); groupErr != nil {
				logrus.WithError(groupErr).Warnf("ttl watcher group closed unexpectedly, restarting from revision %d", watchFromRevision)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(ttlBackoff):
			}
			if ttlBackoff < 30*time.Second {
				ttlBackoff *= 2
			}
		}
	}()
}
