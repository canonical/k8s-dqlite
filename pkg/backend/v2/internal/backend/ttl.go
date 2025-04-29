package internal

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

func (s Backend) ttl(ctx context.Context) {
	run := func(ctx context.Context, key []byte, revision int64, timeout time.Duration) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(timeout):
			s.Delete(ctx, key, revision)
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

		// TODO needs to be restarted, never cancelled/dropped
		group, err := s.WatcherGroup(ctx)
		if err != nil {
			logrus.Errorf("failed to create watch group for ttl: %v", err)
			return
		}
		group.Watch(1, []byte{0}, []byte{255}, startRevision)

		for group := range group.Updates() {
			for _, watcher := range group.Watchers() {
				for _, event := range watcher.Events {
					if event.Kv.Lease > 0 {
						go run(ctx, []byte(event.Kv.Key), event.Kv.ModRevision, time.Duration(event.Kv.Lease)*time.Second)
					}
				}
			}
		}
	}()
}
