package internal

// DropWatcherGroups closes every registered group as if its consumer lagged.
func (s *Backend) DropWatcherGroups() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for w := range s.WatcherGroups {
		s.dropLaggedLocked(w)
	}
}

func (s *Backend) WatcherGroupCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.WatcherGroups)
}

// WatcherCount counts the watchers of all registered groups.
func (s *Backend) WatcherCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for w := range s.WatcherGroups {
		w.mu.Lock()
		n += len(w.watchers)
		w.mu.Unlock()
	}
	return n
}
