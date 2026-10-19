package qbit

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"time"
)

const (
	qbitSessionTTL = time.Hour
	qbitSessionMax = 1024
)

type qbitSession struct {
	username  string
	password  string
	createdAt time.Time
	expiresAt time.Time
}

type qbitSessionStore struct {
	mu       sync.Mutex
	sessions map[string]qbitSession
	now      func() time.Time
}

func newQbitSessionStore() *qbitSessionStore {
	return &qbitSessionStore{
		sessions: make(map[string]qbitSession),
		now:      time.Now,
	}
}

func (s *qbitSessionStore) create(username, password string) (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate qBittorrent session: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw[:])

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.removeExpiredLocked(now)
	if len(s.sessions) >= qbitSessionMax {
		s.removeOldestLocked()
	}
	s.sessions[token] = qbitSession{
		username:  username,
		password:  password,
		createdAt: now,
		expiresAt: now.Add(qbitSessionTTL),
	}
	return token, nil
}

func (s *qbitSessionStore) credentials(token string) (string, string, bool) {
	if s == nil || token == "" {
		return "", "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	session, ok := s.sessions[token]
	if !ok {
		return "", "", false
	}
	if !now.Before(session.expiresAt) {
		delete(s.sessions, token)
		return "", "", false
	}
	return session.username, session.password, true
}

func (s *qbitSessionStore) removeExpiredLocked(now time.Time) {
	for token, session := range s.sessions {
		if !now.Before(session.expiresAt) {
			delete(s.sessions, token)
		}
	}
}

func (s *qbitSessionStore) removeOldestLocked() {
	var oldestToken string
	var oldest time.Time
	for token, session := range s.sessions {
		if oldestToken == "" || session.createdAt.Before(oldest) {
			oldestToken = token
			oldest = session.createdAt
		}
	}
	if oldestToken != "" {
		delete(s.sessions, oldestToken)
	}
}
