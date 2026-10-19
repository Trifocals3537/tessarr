package config

import (
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

const (
	MinAuthPasswordBytes = 6
	MaxAuthPasswordBytes = 72
	MaxAuthUsernameBytes = 128
)

func VerifyAuth(username, password string) bool {
	if username == "" {
		return false
	}
	auth := Get().GetAuth()
	if auth == nil {
		return false
	}
	if username != auth.Username {
		return false
	}
	err := bcrypt.CompareHashAndPassword([]byte(auth.Password), []byte(password))
	return err == nil
}

// ApplyAuthCredentials validates and updates a private configuration draft
// while retaining the installation's existing session secret and API token.
// Runtime callers should use it inside Update so auth.json and config.json are
// committed together before the new snapshot is published.
func (c *Config) ApplyAuthCredentials(username, password string) error {
	username = strings.TrimSpace(username)
	if err := ValidateAuthCredentials(username, password); err != nil {
		return err
	}

	hashedPassword, err := bcrypt.GenerateFromPassword(
		[]byte(password),
		bcrypt.DefaultCost,
	)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	if c.Auth == nil {
		c.Auth = &Auth{}
	}
	c.Auth.Username = username
	c.Auth.Password = string(hashedPassword)
	c.Auth.SessionVersion++
	if c.Auth.SessionVersion == 0 {
		c.Auth.SessionVersion = 1
	}
	c.UseAuth = true
	return nil
}

func ValidateAuthCredentials(username, password string) error {
	username = strings.TrimSpace(username)
	switch {
	case username == "":
		return fmt.Errorf("username is required")
	case len(username) > MaxAuthUsernameBytes:
		return fmt.Errorf("username must be at most %d bytes", MaxAuthUsernameBytes)
	case len(password) < MinAuthPasswordBytes:
		return fmt.Errorf(
			"password must be at least %d bytes",
			MinAuthPasswordBytes,
		)
	case len(password) > MaxAuthPasswordBytes:
		return fmt.Errorf(
			"password must be at most %d bytes",
			MaxAuthPasswordBytes,
		)
	default:
		return nil
	}
}
