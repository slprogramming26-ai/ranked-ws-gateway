package main

import (
	"errors"
	"fmt"
	"strconv"
	"github.com/golang-jwt/jwt/v5"
)

// userIDFromToken prüft den JWT rein kryptografisch, ohne Datenbank.
// Gültige Signatur und nicht abgelaufen ist Beweis genug (PROTOCOL.md § 4.1).
func userIDFromToken(tokenString string, secret []byte) (int, error) {
	claims := jwt.MapClaims{}

	_, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		func(t *jwt.Token) (any, error) { return secret, nil },
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return 0, err
	}

	raw, ok := claims["user_id"].(string)
	if !ok {
		return 0, errors.New("user_id fehlt oder ist kein String")
	}

	id, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("user_id %q ist keine Zahl: %w", raw, err)
	}
	if id <= 0 {
		return 0, fmt.Errorf("user_id %d ist unbrauchbar", id)
	}

	return id, nil
}