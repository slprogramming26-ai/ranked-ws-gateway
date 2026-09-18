package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// Die Zahl steht in PROTOCOL.md § 5.2 — dort ist die Quelle, hier die Kopie.
const maxMessageRunes = 4096

// ClientMessage ist das, was der Flutter-Client schickt (PROTOCOL.md § 4.2).
// KeyVersion ist ein Zeiger, weil "Feld fehlt" von "Feld ist 0" unterschieden
// werden muss: bei group ist es Pflicht, bei dm verboten.
type ClientMessage struct {
	Kind        string `json:"kind"`
	To          int    `json:"to"`
	Message     string `json:"message"`
	KeyVersion  *int   `json:"key_version"`
	ClientMsgID string `json:"client_msg_id"`
}

// parseClientMessage prüft nur die FORM (PROTOCOL.md § 5.2). Kein Fachwissen:
// ob der Empfänger existiert, blockiert hat oder Mitglied ist, klärt Python.
func parseClientMessage(data []byte) (*ClientMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var msg ClientMessage
	if err := dec.Decode(&msg); err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}
	if dec.More() {
		return nil, errors.New("expected exactly one json object")
	}

	if msg.Kind != "dm" && msg.Kind != "group" {
		return nil, errors.New("missing or unknown 'kind' (expected 'dm' or 'group')")
	}
	if msg.To == 0 {
		return nil, errors.New("field 'to' is required")
	}
	if n := utf8.RuneCountInString(msg.Message); n < 1 || n > maxMessageRunes {
		return nil, fmt.Errorf("field 'message' must be 1..%d characters", maxMessageRunes)
	}
	if msg.Kind == "group" && msg.KeyVersion == nil {
		return nil, errors.New("field 'key_version' is required for kind 'group'")
	}
	if msg.Kind == "dm" && msg.KeyVersion != nil {
		return nil, errors.New("field 'key_version' is not allowed for kind 'dm'")
	}

	return &msg, nil
}

// ErrorMessage ist die Antwort aus PROTOCOL.md § 4.3, wenn Go selbst ablehnt.
type ErrorMessage struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

func newErrorMessage(detail string) ErrorMessage {
	return ErrorMessage{Kind: "error", Detail: detail}
}
