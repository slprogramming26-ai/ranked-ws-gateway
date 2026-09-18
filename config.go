package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// Config sind alle Werte, die von außen kommen. Sie werden EINMAL beim Start
// gelesen und geprüft; danach wird die Struktur nur noch gelesen.
type Config struct {
	Port             string // Port, auf dem der Gateway lauscht
	BackendURL       string // Basis-URL des Backends (Strecke B), ohne Slash am Ende
	WSInternalSecret string // Wert für den Header X-WS-Secret
	SecretKey        []byte // JWT-Signaturschlüssel, identisch mit dem Backend
	Algorithm        string // JWT-Verfahren; unterstützt wird nur HS256
	RedisURL         string // dasselbe Redis wie das Backend (Strecke C + Presence)
	InstanceID       string // Kennung dieser Instanz, landet in ws:online:{user_id}
}

// envOr liefert den Wert der Variablen oder den Vorgabewert, wenn sie fehlt
// oder leer ist.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// mask verrät nur, OB ein Geheimnis gesetzt ist – nie seinen Wert.
func mask(s string) string {
	if s == "" {
		return "(leer)"
	}
	return "(gesetzt)"
}

// loadDotEnv liest eine .env im einfachsten sinnvollen Format: KEY=VALUE,
// '#' leitet einen Kommentar ein, leere Zeilen werden übersprungen.
//
// Zwei Entscheidungen darin sind wichtig:
//   - Fehlt die Datei, ist das KEIN Fehler. Auf Railway gibt es keine .env,
//     dort kommt alles aus der echten Umgebung.
//   - Bereits gesetzte Umgebungsvariablen gewinnen. Eine vergessene lokale
//     Datei darf niemals die Produktionswerte überschreiben.
func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if _, alreadySet := os.LookupEnv(key); alreadySet {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// generateInstanceID baut eine Kennung aus Hostname und Zufallssuffix.
// Der Hostname macht Logs lesbar, das Suffix trennt zwei Prozesse auf
// derselben Maschine.
func generateInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}

	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return host
	}

	return host + "-" + hex.EncodeToString(buf)
}

// LoadConfig liest die Umgebung EINMAL beim Start und prüft sie sofort.
// Fehlt etwas Wichtiges, startet der Prozess gar nicht erst: lieber ein
// toter Container als einer, der Nachrichten still verschluckt.
func LoadConfig() (*Config, error) {
	if err := loadDotEnv(".env"); err != nil {
		return nil, err
	}

	cfg := &Config{
		Port:             envOr("PORT", "8080"),
		BackendURL:       strings.TrimRight(envOr("BACKEND_URL", "http://127.0.0.1:8000"), "/"),
		WSInternalSecret: os.Getenv("WS_INTERNAL_SECRET"),
		SecretKey:        []byte(os.Getenv("SECRET_KEY")),
		Algorithm:        envOr("ALGORITHM", "HS256"),
		RedisURL:         os.Getenv("REDIS_URL"),
		InstanceID:       envOr("INSTANCE_ID", generateInstanceID()),
	}

	var problems []string
	if cfg.WSInternalSecret == "" {
		problems = append(problems, "WS_INTERNAL_SECRET fehlt")
	}
	if len(cfg.SecretKey) == 0 {
		problems = append(problems, "SECRET_KEY fehlt")
	}
	if cfg.RedisURL == "" {
		problems = append(problems, "REDIS_URL fehlt")
	}
	// Nur HS256. Ein anderes Verfahren braucht eine andere Prüflogik; still
	// weiterlaufen hieße, Tokens falsch zu validieren.
	if cfg.Algorithm != "HS256" {
		problems = append(problems, fmt.Sprintf("ALGORITHM %q wird nicht unterstützt, nur HS256", cfg.Algorithm))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("unbrauchbare Konfiguration: %s", strings.Join(problems, "; "))
	}

	return cfg, nil
}
