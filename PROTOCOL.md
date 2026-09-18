# Chat-Protokoll: Flutter ⇄ ws-gateway (Go) ⇄ Backend (Python)

**Stand:** 2026-09-04 · **`protocol_version`: 1**

Diese Datei liegt **wortgleich in beiden Repos** (`ranked_backend_api` und `ws-gateway`).
Ändert sich hier etwas, ändert es sich in beiden — sonst driften die Repos auseinander,
und genau davor soll dieses Dokument schützen.

---

## 1. Die Grundregel

**Go ist ein dummes Rohr.** Es kennt weder Blocks noch Rekey noch Gruppenmitgliedschaften.
Die gesamte Fachlogik bleibt in Python. Go weiß nur:

- welcher Socket zu welcher `user_id` gehört,
- wie man JSON weiterreicht, ohne es umzubauen.

Der Grund ist nicht Bequemlichkeit, sondern Wartbarkeit: sobald Go anfängt, Chat-Regeln zu
kennen, leben dieselben Regeln in zwei Sprachen und müssen zweimal gepflegt werden.

---

## 2. Die drei Strecken

```
                 A: WebSocket                B: HTTP (intern, Secret)
   Flutter  ────────────────────►  Go  ────────────────────────────►  Python
   Client   ◄────────────────────      ◄────────────────────────────
                 A: WebSocket                   Antwort 1:1 zurück
                                                                        │
                       C: Redis Pub/Sub  "ws:push"                      │
                Go  ◄───────────────────────────────────────────────────┘
                                (nur abwärts)
```

- **A** ist das heutige Protokoll — **byte-gleich zu vorher.** Flutter bekommt eine neue
  URL und sonst nichts. Das ist die wichtigste Eigenschaft dieses Entwurfs.
- **B** ist Request/Response: Go wartet auf Pythons Antwort und reicht sie **unverändert**
  an genau den einen Client zurück, der gesendet hat.
- **C** ist Broadcast an *alle* Go-Instanzen: so erreichen Nachrichten die *Empfänger*.

**Warum aufwärts HTTP und nicht auch Redis?** Weil `ack` / `error` / `rekey_required` /
`key_outdated` Antworten an genau einen Client sind. Über HTTP ist das eine Zeile Code;
über eine Queue müsste man Antworten korrelieren und zurückrouten.

---

## 3. `protocol_version`

Jeder interne Request (B) und jeder Umschlag (C) trägt `protocol_version`.
Kennt die Gegenseite die Zahl nicht, **verwirft sie die Nachricht, statt sie zu raten.**

- Python weist unbekannte Versionen auf B mit **422** ab.
- Go verwirft Umschläge mit unbekannter Version auf C still (mit Log-Zeile).

Erhöht wird die Zahl nur bei **inkompatiblen** Änderungen — also wenn ein Feld verschwindet,
seinen Typ oder seine Bedeutung ändert. Ein *neues, optionales* Feld ist kompatibel und
braucht keine neue Version.

---

## 4. Strecke A — Client ⇄ Go (WebSocket)

### 4.1 Verbindungsaufbau

```
GET /ws/chat?token=<JWT>
```

Go prüft den JWT **nur kryptografisch**: Signatur mit `SECRET_KEY` (Verfahren aus `ALGORITHM`
in `.env` — beide Dienste teilen sich denselben Wert) und `exp`. Die `user_id` kommt aus dem
gleichnamigen Claim.

> **Kein DB-Zugriff beim Verbindungsaufbau.** Das ist kein Detail, sondern der Fix für einen
> gemessenen Fehler: die heutige Python-Route macht genau hier eine blockierende Abfrage
> gegen Supabase (30–50 ms) und begrenzt den *ganzen Server* damit auf ~20–25 neue
> Verbindungen pro Sekunde. Beim Reconnect-Sturm nach einem Deploy läuft der Accept-Backlog
> über. Ein gültig signierter, nicht abgelaufener Token ist Beweis genug.

Ungültiger oder abgelaufener Token → Socket schließen mit **1008** (Policy Violation).

### 4.2 Client → Go

Unbekannte Felder sind **verboten** (`extra="forbid"`), `message` ist 1–4096 Zeichen.

```jsonc
// DM
{ "kind": "dm", "to": 14, "message": "...", "client_msg_id": "uuid" }

// Gruppe
{ "kind": "group", "to": 2, "message": "...", "key_version": 1, "client_msg_id": "uuid" }
```

`client_msg_id` ist optional, aber der Client **soll** sie schicken: sie ist der
Duplikat-Schutz bei Reconnect-Rennen und erlaubt ihm, seine eigene Zeile beim späteren
REST-Sync wiederzuerkennen.

### 4.3 Go → Client

Fünf Arten, alle über das Feld `kind` unterscheidbar:

```jsonc
// Bestätigung an den SENDER (Antwort auf B)
{ "kind": "ack", "to": 14, "delivered_live": 1 }

// Eingehende DM an den EMPFÄNGER (aus C)
{ "kind": "dm", "sender_id": 2, "message": "...",
  "created_at": "2026-09-04T20:42:14.872677Z", "client_msg_id": "uuid" }

// Eingehende Gruppen-Nachricht an die MITGLIEDER (aus C)
{ "kind": "group", "group_chat_id": 2, "sender_id": 2, "message": "...",
  "created_at": "...", "key_version": 1, "client_msg_id": "uuid" }

// Auftrag: neuen Gruppenschlüssel erzeugen + verteilen, dann erneut senden
{ "kind": "rekey_required", "group_chat_id": 2 }

// Auftrag: aktuellen Schlüssel holen, neu verschlüsseln, erneut senden
{ "kind": "key_outdated", "group_chat_id": 2, "current_version": 1 }

// Fachlicher Fehler (blockiert, kein Mitglied, Gruppe existiert nicht, ...)
{ "kind": "error", "detail": "..." }
```

`delivered_live` = Anzahl der Empfänger mit offener Verbindung im Moment des Sendens
(DM: 0 oder 1; Gruppe: 0..n). Es ist eine **Auskunft, keine Zustellgarantie** — der Rest
holt die Nachricht per `GET /messages?since=` nach.

> **`rekey_required` und `key_outdated` sind kein `error`.** Sie sind Arbeitsaufträge.
> Wer sie zu `error` verallgemeinert, nimmt dem Client die Möglichkeit, den Schlüssel zu
> erneuern — und dann kann er in dieser Gruppe **nie wieder senden**. In Python erben beide
> von `ChatError`, sie müssen also *vor* dem generischen `except` gefangen werden.

### 4.4 Die Antwortregel

**Auf jede Client-Nachricht folgt genau eine Antwort** — `ack`, `error`, `rekey_required`
oder `key_outdated`. Nie keine. Ein Client, der auf eine Antwort wartet, die nie kommt,
hängt; das ist schlimmer als ein Fehler. Auch wenn Go intern stolpert (Python nicht
erreichbar, unerwarteter Statuscode), schickt es `{"kind": "error", ...}`.

---

## 5. Strecke B — Go → Python (HTTP, intern)

```
POST /internal/ws/dm
POST /internal/ws/group
Header: X-WS-Secret: <ws_internal_secret>
```

```jsonc
// dm
{ "protocol_version": 1, "sender_id": 2, "to": 14,
  "message": "...", "client_msg_id": "uuid" }

// group  (to = group_chat_id)
{ "protocol_version": 1, "sender_id": 2, "to": 2,
  "message": "...", "key_version": 1, "client_msg_id": "uuid" }
```

> ⚠️ **Sicherheitskritisch:** `sender_id` wird von Go **behauptet**, nicht bewiesen.
> Wer diesen Endpoint mit gültigem Secret erreicht, kann als *beliebiger* Nutzer schreiben.
> Daraus folgt: starkes Secret, Vergleich mit `secrets.compare_digest` (nicht `!=`),
> Railway Private Network, und **nicht** öffentlich binden.

### 5.1 Statuscodes

Die Trennung ist bewusst und trägt das ganze Design:

| Code | Bedeutung | Was Go tut |
|---|---|---|
| **200** | Fachliches Ergebnis. Body ist **wörtlich** das Client-JSON — auch bei `error`, `rekey_required`, `key_outdated`. | Body unverändert in den Socket des Senders schreiben. |
| **401** | Secret fehlt oder falsch. | Bug/Angriff. Loggen, `error` an den Client. |
| **422** | Schema passt nicht (unbekannte `protocol_version`, Feld fehlt, `message` zu lang). | Bug: die Repos sind auseinander. Loggen, `error` an den Client. |

**Warum fachliche Fehler 200 sind:** Bekäme `error` ein 400 und `key_outdated` ein 409,
müsste Go Statuscodes auf Protokoll-Nachrichten übersetzen — und wüsste damit wieder etwas
über Fachlogik. So bleibt es beim dummen Rohr: 200 → durchreichen, alles andere → Störung.

### 5.2 Eingabeprüfung: wer prüft was

Damit ein 422 wirklich "die Repos sind auseinander" bedeutet und nicht "ein Client hat
Unsinn geschickt", prüft **Go vorab die Form** — das braucht kein Fachwissen:

- gültiges JSON, `kind` ist `dm` oder `group`
- `message` ist 1–4096 Zeichen
- Pflichtfelder da, keine unbekannten Felder
- bei `group`: `key_version` ist eine Zahl

Verstößt der Client dagegen, antwortet **Go direkt** mit `{"kind": "error", "detail": "..."}`
und fragt Python gar nicht erst. Python prüft dieselben Regeln trotzdem weiter — nicht als
Doppelarbeit, sondern weil Python dem Aufrufer grundsätzlich nicht traut.

Die Zahl **4096** ist damit an zwei Stellen festgeschrieben. Deshalb steht sie hier: dieses
Dokument ist die Quelle, beide Repos folgen ihm.

---

## 6. Strecke C — Python → Go (Redis Pub/Sub)

**Kanal:** `ws:push` (ein globaler Kanal, alle Instanzen lauschen)

```jsonc
{
  "protocol_version": 1,
  "targets": [14],          // user_ids, die das bekommen sollen
  "payload": { "kind": "dm", "sender_id": 2, ... }   // fertiges Client-JSON
}
```

Go: für jede `user_id` in `targets` den *lokalen* Socket suchen; gefunden → `payload`
**wörtlich** hineinschreiben; nicht gefunden → nichts tun (der Nutzer hängt an einer anderen
Instanz oder ist offline). `payload` wird nie geöffnet oder umgebaut.

**Der Sender steht nie in `targets`** — er bekommt sein `ack` über Strecke B.

### 6.1 Warum Verlust hier ungefährlich ist

Python speichert **vor** dem Publish nach Postgres. Ein verlorener Push kostet die
Live-Zustellung, nie die Nachricht — der Client holt sie per `GET /messages?since=` nach.
Deshalb ist Redis Pub/Sub (fire-and-forget, keine Bestätigung) hier ausreichend.

Die ehrliche Schwäche, die man kennen muss: **Pub/Sub hat keinen Rückstau.** Ein zu langsamer
Abonnent wird bei ~32 MB Rückstand von Redis gekappt. Verkraftbar, weil Go nur Bytes in
Sockets schreibt und Clients ohnehin nachladen können.

### 6.2 Warum ein globaler Kanal

Kosten = Nachrichten/s × Größe × Instanzen. Eine Go-Instanz trägt Zehntausende Sockets;
das wird erst bei ~20–50 Instanzen zum Thema. Der Umstieg auf `ws:inst:{id}` sind später
drei Änderungen (Presence gibt die `instance_id` zurück, Publisher gruppiert danach, Go
abonniert seinen eigenen Kanal) — rein serverintern, **Flutter merkt nichts.** Deshalb wird
die `instance_id` ab Tag eins mitgeschrieben, auch wenn Python sie heute wegwirft.

---

## 7. Presence

Nur damit Python `delivered_live` überhaupt noch berechnen kann — es kennt die Verbindungen
ja nicht mehr.

| Wer | Was |
|---|---|
| Go, bei Verbindungsaufbau | `SETEX ws:online:{user_id} 60 {instance_id}` |
| Go, alle ~25 s | denselben `SETEX` erneut (Refresh) |
| Go, beim Trennen | `DEL ws:online:{user_id}` |
| Python | `GET` (DM) bzw. `MGET` (Gruppe), rein lesend |

TTL 60 s bei 25 s Refresh heißt: zwei verpasste Refreshes sind noch verzeihlich. Stirbt eine
Instanz hart, räumt die TTL nach spätestens einer Minute auf.

> **Reihenfolge-Regel: erst publishen, dann Presence lesen.** Presence entscheidet *nur über
> die Zahl im Ack*, nie darüber, *ob* gepusht wird. Eine veraltete Presence verursacht damit
> höchstens eine falsche Auskunft — niemals eine verlorene Nachricht.

Bei mehreren Verbindungen desselben Nutzers (zwei Geräte) gewinnt die zuletzt geschriebene
`instance_id`. Für `delivered_live` reicht das: die Frage ist "irgendwo online?", nicht "wo".

---

## 8. Was Go ausdrücklich **nicht** tut

- keine Datenbank — kein Postgres-Treiber im Go-Repo
- keine Blocks, keine Mitgliedschaften, keine Rekey-Logik
- kein Entschlüsseln, kein Hineinschauen in `message` (E2E: es kann gar nicht)
- kein Zwischenspeichern verpasster Nachrichten — dafür gibt es REST
- kein Umbauen von `payload`

Was Go **schon** tut und Python heute nicht kann: **Ratenbegrenzung auf dem WS-Pfad.**
`slowapi` greift nur bei HTTP, der WebSocket-Pfad ist heute völlig ungebremst. Die Grenzen
sind Go-intern und nicht Teil dieses Protokolls; überschreitet ein Client sie, bekommt er
`{"kind": "error", "detail": "rate limit exceeded"}`.

---

## 9. Wenn etwas geändert wird

1. Erst dieses Dokument, dann der Code — in **beiden** Repos.
2. Neues optionales Feld → `protocol_version` bleibt.
3. Feld entfernt, umbenannt, Typ oder Bedeutung geändert → `protocol_version` **+1**,
   und beide Seiten müssen die neue Zahl kennen, bevor eine Seite sie sendet.
4. Änderungen an Strecke A treffen **Flutter** und brauchen einen App-Rollout —
   Strecke B und C sind rein serverintern und jederzeit gemeinsam änderbar.
