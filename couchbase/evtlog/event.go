package evtlog

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Component represents to component which is reporting this event, for a complete list of supported components see
// MB-47035 and its parent.
type Component string

// Severity represents the severity of an event.
type Severity string

const (
	// SeverityInfo is the 'info' level, describing something that occurs during normal execution and is therefore
	// expected.
	SeverityInfo Severity = "info"

	// SeverityWarn is the 'warn' level, describing something that's normal (or perhaps rare/unexpected).
	SeverityWarn Severity = "warn"

	// SeverityError is the 'error' level, describing error scenarios where something has failed/occurred that shouldn't
	// happen during normal execution.
	SeverityError Severity = "error"

	// SeverityFatal is the 'fatal' level, describing an error case which is unrecoverable.
	SeverityFatal Severity = "fatal"
)

// EventIDsThatInvalidateSnapshots is the set of Couchbase system event IDs that must not have happened during a
// snapshot backup. If one or more of them has happened since the snapshots started taking place then the snapshots
// should be thrown away and retried.
var EventIDsThatInvalidateSnapshots = map[EventID]struct{}{
	0:    {}, // Node successfully joined the cluster.
	1:    {}, // Service started.
	4:    {}, // Rebalance failed.
	5:    {}, // Rebalance interrupted.
	6:    {}, // Graceful failover initiated.
	7:    {}, // Graceful failover completed.
	8:    {}, // Graceful failover failed.
	9:    {}, // Graceful failover interrupted.
	10:   {}, // Hard failover initiated.
	11:   {}, // Hard failover completed.
	12:   {}, // Hard failover failed.
	13:   {}, // Hard failover interrupted.
	14:   {}, // Auto failover initiated.
	15:   {}, // Auto failover completed.
	16:   {}, // Auto failover failed.
	17:   {}, // Auto failover warning.
	19:   {}, // Service crashed.
	20:   {}, // Node down.
	8192: {}, // Bucket created.
	8193: {}, // Bucket deleted.
}

// EventID is the unique identifier of the event type, currently each service is apportioned 1024 event ids.
//
// NOTE: See MB-47035 and its parent for more information about which services are supplied which ids.
type EventID uint

// InvalidatesSnapshots returns true if the given event ID should invalidate a snapshot backup.
func (e EventID) InvalidatesSnapshots() bool {
	_, ok := EventIDsThatInvalidateSnapshots[e]
	return ok
}

// Event represents an event, and is the structure which will be used when reporting events using a 'Service'.
type Event struct {
	// Required attributes which, if not supplied will result in an error.
	Component   Component `json:"component"`
	Severity    Severity  `json:"severity"`
	EventID     EventID   `json:"event_id"`
	Description string    `json:"description"`

	// Optional attributes which may/or may not be supplied; the general recommendation is to include some additional
	// useful information which describes the event.
	ExtraAttributes any    `json:"extra_attributes"`
	SubComponent    string `json:"sub_component"`
}

// MarshalJSON implements the 'json.Marshaller' interface, and fills in any required automatically generated fields.
func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Timestamp       string `json:"timestamp,omitempty"`
		Component       string `json:"component,omitempty"`
		Severity        string `json:"severity,omitempty"`
		EventID         uint   `json:"event_id,omitempty"`
		Description     string `json:"description,omitempty"`
		UUID            string `json:"uuid,omitempty"`
		ExtraAttributes any    `json:"extra_attributes,omitempty"`
		SubComponent    string `json:"sub_component,omitempty"`
	}{
		Timestamp:       timestamp(),
		Component:       string(e.Component),
		Severity:        string(e.Severity),
		EventID:         uint(e.EventID),
		Description:     e.Description,
		UUID:            uuid.NewString(),
		ExtraAttributes: e.ExtraAttributes,
		SubComponent:    e.SubComponent,
	})
}

// timestamp returns the current UTC time formatted in a stable version of ISO 8601.
func timestamp() string {
	return stableISO8601(time.Now().UTC())
}

// stableISO8601 formats the given time using a stable version of ISO 8601.
func stableISO8601(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000Z")
}
