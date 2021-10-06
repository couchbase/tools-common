package elutil

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

// EventID is the unique identifier of the event type, currently each service is apportioned 1024 event ids.
//
// NOTE: See MB-47035 and its parent for more information about which services are supplied which ids.
type EventID uint

// Event represents an event, and is the structure which will be used when reporting events using a 'Service'.
type Event struct {
	// Required attributes which, if not supplied will result in an error.
	Component   Component `json:"component"`
	Severity    Severity  `json:"severity"`
	EventID     EventID   `json:"event_id"`
	Description string    `json:"description"`

	// Optional attributes which may/or may not be supplied; the general recommendation is to include some additional
	// useful information which describes the event.
	ExtraAttributes interface{} `json:"extra_attributes"`
	SubComponent    string      `json:"sub_component"`
}

// MarshalJSON implements the 'json.Marshaller' interface, and fills in any required automatically generated fields.
func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Timestamp       string      `json:"timestamp,omitempty"`
		Component       string      `json:"component,omitempty"`
		Severity        string      `json:"severity,omitempty"`
		EventID         uint        `json:"event_id,omitempty"`
		Description     string      `json:"description,omitempty"`
		UUID            string      `json:"uuid,omitempty"`
		ExtraAttributes interface{} `json:"extra_attributes,omitempty"`
		SubComponent    string      `json:"sub_component,omitempty"`
	}{
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
		Component:       string(e.Component),
		Severity:        string(e.Severity),
		EventID:         uint(e.EventID),
		Description:     e.Description,
		UUID:            uuid.NewString(),
		ExtraAttributes: e.ExtraAttributes,
		SubComponent:    e.SubComponent,
	})
}
