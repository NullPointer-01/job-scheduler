package handler

import (
	"encoding/json"
	"time"
)

type SubmitJobRequest struct {
	Type  *string         `json:"type"`
	Data  json.RawMessage `json:"data"`
	RunAt *time.Time      `json:"run_at"`
}
