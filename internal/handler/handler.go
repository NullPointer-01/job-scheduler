package handler

import (
	"distributed-job-scheduler/internal/store"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const defaultTimeoutMillis = 3000

type Handler struct {
	st store.Store
}

func NewHandler(st store.Store) *Handler {
	return &Handler{st: st}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", h.loadHome)
	mux.HandleFunc("POST /jobs", h.submitJob)
	mux.HandleFunc("GET /jobs/{id}", h.getJob)
	mux.HandleFunc("DELETE /jobs/{id}", h.cancelJob)
}

func (h *Handler) loadHome(w http.ResponseWriter, r *http.Request) {
	writeResponse(w, http.StatusOK, "Hello Scheduler!")
}

func (h *Handler) submitJob(w http.ResponseWriter, r *http.Request) {
	var req SubmitJobRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON: "+err.Error())
		return
	}

	if req.Type == nil {
		writeError(w, http.StatusBadRequest, "Job type is required")
		return
	}

	data := req.Data
	if req.Data == nil {
		data = json.RawMessage("{}")
	}

	now := time.Now().Truncate(time.Second)
	runAt := req.RunAt
	if req.RunAt == nil {
		runAt = &now
	}

	timeoutMillis := req.TimeoutMillis
	if req.TimeoutMillis == 0 {
		timeoutMillis = defaultTimeoutMillis
	}

	job := store.Job{
		Id:            uuid.New(),
		Type:          *req.Type,
		Data:          data,
		Status:        store.StateScheduled,
		RunAt:         *runAt,
		TimeoutMillis: timeoutMillis,
		CreatedAt:     now,
		ModifiedAt:    now,
	}

	createdJob, err := h.st.CreateJob(r.Context(), job)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to submit job: "+err.Error())
		return
	}
	writeResponse(w, http.StatusOK, createdJob)
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	jobId, ok := parseId(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusBadRequest, "Invalid job id")
		return
	}

	job, err := h.st.GetJob(r.Context(), jobId)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "Job not found")
		return
	}

	writeResponse(w, http.StatusOK, job)
}

func (h *Handler) cancelJob(w http.ResponseWriter, r *http.Request) {
	jobId, ok := parseId(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusBadRequest, "Invalid job id")
		return
	}

	err := h.st.CancelJob(r.Context(), jobId)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "Job not found")
		return
	}

	if errors.Is(err, store.ErrDenyCancellation) {
		writeError(w, http.StatusConflict, "Job cannot be cancelled")
		return
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeResponse(w, http.StatusOK, "Job cancelled")
}

func parseId(raw string) (uuid.UUID, bool) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, false
	}

	return id, true
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeResponse(w, status, message)
}

func writeResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
