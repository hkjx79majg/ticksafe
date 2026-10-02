package server

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"sort"
	"unicode"
	"unicode/utf8"
)

const (
	maxHorizon = 1_000_000_000_000
	maxTasks   = 256
	maxIDRunes = 64
	maxBody    = 1 << 20
)

type analyzeRequest struct {
	Horizon *int64      `json:"horizon"`
	Tasks   []taskInput `json:"tasks"`
}

type taskInput struct {
	ID        *string `json:"id"`
	Priority  *int64  `json:"priority"`
	Release   *int64  `json:"release"`
	Execution *int64  `json:"execution"`
	Deadline  *int64  `json:"deadline"`
}

type interval struct {
	TaskID string `json:"taskId"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

type taskResult struct {
	Executed       int64  `json:"executed"`
	Remaining      int64  `json:"remaining"`
	Completion     *int64 `json:"completion"`
	DeadlineStatus string `json:"deadlineStatus"`
}

type analyzeResponse struct {
	Timeline []interval   `json:"timeline"`
	Results  []taskResult `json:"results"`
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": code},
	})
}

func analyzeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req analyzeRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	tasks, horizon, ok := validateRequest(&req)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	resp := analyze(horizon, tasks)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

type schedTask struct {
	id        string
	priority  int64
	release   int64
	execution int64
	deadline  int64
	index     int
}

func validID(s string) bool {
	n := utf8.RuneCountInString(s)
	if n < 1 || n > maxIDRunes {
		return false
	}
	for _, r := range s {
		if unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func validateRequest(req *analyzeRequest) ([]schedTask, int64, bool) {
	if req.Horizon == nil || *req.Horizon < 1 || *req.Horizon > maxHorizon {
		return nil, 0, false
	}
	horizon := *req.Horizon
	if len(req.Tasks) < 1 || len(req.Tasks) > maxTasks {
		return nil, 0, false
	}
	seen := make(map[string]struct{}, len(req.Tasks))
	tasks := make([]schedTask, len(req.Tasks))
	for i, t := range req.Tasks {
		if t.ID == nil || t.Priority == nil || t.Release == nil ||
			t.Execution == nil || t.Deadline == nil {
			return nil, 0, false
		}
		if !validID(*t.ID) {
			return nil, 0, false
		}
		if _, dup := seen[*t.ID]; dup {
			return nil, 0, false
		}
		seen[*t.ID] = struct{}{}
		if *t.Priority < 0 || *t.Priority > 255 {
			return nil, 0, false
		}
		if *t.Release < 0 || *t.Release >= horizon {
			return nil, 0, false
		}
		if *t.Execution < 1 {
			return nil, 0, false
		}
		if *t.Deadline <= *t.Release {
			return nil, 0, false
		}
		tasks[i] = schedTask{
			id:        *t.ID,
			priority:  *t.Priority,
			release:   *t.Release,
			execution: *t.Execution,
			deadline:  *t.Deadline,
			index:     i,
		}
	}
	return tasks, horizon, true
}

// higher reports whether task a should run ahead of task b: smaller priority
// value first, then earlier release, then earlier input order.
func higher(a, b schedTask) bool {
	if a.priority != b.priority {
		return a.priority < b.priority
	}
	if a.release != b.release {
		return a.release < b.release
	}
	return a.index < b.index
}

func analyze(horizon int64, tasks []schedTask) analyzeResponse {
	order := make([]int, len(tasks))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(x, y int) bool {
		a, b := tasks[order[x]], tasks[order[y]]
		if a.release != b.release {
			return a.release < b.release
		}
		return a.index < b.index
	})

	remaining := make([]int64, len(tasks))
	completion := make([]int64, len(tasks))
	ready := make([]bool, len(tasks))
	for i := range tasks {
		remaining[i] = tasks[i].execution
		completion[i] = -1
	}

	timeline := []interval{}
	next := 0
	for t := int64(0); t < horizon; {
		for next < len(order) && tasks[order[next]].release <= t {
			ready[order[next]] = true
			next++
		}
		best := -1
		for i := range tasks {
			if !ready[i] || remaining[i] == 0 {
				continue
			}
			if best == -1 || higher(tasks[i], tasks[best]) {
				best = i
			}
		}
		if best == -1 {
			if next >= len(order) {
				break
			}
			t = tasks[order[next]].release
			continue
		}
		runUntil := horizon
		if next < len(order) && tasks[order[next]].release < runUntil {
			runUntil = tasks[order[next]].release
		}
		if remaining[best] < runUntil-t {
			runUntil = t + remaining[best]
		}
		if n := len(timeline); n > 0 && timeline[n-1].TaskID == tasks[best].id && timeline[n-1].End == t {
			timeline[n-1].End = runUntil
		} else {
			timeline = append(timeline, interval{TaskID: tasks[best].id, Start: t, End: runUntil})
		}
		remaining[best] -= runUntil - t
		if remaining[best] == 0 {
			completion[best] = runUntil
			ready[best] = false
		}
		t = runUntil
	}

	results := make([]taskResult, len(tasks))
	for i := range tasks {
		res := taskResult{
			Executed:  tasks[i].execution - remaining[i],
			Remaining: remaining[i],
		}
		if completion[i] >= 0 {
			c := completion[i]
			res.Completion = &c
		}
		switch {
		case completion[i] >= 0 && completion[i] <= tasks[i].deadline:
			res.DeadlineStatus = "met"
		case tasks[i].deadline <= horizon:
			res.DeadlineStatus = "missed"
		default:
			res.DeadlineStatus = "pending"
		}
		results[i] = res
	}
	return analyzeResponse{Timeline: timeline, Results: results}
}
