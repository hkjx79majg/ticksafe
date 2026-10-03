package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"unicode"
	"unicode/utf8"
)

const (
	analyzePath         = "/v1/schedules/analyze"
	maxAnalyzeBodyBytes = 1 << 20
	maxHorizon          = 1_000_000_000_000
	maxTasks            = 256
	maxIDRunes          = 64
)

type analyzeTaskIn struct {
	ID        string `json:"id"`
	Priority  *int64 `json:"priority"`
	Release   *int64 `json:"release"`
	Execution *int64 `json:"execution"`
	Deadline  *int64 `json:"deadline"`
}

type analyzeRequest struct {
	Horizon *int64           `json:"horizon"`
	Tasks   *[]analyzeTaskIn `json:"tasks"`
}

type timelineInterval struct {
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
	Timeline []timelineInterval `json:"timeline"`
	Results  []taskResult       `json:"results"`
}

type scheduledTask struct {
	id        string
	priority  int64
	release   int64
	execution int64
	deadline  int64
}

func handleAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req analyzeRequest
	if !decodeStrictRequest(w, r, &req) {
		return
	}
	tasks, ok := validateAnalyzeRequest(&req)
	if !ok {
		writeAPIError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	resp := simulateSchedule(tasks, *req.Horizon)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func writeAPIError(w http.ResponseWriter, status int, code string) {
	http.Error(w, `{"error":{"code":"`+code+`"}}`, status)
}

// decodeStrictRequest runs the shared request pipeline for JSON endpoints:
// 405 is handled by callers before this point. It enforces the
// application/json media type, the 1 MiB body limit, unique object keys, no
// trailing content and no unknown fields, and maps decode failures to
// 400 invalid_json or 422 validation_failed. It returns false after writing
// the error response.
func decodeStrictRequest(w http.ResponseWriter, r *http.Request, dst any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeAPIError(w, http.StatusUnsupportedMediaType, "unsupported_media_type")
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAnalyzeBodyBytes))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json")
		return false
	}
	// Reject duplicate keys and trailing tokens before the strict struct decode.
	if err := rejectDuplicateKeys(body); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json")
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		// Well-formed JSON with a value of the wrong JSON type violates the
		// request schema (422); malformed syntax and unknown fields stay 400.
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			writeAPIError(w, http.StatusUnprocessableEntity, "validation_failed")
			return false
		}
		writeAPIError(w, http.StatusBadRequest, "invalid_json")
		return false
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		writeAPIError(w, http.StatusBadRequest, "invalid_json")
		return false
	}
	return true
}

var errDuplicateKey = errors.New("duplicate JSON object key")

// rejectDuplicateKeys tokenizes the whole JSON document and rejects repeated
// member names at any object nesting level or trailing content.
func rejectDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := scanDuplicateKeys(dec); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return fmt.Errorf("trailing content after JSON document")
	}
	return nil
}

func scanDuplicateKeys(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch d {
	case '{':
		seen := make(map[string]struct{})
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return err
			}
			key := keyTok.(string)
			if _, dup := seen[key]; dup {
				return errDuplicateKey
			}
			seen[key] = struct{}{}
			if err := scanDuplicateKeys(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	case '[':
		for dec.More() {
			if err := scanDuplicateKeys(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	}
	return nil
}

func validateAnalyzeRequest(req *analyzeRequest) ([]scheduledTask, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, false
	}
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, false
	}
	horizon := *req.Horizon
	tasks := make([]scheduledTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	for i := range *req.Tasks {
		in := (*req.Tasks)[i]
		if !validTaskID(in.ID) {
			return nil, false
		}
		if _, dup := ids[in.ID]; dup {
			return nil, false
		}
		ids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, false
		}
		if in.Release == nil || *in.Release < 0 || *in.Release >= horizon {
			return nil, false
		}
		if in.Execution == nil || *in.Execution <= 0 {
			return nil, false
		}
		if in.Deadline == nil || *in.Deadline <= *in.Release {
			return nil, false
		}
		tasks = append(tasks, scheduledTask{
			id:        in.ID,
			priority:  *in.Priority,
			release:   *in.Release,
			execution: *in.Execution,
			deadline:  *in.Deadline,
		})
	}
	return tasks, true
}

// validTaskID reports whether id holds 1..64 non-whitespace Unicode code points.
func validTaskID(id string) bool {
	if id == "" {
		return false
	}
	count := 0
	for len(id) > 0 {
		r, size := utf8.DecodeRuneInString(id)
		if r == utf8.RuneError && size == 1 {
			return false
		}
		if unicode.IsSpace(r) {
			return false
		}
		count++
		id = id[size:]
	}
	return count <= maxIDRunes
}

type executionSegment struct {
	index      int
	start, end int64
}

// simulateSchedule runs an event-driven single-core fixed-priority preemptive
// simulation over [0, horizon).
func simulateSchedule(tasks []scheduledTask, horizon int64) analyzeResponse {
	n := len(tasks)
	remaining := make([]int64, n)
	for i, t := range tasks {
		remaining[i] = t.execution
	}
	released := make([]bool, n)
	executed := make([]int64, n)
	completion := make([]*int64, n)

	// Indices sorted by (release, input order) for release bookkeeping and
	// same-priority tie breaking.
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		ia, ib := order[a], order[b]
		if tasks[ia].release != tasks[ib].release {
			return tasks[ia].release < tasks[ib].release
		}
		return ia < ib
	})
	nextRelease := 0
	releaseUpTo := func(now int64) {
		for nextRelease < n && tasks[order[nextRelease]].release <= now {
			released[order[nextRelease]] = true
			nextRelease++
		}
	}

	// Best runnable task: smallest priority, then earliest release, then
	// earliest input index.
	bestRunnable := func() int {
		best := -1
		for i := 0; i < n; i++ {
			if !released[i] || remaining[i] <= 0 {
				continue
			}
			if best == -1 {
				best = i
				continue
			}
			a, b := tasks[i], tasks[best]
			if a.priority < b.priority ||
				(a.priority == b.priority && a.release < b.release) ||
				(a.priority == b.priority && a.release == b.release && i < best) {
				best = i
			}
		}
		return best
	}

	var segments []executionSegment
	current := -1
	var now int64
	for now < horizon {
		releaseUpTo(now)
		if current >= 0 && remaining[current] == 0 {
			current = -1
		}
		best := bestRunnable()
		switch {
		case current == -1:
			current = best
		case best != -1 && tasks[best].priority < tasks[current].priority:
			// Strictly higher priority preempts; equal priority never does.
			current = best
		}

		if current == -1 {
			// Idle: jump to the next release (or the horizon).
			if nextRelease >= n {
				now = horizon
				break
			}
			now = tasks[order[nextRelease]].release
			continue
		}

		// Run until completion, the horizon, or the next strictly-higher-priority
		// release, whichever comes first.
		runEnd := horizon
		if remaining[current] < horizon-now {
			runEnd = now + remaining[current]
		}
		for k := nextRelease; k < n; k++ {
			j := order[k]
			if tasks[j].priority < tasks[current].priority &&
				tasks[j].release > now && tasks[j].release < runEnd {
				runEnd = tasks[j].release
			}
		}

		segments = append(segments, executionSegment{index: current, start: now, end: runEnd})
		delta := runEnd - now
		executed[current] += delta
		remaining[current] -= delta
		now = runEnd
		if remaining[current] == 0 {
			done := now
			completion[current] = &done
			current = -1
		}
	}

	timeline := make([]timelineInterval, 0, len(segments))
	for _, seg := range segments {
		last := len(timeline) - 1
		if last >= 0 && timeline[last].TaskID == tasks[seg.index].id && timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, timelineInterval{
			TaskID: tasks[seg.index].id,
			Start:  seg.start,
			End:    seg.end,
		})
	}

	results := make([]taskResult, n)
	for i, t := range tasks {
		var status string
		switch c := completion[i]; {
		case c != nil:
			if *c <= t.deadline {
				status = "met"
			} else {
				status = "missed"
			}
		case t.deadline <= horizon:
			status = "missed"
		default:
			status = "pending"
		}
		results[i] = taskResult{
			Executed:       executed[i],
			Remaining:      remaining[i],
			Completion:     completion[i],
			DeadlineStatus: status,
		}
	}

	return analyzeResponse{Timeline: timeline, Results: results}
}
