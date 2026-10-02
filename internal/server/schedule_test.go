package server

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func postAnalyze(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, analyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func analyzeOK(t *testing.T, body string) analyzeResponse {
	t.Helper()
	rec := postAnalyze(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp analyzeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, status, rec.Body.String())
	}
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("error payload is not JSON: %v: %s", err, rec.Body.String())
	}
	if payload.Error.Code != code {
		t.Fatalf("error code = %q, want %q", payload.Error.Code, code)
	}
}

func TestAnalyzePreemptionAndDeadlines(t *testing.T) {
	// A (low) starts at 0; B (high) releases at 5 and preempts.
	resp := analyzeOK(t, `{
		"horizon": 100,
		"tasks": [
			{"id":"A","priority":2,"release":0,"execution":10,"deadline":10},
			{"id":"B","priority":1,"release":5,"execution":10,"deadline":20}
		]
	}`)
	wantTimeline := []timelineInterval{
		{TaskID: "A", Start: 0, End: 5},
		{TaskID: "B", Start: 5, End: 15},
		{TaskID: "A", Start: 15, End: 20},
	}
	if len(resp.Timeline) != len(wantTimeline) {
		t.Fatalf("timeline = %+v, want %+v", resp.Timeline, wantTimeline)
	}
	for i := range wantTimeline {
		if resp.Timeline[i] != wantTimeline[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v", i, resp.Timeline[i], wantTimeline[i])
		}
	}
	if len(resp.Results) != 2 {
		t.Fatalf("results = %+v", resp.Results)
	}
	completion := func(r taskResult) int64 {
		if r.Completion == nil {
			t.Fatalf("completion is nil: %+v", r)
		}
		return *r.Completion
	}
	a, b := resp.Results[0], resp.Results[1]
	if a.Executed != 10 || a.Remaining != 0 || completion(a) != 20 || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
	if b.Executed != 10 || b.Remaining != 0 || completion(b) != 15 || b.DeadlineStatus != "met" {
		t.Fatalf("B result = %+v", b)
	}
}

func TestAnalyzeSamePriorityFIFO(t *testing.T) {
	// Same priority: B releasing at 5 must not preempt A; C released at 0
	// (after A in input order) runs after A but before the later-released B.
	resp := analyzeOK(t, `{
		"horizon": 50,
		"tasks": [
			{"id":"A","priority":1,"release":0,"execution":10,"deadline":50},
			{"id":"B","priority":1,"release":5,"execution":5,"deadline":50},
			{"id":"C","priority":1,"release":0,"execution":3,"deadline":50}
		]
	}`)
	wantTimeline := []timelineInterval{
		{TaskID: "A", Start: 0, End: 10},
		{TaskID: "C", Start: 10, End: 13},
		{TaskID: "B", Start: 13, End: 18},
	}
	if len(resp.Timeline) != len(wantTimeline) {
		t.Fatalf("timeline = %+v, want %+v", resp.Timeline, wantTimeline)
	}
	for i := range wantTimeline {
		if resp.Timeline[i] != wantTimeline[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v", i, resp.Timeline[i], wantTimeline[i])
		}
	}
}

func TestAnalyzeIdleOmittedAndHorizonStatuses(t *testing.T) {
	// Task releases at 10: the leading idle gap must be omitted; it meets a
	// deadline equal to its completion.
	resp := analyzeOK(t, `{
		"horizon": 30,
		"tasks": [{"id":"T","priority":0,"release":10,"execution":5,"deadline":15}]
	}`)
	if len(resp.Timeline) != 1 || resp.Timeline[0] != (timelineInterval{TaskID: "T", Start: 10, End: 15}) {
		t.Fatalf("timeline = %+v", resp.Timeline)
	}
	r := resp.Results[0]
	if r.Executed != 5 || r.Remaining != 0 || r.Completion == nil || *r.Completion != 15 || r.DeadlineStatus != "met" {
		t.Fatalf("result = %+v", r)
	}

	// Unfinished at horizon: deadline == horizon means missed; deadline past
	// horizon means pending. Execution is truncated to the horizon.
	resp = analyzeOK(t, `{
		"horizon": 20,
		"tasks": [
			{"id":"M","priority":0,"release":0,"execution":30,"deadline":20},
			{"id":"P","priority":1,"release":0,"execution":30,"deadline":21}
		]
	}`)
	if len(resp.Timeline) != 1 || resp.Timeline[0] != (timelineInterval{TaskID: "M", Start: 0, End: 20}) {
		t.Fatalf("timeline = %+v", resp.Timeline)
	}
	m, p := resp.Results[0], resp.Results[1]
	if m.Executed != 20 || m.Remaining != 10 || m.Completion != nil || m.DeadlineStatus != "missed" {
		t.Fatalf("M result = %+v", m)
	}
	if p.Executed != 0 || p.Remaining != 30 || p.Completion != nil || p.DeadlineStatus != "pending" {
		t.Fatalf("P result = %+v", p)
	}

	// Completing exactly at the horizon with deadline == horizon is met.
	resp = analyzeOK(t, `{
		"horizon": 5,
		"tasks": [{"id":"E","priority":0,"release":0,"execution":5,"deadline":5}]
	}`)
	e := resp.Results[0]
	if e.Executed != 5 || e.Remaining != 0 || e.Completion == nil || *e.Completion != 5 || e.DeadlineStatus != "met" {
		t.Fatalf("E result = %+v", e)
	}
}

func TestAnalyzeIntervalsBoundedAndDeterministic(t *testing.T) {
	// Repeated higher-priority releases; intervals must stay inside [0,h),
	// stay half-open and non-overlapping, and repeat identically.
	body := `{
		"horizon": 12,
		"tasks": [
			{"id":"low","priority":9,"release":0,"execution":20,"deadline":100},
			{"id":"hi","priority":0,"release":3,"execution":2,"deadline":10},
			{"id":"hi2","priority":0,"release":7,"execution":3,"deadline":11}
		]
	}`
	first := analyzeOK(t, body)
	second := analyzeOK(t, body)
	if len(first.Timeline) == 0 {
		t.Fatalf("expected execution intervals")
	}
	var prevEnd int64
	for _, iv := range first.Timeline {
		if iv.Start < 0 || iv.End > 12 || iv.Start >= iv.End {
			t.Fatalf("interval out of bounds or empty: %+v", iv)
		}
		if iv.Start < prevEnd {
			t.Fatalf("overlapping/out-of-order interval: %+v", iv)
		}
		prevEnd = iv.End
	}
	for i := range first.Timeline {
		if first.Timeline[i] != second.Timeline[i] {
			t.Fatalf("non-deterministic timeline: %+v vs %+v", first.Timeline, second.Timeline)
		}
	}
}

func TestAnalyzeAcceptsUnicodeAndBoundaryValues(t *testing.T) {
	resp := analyzeOK(t, `{
		"horizon": 1000000000000,
		"tasks": [
			{"id":"任務-α","priority":255,"release":999999999999,"execution":1,"deadline":1000000000000}
		]
	}`)
	if len(resp.Timeline) != 1 || resp.Timeline[0].TaskID != "任務-α" {
		t.Fatalf("timeline = %+v", resp.Timeline)
	}
	if resp.Results[0].DeadlineStatus != "met" {
		t.Fatalf("result = %+v", resp.Results[0])
	}
}

func TestAnalyzeInvalidJSON(t *testing.T) {
	cases := map[string]string{
		"empty":              "",
		"syntax error":       "{",
		"trailing token":     `{"horizon":3,"tasks":[{"id":"a","priority":0,"release":0,"execution":1,"deadline":1}]} garbage`,
		"unknown field":      `{"horizon":3,"tasks":[],"bogus":1}`,
		"duplicate top key":  `{"horizon":3,"horizon":4,"tasks":[]}`,
		"duplicate task key": `{"horizon":3,"tasks":[{"id":"a","id":"b","priority":0,"release":0,"execution":1,"deadline":1}]}`,
		"malformed number":   `{"horizon":01,"tasks":[]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postAnalyze(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}
}

func TestAnalyzeValidationFailures(t *testing.T) {
	cases := map[string]string{
		"json null body":       `null`,
		"json array body":      `[]`,
		"missing horizon":      `{"tasks":[]}`,
		"missing tasks":        `{"horizon":3}`,
		"zero horizon":         `{"horizon":0,"tasks":[]}`,
		"negative horizon":     `{"horizon":-1,"tasks":[]}`,
		"horizon too large":    `{"horizon":1000000000001,"tasks":[]}`,
		"empty tasks":          `{"horizon":3,"tasks":[]}`,
		"empty id":             `{"horizon":3,"tasks":[{"id":"","priority":0,"release":0,"execution":1,"deadline":1}]}`,
		"whitespace id":        `{"horizon":3,"tasks":[{"id":"a b","priority":0,"release":0,"execution":1,"deadline":1}]}`,
		"ideographic space id": `{"horizon":3,"tasks":[{"id":"a　b","priority":0,"release":0,"execution":1,"deadline":1}]}`,
		"duplicate id":         `{"horizon":30,"tasks":[` + taskObj("a", 0, 0, 1, 1) + "," + taskObj("a", 0, 0, 1, 1) + "]}",
		"priority negative":    `{"horizon":3,"tasks":[{"id":"a","priority":-1,"release":0,"execution":1,"deadline":1}]}`,
		"priority too large":   `{"horizon":3,"tasks":[{"id":"a","priority":256,"release":0,"execution":1,"deadline":1}]}`,
		"release negative":     `{"horizon":3,"tasks":[{"id":"a","priority":0,"release":-1,"execution":1,"deadline":1}]}`,
		"release at horizon":   `{"horizon":3,"tasks":[{"id":"a","priority":0,"release":3,"execution":1,"deadline":4}]}`,
		"execution zero":       `{"horizon":3,"tasks":[{"id":"a","priority":0,"release":0,"execution":0,"deadline":1}]}`,
		"deadline at release":  `{"horizon":3,"tasks":[{"id":"a","priority":0,"release":0,"execution":1,"deadline":0}]}`,
		"deadline before rel":  `{"horizon":3,"tasks":[{"id":"a","priority":0,"release":1,"execution":1,"deadline":1}]}`,
		"missing task field":   `{"horizon":3,"tasks":[{"id":"a","priority":0,"release":0,"execution":1}]}`,
		"null task field":      `{"horizon":3,"tasks":[{"id":"a","priority":null,"release":0,"execution":1,"deadline":1}]}`,
		"wrong field type":     `{"horizon":3,"tasks":[{"id":"a","priority":"0","release":0,"execution":1,"deadline":1}]}`,
		"tasks not an array":   `{"horizon":3,"tasks":{}}`,
		"horizon not a number": `{"horizon":"3","tasks":[]}`,
		"number out of int64":  `{"horizon":999999999999999999999999999999,"tasks":[]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postAnalyze(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	// 256 tasks is the upper bound; 257 fails.
	var many bytes.Buffer
	many.WriteString(`{"horizon":100000,"tasks":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(taskObj(string(rune('a'+i%26))+strconv.Itoa(i), 0, 0, 1, 1000000))
	}
	many.WriteString(`]}`)
	assertErrorCode(t, postAnalyze(t, many.String(), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")

	// A 65-rune id fails; a 64-rune id (with a multibyte rune) succeeds.
	long65 := strings.Repeat("x", 65)
	assertErrorCode(t, postAnalyze(t,
		`{"horizon":3,"tasks":[{"id":"`+long65+`","priority":0,"release":0,"execution":1,"deadline":1}]}`,
		"application/json"), http.StatusUnprocessableEntity, "validation_failed")

	id64 := strings.Repeat("z", 63) + "é"
	analyzeOK(t, `{"horizon":3,"tasks":[{"id":"`+id64+`","priority":0,"release":0,"execution":1,"deadline":1}]}`)
}

func taskObj(id string, prio, rel, exec, dead int64) string {
	return `{"id":"` + id + `","priority":` + strconv.FormatInt(prio, 10) +
		`,"release":` + strconv.FormatInt(rel, 10) +
		`,"execution":` + strconv.FormatInt(exec, 10) +
		`,"deadline":` + strconv.FormatInt(dead, 10) + `}`
}

func TestAnalyzeMethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, analyzePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
}

func TestAnalyzeUnsupportedMediaType(t *testing.T) {
	body := `{"horizon":3,"tasks":[]}`
	// No Content-Type at all.
	assertErrorCode(t, postAnalyze(t, body, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	// Explicitly wrong type.
	assertErrorCode(t, postAnalyze(t, body, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	// Parameters on application/json are accepted.
	rec := postAnalyze(t, `{"horizon":3,"tasks":[{"id":"a","priority":0,"release":0,"execution":1,"deadline":1}]}`,
		"application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestAnalyzeUnknownPathAndHealthzUnchanged(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown path status = %d, want 404", rec.Code)
	}

	// Health body still matches the frozen baseline.
	rec = httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d", rec.Code)
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("health payload is not JSON: %v", err)
	}
	if payload["status"] != "ok" || payload["service"] != "ticksafe" || payload["version"] != Version {
		t.Fatalf("unexpected health payload: %v", payload)
	}

	// Method restriction on healthz remains GET-only.
	rec = httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("healthz POST: status=%d allow=%q", rec.Code, rec.Header().Get("Allow"))
	}
}

func TestAnalyzeNoPartialResultsOnError(t *testing.T) {
	rec := postAnalyze(t, `{"horizon": 3, "tasks": [{"id":"a"`, "application/json")
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_json")
	body, _ := io.ReadAll(rec.Body)
	if strings.Contains(string(body), "timeline") || strings.Contains(string(body), "results") {
		t.Fatalf("error response leaked partial results: %s", body)
	}
}

// TestSimulateScheduleDifferential compares the event-driven simulation
// against an independent tick-by-tick reference implementation.
func TestSimulateScheduleDifferential(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	for iter := 0; iter < 2000; iter++ {
		n := 1 + rng.Intn(8)
		horizon := int64(1 + rng.Intn(40))
		tasks := make([]scheduledTask, n)
		ids := map[string]bool{}
		for i := 0; i < n; i++ {
			var id string
			for {
				id = "t" + strconv.Itoa(rng.Intn(20))
				if !ids[id] {
					ids[id] = true
					break
				}
			}
			tasks[i] = scheduledTask{
				id:        id,
				priority:  int64(rng.Intn(4)),
				release:   int64(rng.Intn(int(horizon))),
				execution: int64(1 + rng.Intn(15)),
				deadline:  int64(0),
			}
			if rng.Intn(2) == 0 {
				tasks[i].deadline = tasks[i].release + int64(1+rng.Intn(30))
			} else {
				tasks[i].deadline = tasks[i].release + int64(1+rng.Intn(3))
			}
		}

		got := simulateSchedule(tasks, horizon)

		// Independent tick-by-tick model.
		remaining := make([]int64, n)
		for i, tk := range tasks {
			remaining[i] = tk.execution
		}
		var refSegs []executionSegment
		completion := make([]*int64, n)
		current := -1
		for now := int64(0); now < horizon; now++ {
			best := -1
			for i := 0; i < n; i++ {
				if tasks[i].release > now || remaining[i] == 0 {
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
			if current >= 0 && remaining[current] == 0 {
				current = -1
			}
			if best != -1 && (current == -1 || tasks[best].priority < tasks[current].priority) {
				current = best
			}
			if current == -1 {
				continue
			}
			remaining[current]--
			last := len(refSegs) - 1
			if last >= 0 && refSegs[last].index == current && refSegs[last].end == now {
				refSegs[last].end = now + 1
			} else {
				refSegs = append(refSegs, executionSegment{index: current, start: now, end: now + 1})
			}
			if remaining[current] == 0 {
				done := now + 1
				completion[current] = &done
				current = -1
			}
		}

		if len(got.Timeline) != len(refSegs) {
			t.Fatalf("iter %d (seed %d): interval count %d != %d; got %+v",
				iter, seed, len(got.Timeline), len(refSegs), got.Timeline)
		}
		for k, seg := range refSegs {
			want := timelineInterval{TaskID: tasks[seg.index].id, Start: seg.start, End: seg.end}
			if got.Timeline[k] != want {
				t.Fatalf("iter %d (seed %d): interval %d got %+v, want %+v",
					iter, seed, k, got.Timeline[k], want)
			}
		}
		for i, tk := range tasks {
			executed := tk.execution - remaining[i]
			if got.Results[i].Executed != executed {
				t.Fatalf("iter %d (seed %d): task %d executed got %d want %d",
					iter, seed, i, got.Results[i].Executed, executed)
			}
			if got.Results[i].Remaining != remaining[i] {
				t.Fatalf("iter %d (seed %d): task %d remaining got %d want %d",
					iter, seed, i, got.Results[i].Remaining, remaining[i])
			}
			if (got.Results[i].Completion == nil) != (completion[i] == nil) ||
				(got.Results[i].Completion != nil && *got.Results[i].Completion != *completion[i]) {
				t.Fatalf("iter %d (seed %d): task %d completion got %v want %v",
					iter, seed, i, got.Results[i].Completion, completion[i])
			}
			var wantStatus string
			switch c := completion[i]; {
			case c != nil && *c <= tk.deadline:
				wantStatus = "met"
			case c != nil:
				wantStatus = "missed"
			case tk.deadline <= horizon:
				wantStatus = "missed"
			default:
				wantStatus = "pending"
			}
			if got.Results[i].DeadlineStatus != wantStatus {
				t.Fatalf("iter %d (seed %d): task %d status got %q want %q",
					iter, seed, i, got.Results[i].DeadlineStatus, wantStatus)
			}
		}
	}
}
