package server

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func postSemaphore(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, semaphoreAnalyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func semaphoreOK(t *testing.T, body string) semResponse {
	t.Helper()
	rec := postSemaphore(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp semResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func semDeclJSON(name string, initial, maximum int64) string {
	return `{"name":` + strconv.Quote(name) + `,"initial":` + strconv.FormatInt(initial, 10) +
		`,"maximum":` + strconv.FormatInt(maximum, 10) + `}`
}

func semReq(horizon int64, semaphores string, tasks ...string) string {
	return `{"horizon":` + strconv.FormatInt(horizon, 10) +
		`,"semaphores":[` + semaphores + `],"tasks":[` + strings.Join(tasks, ",") + `]}`
}

func semTaskJSON(id string, prio, rel, dead int64, actions ...string) string {
	return `{"id":` + strconv.Quote(id) + `,"priority":` + strconv.FormatInt(prio, 10) +
		`,"release":` + strconv.FormatInt(rel, 10) +
		`,"deadline":` + strconv.FormatInt(dead, 10) +
		`,"actions":[` + strings.Join(actions, ",") + `]}`
}

func semRun(n int64) string { return `{"run":` + strconv.FormatInt(n, 10) + `}` }
func semWait(s string) string {
	return `{"wait":` + strconv.Quote(s) + `}`
}
func semPost(s string) string {
	return `{"post":` + strconv.Quote(s) + `}`
}

func checkSemTimeline(t *testing.T, got, want []semTimelineInterval) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("timeline = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v (all %+v)", i, got[i], want[i], got)
		}
	}
}

func TestSemaphoreBasicCounting(t *testing.T) {
	// A takes the only permit, runs, returns it; B then does the same.
	body := semReq(50, semDeclJSON("S", 1, 2),
		semTaskJSON("A", 0, 0, 50, semWait("S"), semRun(3), semPost("S")),
		semTaskJSON("B", 1, 1, 50, semWait("S"), semRun(2), semPost("S")))
	resp := semaphoreOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@5", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "A", Start: 0, End: 3},
		{TaskID: "B", Start: 3, End: 5},
	})
	a, b := resp.Results[0], resp.Results[1]
	if a.State != "completed" || a.Executed != 3 || a.Completion == nil || *a.Completion != 3 ||
		a.BlockedOn != nil || a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "completed" || b.Executed != 2 || b.Completion == nil || *b.Completion != 5 {
		t.Fatalf("B result = %+v", b)
	}
	if resp.FaultAt != nil || resp.FaultTask != nil || resp.FaultSemaphore != nil {
		t.Fatalf("fault fields set on non-overflow: %+v", resp)
	}
}

func TestSemaphoreDirectHandoff(t *testing.T) {
	// A blocks on the empty semaphore at t=0; B's post at t=1 is delivered
	// directly to A without changing the count. B keeps the CPU (higher
	// priority), A runs after B completes.
	body := semReq(20, semDeclJSON("S", 0, 1),
		semTaskJSON("A", 1, 0, 20, semWait("S"), semRun(2)),
		semTaskJSON("B", 0, 1, 20, semPost("S"), semRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 4 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@4", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "B", Start: 1, End: 2},
		{TaskID: "A", Start: 2, End: 4},
	})
	a, b := resp.Results[0], resp.Results[1]
	if a.State != "completed" || a.Executed != 2 || a.Completion == nil || *a.Completion != 4 {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "completed" || b.Executed != 1 || b.Completion == nil || *b.Completion != 2 {
		t.Fatalf("B result = %+v", b)
	}
}

func TestSemaphoreHandoffPreempts(t *testing.T) {
	// A (highest priority) blocks on the empty semaphore; B posts at t=2 and
	// the unblocked A preempts B at that instant.
	body := semReq(20, semDeclJSON("S", 0, 1),
		semTaskJSON("A", 0, 0, 20, semWait("S"), semRun(2)),
		semTaskJSON("B", 1, 1, 20, semRun(1), semPost("S")))
	resp := semaphoreOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 4 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@4", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "B", Start: 1, End: 2},
		{TaskID: "A", Start: 2, End: 4},
	})
}

func TestSemaphoreEarliestWaiterTieBreak(t *testing.T) {
	// A and B both block on S at t=0 (B settles first, higher priority); the
	// same-instant tie is broken by input order, so C's post wakes A. B stays
	// blocked forever: no future releases, so the simulation stalls.
	body := semReq(50, semDeclJSON("S", 0, 1),
		semTaskJSON("A", 2, 0, 50, semWait("S"), semRun(2)),
		semTaskJSON("B", 1, 0, 50, semWait("S"), semRun(1)),
		semTaskJSON("C", 0, 1, 50, semPost("S")))
	resp := semaphoreOK(t, body)
	if resp.Status != "stalled" || resp.StoppedAt != 3 {
		t.Fatalf("status=%q stoppedAt=%d, want stalled@3", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "A", Start: 1, End: 3},
	})
	a, b, c := resp.Results[0], resp.Results[1], resp.Results[2]
	if a.State != "completed" || a.Completion == nil || *a.Completion != 3 {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "blocked" || b.BlockedOn == nil || *b.BlockedOn != "S" || b.Executed != 0 {
		t.Fatalf("B result = %+v", b)
	}
	if c.State != "completed" || c.Completion == nil || *c.Completion != 1 {
		t.Fatalf("C result = %+v", c)
	}
}

func TestSemaphoreOverflow(t *testing.T) {
	// The count is already at maximum with no waiters: A's post is an
	// overflow fault and the analysis stops on the spot.
	body := semReq(10, semDeclJSON("S", 1, 1),
		semTaskJSON("A", 0, 0, 50, semPost("S")))
	resp := semaphoreOK(t, body)
	if resp.Status != "overflow" || resp.StoppedAt != 0 {
		t.Fatalf("status=%q stoppedAt=%d, want overflow@0", resp.Status, resp.StoppedAt)
	}
	if resp.FaultAt == nil || *resp.FaultAt != 0 {
		t.Fatalf("faultAt = %v, want 0", resp.FaultAt)
	}
	if resp.FaultTask == nil || *resp.FaultTask != "A" {
		t.Fatalf("faultTask = %v, want A", resp.FaultTask)
	}
	if resp.FaultSemaphore == nil || *resp.FaultSemaphore != "S" {
		t.Fatalf("faultSemaphore = %v, want S", resp.FaultSemaphore)
	}
	if len(resp.Timeline) != 0 {
		t.Fatalf("timeline = %+v, want empty", resp.Timeline)
	}
	a := resp.Results[0]
	if a.State != "ready" || a.Executed != 0 || a.Completion != nil ||
		a.BlockedOn != nil || a.DeadlineStatus != "pending" {
		t.Fatalf("A result = %+v", a)
	}
}

func TestSemaphoreOverflowMidRun(t *testing.T) {
	// A runs first; its second post overflows at t=3. B never runs.
	body := semReq(50, semDeclJSON("S", 0, 1),
		semTaskJSON("A", 0, 0, 50, semRun(3), semPost("S"), semPost("S")),
		semTaskJSON("B", 1, 1, 50, semRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "overflow" || resp.StoppedAt != 3 {
		t.Fatalf("status=%q stoppedAt=%d, want overflow@3", resp.Status, resp.StoppedAt)
	}
	if resp.FaultAt == nil || *resp.FaultAt != 3 || resp.FaultTask == nil || *resp.FaultTask != "A" {
		t.Fatalf("fault = %+v, want A@3", resp)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "A", Start: 0, End: 3},
	})
	if b := resp.Results[1]; b.State != "ready" || b.Executed != 0 {
		t.Fatalf("B result = %+v", b)
	}
}

func TestSemaphoreStalled(t *testing.T) {
	// A blocks on an empty semaphore nobody ever posts to.
	body := semReq(50, semDeclJSON("S", 0, 1),
		semTaskJSON("A", 0, 0, 50, semWait("S"), semRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "stalled" || resp.StoppedAt != 0 {
		t.Fatalf("status=%q stoppedAt=%d, want stalled@0", resp.Status, resp.StoppedAt)
	}
	a := resp.Results[0]
	if a.State != "blocked" || a.BlockedOn == nil || *a.BlockedOn != "S" ||
		a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
}

func TestSemaphoreHorizon(t *testing.T) {
	// A is still running when the horizon cuts the schedule.
	body := semReq(5, semDeclJSON("S", 1, 1),
		semTaskJSON("A", 0, 0, 50, semWait("S"), semRun(10), semPost("S")))
	resp := semaphoreOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@5", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "A", Start: 0, End: 5},
	})
	a := resp.Results[0]
	if a.State != "ready" || a.Executed != 5 || a.Completion != nil || a.DeadlineStatus != "pending" {
		t.Fatalf("A result = %+v", a)
	}
}

func TestSemaphoreHorizonExactEndDrainsZeroActions(t *testing.T) {
	// The run ends exactly at the horizon: only the current task's trailing
	// zero-duration actions are processed; B never runs.
	body := semReq(5, semDeclJSON("S", 0, 2),
		semTaskJSON("A", 0, 0, 50, semRun(5), semPost("S"), semPost("S")),
		semTaskJSON("B", 1, 0, 50, semWait("S"), semRun(1)))
	resp := semaphoreOK(t, body)
	// A completes exactly at the horizon; B never runs, so not all tasks are
	// completed and the status stays "horizon".
	if resp.Status != "horizon" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@5", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "A", Start: 0, End: 5},
	})
	a, b := resp.Results[0], resp.Results[1]
	if a.State != "completed" || a.Completion == nil || *a.Completion != 5 {
		t.Fatalf("A result = %+v", a)
	}
	// B's wait was satisfiable only after the horizon, so it never executes.
	if b.State != "ready" || b.Executed != 0 || b.BlockedOn != nil {
		t.Fatalf("B result = %+v", b)
	}
}

func TestSemaphoreOverflowAtHorizonBoundary(t *testing.T) {
	// The run ends exactly at the horizon and the drained post overflows.
	body := semReq(5, semDeclJSON("S", 2, 2),
		semTaskJSON("A", 0, 0, 50, semRun(5), semPost("S")))
	resp := semaphoreOK(t, body)
	if resp.Status != "overflow" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want overflow@5", resp.Status, resp.StoppedAt)
	}
	if resp.FaultAt == nil || *resp.FaultAt != 5 {
		t.Fatalf("faultAt = %v, want 5", resp.FaultAt)
	}
}

func TestSemaphoreHandoffLeavesCountForNextWaiter(t *testing.T) {
	// S starts at 1: A waits (count 1->0), B waits (blocks), C posts (direct
	// handoff to B, count stays 0), D waits and must block: the handoff did
	// not create a spare permit.
	body := semReq(50, semDeclJSON("S", 1, 1),
		semTaskJSON("A", 0, 0, 50, semWait("S"), semRun(1)),
		semTaskJSON("B", 1, 0, 50, semWait("S"), semRun(1)),
		semTaskJSON("C", 2, 0, 50, semPost("S")),
		semTaskJSON("D", 3, 0, 50, semWait("S"), semRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "stalled" {
		t.Fatalf("status=%q, want stalled", resp.Status)
	}
	d := resp.Results[3]
	if d.State != "blocked" || d.BlockedOn == nil || *d.BlockedOn != "S" {
		t.Fatalf("D result = %+v", d)
	}
}

func TestSemaphoreValidation(t *testing.T) {
	okTask := semTaskJSON("A", 0, 0, 50, semWait("S"), semRun(1), semPost("S"))
	cases := []struct {
		name string
		body string
	}{
		{"no semaphores", `{"horizon":10,"tasks":[` + okTask + `]}`},
		{"empty semaphores", semReq(10, "", okTask)},
		{"duplicate semaphore", semReq(10, semDeclJSON("S", 0, 1)+","+semDeclJSON("S", 0, 1), okTask)},
		{"semaphore name with space", semReq(10, semDeclJSON("S S", 0, 1), okTask)},
		{"empty semaphore name", semReq(10, semDeclJSON("", 0, 1), okTask)},
		{"initial above maximum", semReq(10, semDeclJSON("S", 2, 1), okTask)},
		{"negative initial", semReq(10, semDeclJSON("S", -1, 1), okTask)},
		{"zero maximum", semReq(10, semDeclJSON("S", 0, 0), okTask)},
		{"maximum too large", semReq(10, semDeclJSON("S", 0, 65536), okTask)},
		{"missing initial", `{"horizon":10,"semaphores":[{"name":"S","maximum":1}],"tasks":[` + okTask + `]}`},
		{"missing maximum", `{"horizon":10,"semaphores":[{"name":"S","initial":0}],"tasks":[` + okTask + `]}`},
		{"wait undeclared", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 50, semWait("X")))},
		{"post undeclared", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 50, semPost("X")))},
		{"two verbs", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 50, `{"run":1,"wait":"S"}`))},
		{"no verbs", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 50, `{}`))},
		{"zero run", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 50, semRun(0)))},
		{"negative run", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 50, semRun(-1)))},
		{"run total overflow", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 50, semRun(math.MaxInt64), semRun(1)))},
		{"global run overflow", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 50, semRun(math.MaxInt64)),
			semTaskJSON("B", 0, 0, 50, semRun(1)))},
		{"empty actions", semReq(10, semDeclJSON("S", 0, 1),
			`{"id":"A","priority":0,"release":0,"deadline":50,"actions":[]}`)},
		{"duplicate task id", semReq(10, semDeclJSON("S", 0, 1), okTask, okTask)},
		{"bad priority", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 256, 0, 50, semRun(1)))},
		{"release at horizon", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 10, 50, semRun(1)))},
		{"deadline at release", semReq(10, semDeclJSON("S", 0, 1),
			semTaskJSON("A", 0, 0, 0, semRun(1)))},
		{"zero horizon", semReq(0, semDeclJSON("S", 0, 1), okTask)},
		{"horizon too large", semReq(1_000_000_000_001, semDeclJSON("S", 0, 1), okTask)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := postSemaphore(t, tc.body, "application/json")
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422; body: %s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"validation_failed"`) {
				t.Fatalf("body = %s, want validation_failed", rec.Body.String())
			}
		})
	}
}

func TestSemaphoreErrorSemantics(t *testing.T) {
	body := semReq(10, semDeclJSON("S", 0, 1),
		semTaskJSON("A", 0, 0, 50, semRun(1)))

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, semaphoreAnalyzePath, nil)
	Handler().ServeHTTP(rec, r)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET: code=%d allow=%q, want 405 POST", rec.Code, rec.Header().Get("Allow"))
	}

	if rec := postSemaphore(t, body, "text/plain"); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain: code=%d, want 415", rec.Code)
	}
	if rec := postSemaphore(t, body, ""); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("missing content type: code=%d, want 415", rec.Code)
	}
	if rec := postSemaphore(t, `{"horizon":`, "application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad syntax: code=%d, want 400", rec.Code)
	}
	if rec := postSemaphore(t, body+` {}`, "application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("trailing content: code=%d, want 400", rec.Code)
	}
	if rec := postSemaphore(t, `{"horizon":10,"horizon":10,"semaphores":[],"tasks":[]}`,
		"application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("duplicate key: code=%d, want 400", rec.Code)
	}
	if rec := postSemaphore(t, strings.Replace(body, `"horizon"`, `"horizonX"`, 1),
		"application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: code=%d, want 400", rec.Code)
	}
	wrongType := `{"horizon":10,"semaphores":"x","tasks":[` + semTaskJSON("A", 0, 0, 50, semRun(1)) + `]}`
	if rec := postSemaphore(t, wrongType, "application/json"); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("wrong type: code=%d, want 422", rec.Code)
	}

	// Oversized body is rejected as 400.
	big := strings.Repeat(" ", 1<<20) + body
	if rec := postSemaphore(t, big, "application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized: code=%d, want 400", rec.Code)
	}
}

func TestSemaphoreDeterministicAndStateless(t *testing.T) {
	body := semReq(20, semDeclJSON("S", 0, 1),
		semTaskJSON("A", 1, 0, 20, semWait("S"), semRun(2)),
		semTaskJSON("B", 0, 1, 20, semPost("S"), semRun(1)))
	first := postSemaphore(t, body, "application/json")
	second := postSemaphore(t, body, "application/json")
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("codes = %d, %d, want 200", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("repeated request differs:\n%s\n%s", first.Body.String(), second.Body.String())
	}
}
