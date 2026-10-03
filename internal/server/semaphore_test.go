package server

import (
	"bytes"
	"encoding/json"
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

func semDecl(name string, initial, maximum int64) string {
	return `{"name":` + strconv.Quote(name) +
		`,"initial":` + strconv.FormatInt(initial, 10) +
		`,"maximum":` + strconv.FormatInt(maximum, 10) + `}`
}

func semReq(horizon int64, semaphores string, tasks ...string) string {
	return `{"horizon":` + strconv.FormatInt(horizon, 10) +
		`,"semaphores":[` + semaphores + `],"tasks":[` + strings.Join(tasks, ",") + `]}`
}

func semWait(s string) string { return `{"wait":` + strconv.Quote(s) + `}` }
func semPost(s string) string { return `{"post":` + strconv.Quote(s) + `}` }

func checkSemTimeline(t *testing.T, got []semTimelineInterval, want []semTimelineInterval) {
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

func TestSemaphoreDirectDeliveryPreemption(t *testing.T) {
	// B blocks on the empty semaphore at t=0; A's post at t=1 is delivered
	// directly to B (the count stays 0) and the unblocked higher-priority B
	// preempts A at that instant.
	body := semReq(20, semDecl("S", 0, 2),
		mbTask("B", 0, 0, 20, semWait("S"), mbRun(2)),
		mbTask("A", 1, 1, 20, semPost("S"), mbRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 4 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@4", resp.Status, resp.StoppedAt)
	}
	if resp.FaultAt != nil || resp.FaultTask != nil || resp.FaultSemaphore != nil {
		t.Fatalf("fault fields set on non-overflow: %+v", resp)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "B", Start: 1, End: 3},
		{TaskID: "A", Start: 3, End: 4},
	})
	b, a := resp.Results[0], resp.Results[1]
	if b.State != "completed" || b.Executed != 2 || b.Completion == nil || *b.Completion != 3 ||
		b.BlockedOn != nil || b.DeadlineStatus != "met" {
		t.Fatalf("B result = %+v", b)
	}
	if a.State != "completed" || a.Executed != 1 || a.Completion == nil || *a.Completion != 4 ||
		a.BlockedOn != nil || a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v", a)
	}
}

func TestSemaphoreCounting(t *testing.T) {
	// Posts with no waiters increment the count; waits on a non-zero count
	// decrement it without blocking.
	body := semReq(10, semDecl("S", 1, 3),
		mbTask("A", 0, 0, 10, semPost("S"), semPost("S"), semWait("S"), semWait("S"), mbRun(2)))
	resp := semaphoreOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 2 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@2", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{{TaskID: "A", Start: 0, End: 2}})
	a := resp.Results[0]
	if a.State != "completed" || a.Executed != 2 || a.Completion == nil || *a.Completion != 2 {
		t.Fatalf("A result = %+v", a)
	}
}

func TestSemaphoreOverflow(t *testing.T) {
	// Post at the maximum with no waiter stops the analysis on the spot.
	body := semReq(100, semDecl("S", 1, 1),
		mbTask("A", 0, 0, 200, mbRun(5), semPost("S"), mbRun(5)),
		mbTask("B", 1, 10, 200, mbRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "overflow" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want overflow@5", resp.Status, resp.StoppedAt)
	}
	if resp.FaultAt == nil || *resp.FaultAt != 5 {
		t.Fatalf("faultAt = %v, want 5", resp.FaultAt)
	}
	if resp.FaultTask == nil || *resp.FaultTask != "A" {
		t.Fatalf("faultTask = %v, want A", resp.FaultTask)
	}
	if resp.FaultSemaphore == nil || *resp.FaultSemaphore != "S" {
		t.Fatalf("faultSemaphore = %v, want S", resp.FaultSemaphore)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{{TaskID: "A", Start: 0, End: 5}})
	a, b := resp.Results[0], resp.Results[1]
	if a.State != "ready" || a.Executed != 5 || a.Completion != nil ||
		a.BlockedOn != nil || a.DeadlineStatus != "pending" {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "unreleased" || b.Executed != 0 || b.Completion != nil ||
		b.BlockedOn != nil || b.DeadlineStatus != "pending" {
		t.Fatalf("B result = %+v", b)
	}
}

func TestSemaphoreOverflowAtHorizonBoundary(t *testing.T) {
	// A run ending exactly at the horizon still drains the task's trailing
	// zero-duration actions; an overflowing post there faults at the horizon.
	body := semReq(10, semDecl("S", 1, 1),
		mbTask("A", 0, 0, 50, mbRun(10), semPost("S")))
	resp := semaphoreOK(t, body)
	if resp.Status != "overflow" || resp.StoppedAt != 10 {
		t.Fatalf("status=%q stoppedAt=%d, want overflow@10", resp.Status, resp.StoppedAt)
	}
	if resp.FaultAt == nil || *resp.FaultAt != 10 {
		t.Fatalf("faultAt = %v, want 10", resp.FaultAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{{TaskID: "A", Start: 0, End: 10}})
}

func TestSemaphoreStalled(t *testing.T) {
	// Both tasks block on an empty semaphore and no release is pending.
	body := semReq(50, semDecl("S", 0, 1),
		mbTask("A", 0, 0, 50, semWait("S"), mbRun(1)),
		mbTask("B", 1, 2, 50, semWait("S"), mbRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "stalled" || resp.StoppedAt != 2 {
		t.Fatalf("status=%q stoppedAt=%d, want stalled@2", resp.Status, resp.StoppedAt)
	}
	if len(resp.Timeline) != 0 {
		t.Fatalf("timeline = %+v, want empty", resp.Timeline)
	}
	for i, r := range resp.Results {
		if r.State != "blocked" || r.BlockedOn == nil || *r.BlockedOn != "S" ||
			r.Executed != 0 || r.Completion != nil || r.DeadlineStatus != "missed" {
			t.Fatalf("results[%d] = %+v", i, r)
		}
	}
}

func TestSemaphoreHorizon(t *testing.T) {
	// A long run is cut by the horizon.
	body := semReq(50, semDecl("S", 1, 1),
		mbTask("A", 0, 0, 50, semWait("S"), mbRun(100)))
	resp := semaphoreOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 50 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@50", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{{TaskID: "A", Start: 0, End: 50}})
	a := resp.Results[0]
	if a.State != "ready" || a.Executed != 50 || a.Completion != nil ||
		a.BlockedOn != nil || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
}

func TestSemaphoreRunEndsAtHorizon(t *testing.T) {
	// The run ends exactly at the horizon: only the current task's trailing
	// zero-duration actions are processed; B never runs and never blocks.
	body := semReq(10, semDecl("S", 0, 1),
		mbTask("A", 0, 0, 10, mbRun(10), semPost("S")),
		mbTask("B", 1, 0, 10, semWait("S"), mbRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 10 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@10", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{{TaskID: "A", Start: 0, End: 10}})
	a, b := resp.Results[0], resp.Results[1]
	if a.State != "completed" || a.Executed != 10 || a.Completion == nil || *a.Completion != 10 ||
		a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "ready" || b.Executed != 0 || b.Completion != nil ||
		b.BlockedOn != nil || b.DeadlineStatus != "missed" {
		t.Fatalf("B result = %+v", b)
	}
}

func TestSemaphoreWaiterTieByInputOrder(t *testing.T) {
	// B and C block at the same instant; the post wakes the one earliest in
	// input order (B) first.
	body := semReq(20, semDecl("S", 0, 1),
		mbTask("C", 2, 0, 20, semWait("S"), mbRun(1)),
		mbTask("B", 1, 0, 20, semWait("S"), mbRun(1)),
		mbTask("A", 0, 1, 20, semPost("S"), semPost("S"), mbRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 4 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@4", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "A", Start: 1, End: 2},
		{TaskID: "B", Start: 2, End: 3},
		{TaskID: "C", Start: 3, End: 4},
	})
	c, b, a := resp.Results[0], resp.Results[1], resp.Results[2]
	if a.Completion == nil || *a.Completion != 2 {
		t.Fatalf("A result = %+v", a)
	}
	if b.Completion == nil || *b.Completion != 3 {
		t.Fatalf("B result = %+v", b)
	}
	if c.Completion == nil || *c.Completion != 4 {
		t.Fatalf("C result = %+v", c)
	}
}

func TestSemaphoreSamePriorityNoPreemption(t *testing.T) {
	// The post unblocks an equal-priority waiter; the running task keeps the
	// processor, and its adjacent run intervals merge in the timeline.
	body := semReq(20, semDecl("S", 0, 1),
		mbTask("A", 1, 0, 20, mbRun(2), semPost("S"), mbRun(2)),
		mbTask("B", 1, 0, 20, semWait("S"), mbRun(1)))
	resp := semaphoreOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@5", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{
		{TaskID: "A", Start: 0, End: 4},
		{TaskID: "B", Start: 4, End: 5},
	})
	a, b := resp.Results[0], resp.Results[1]
	if a.Executed != 4 || a.Completion == nil || *a.Completion != 4 {
		t.Fatalf("A result = %+v", a)
	}
	if b.Executed != 1 || b.Completion == nil || *b.Completion != 5 {
		t.Fatalf("B result = %+v", b)
	}
}

func TestSemaphoreIdleJump(t *testing.T) {
	// The processor idles until the next release; idle time is omitted.
	body := semReq(20, semDecl("S", 1, 1),
		mbTask("A", 0, 5, 20, semWait("S"), mbRun(2)))
	resp := semaphoreOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 7 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@7", resp.Status, resp.StoppedAt)
	}
	checkSemTimeline(t, resp.Timeline, []semTimelineInterval{{TaskID: "A", Start: 5, End: 7}})
}

func TestSemaphoreDeterministic(t *testing.T) {
	body := semReq(30, semDecl("S", 1, 2)+","+semDecl("T", 0, 1),
		mbTask("A", 2, 0, 30, semWait("S"), mbRun(3), semPost("T")),
		mbTask("B", 1, 2, 30, semPost("S"), mbRun(2), semWait("T")),
		mbTask("C", 0, 5, 30, semWait("T"), mbRun(1), semPost("S")))
	first := semaphoreOK(t, body)
	second := semaphoreOK(t, body)
	fj, _ := json.Marshal(first)
	sj, _ := json.Marshal(second)
	if !bytes.Equal(fj, sj) {
		t.Fatalf("non-deterministic response:\n%s\n%s", fj, sj)
	}
}

func TestSemaphoreValidationFailures(t *testing.T) {
	good := mbTask("a", 0, 0, 20, semWait("S"), semPost("S"))
	base := func(inner string) string { return semReq(20, semDecl("S", 1, 2), inner) }
	cases := map[string]string{
		"missing horizon":       `{"semaphores":[` + semDecl("S", 0, 1) + `],"tasks":[` + good + `]}`,
		"zero horizon":          semReq(0, semDecl("S", 0, 1), good),
		"horizon too large":     semReq(1000000000001, semDecl("S", 0, 1), good),
		"missing semaphores":    `{"horizon":20,"tasks":[` + good + `]}`,
		"empty semaphores":      semReq(20, "", good),
		"semaphore empty name":  semReq(20, semDecl("", 0, 1), good),
		"semaphore whitespace":  semReq(20, semDecl("a b", 0, 1), good),
		"semaphore dup name":    semReq(20, semDecl("S", 0, 1)+","+semDecl("S", 1, 2), good),
		"initial missing":       `{"horizon":20,"semaphores":[{"name":"S","maximum":1}],"tasks":[` + good + `]}`,
		"maximum missing":       `{"horizon":20,"semaphores":[{"name":"S","initial":0}],"tasks":[` + good + `]}`,
		"initial negative":      semReq(20, semDecl("S", -1, 1), good),
		"initial above maximum": semReq(20, semDecl("S", 2, 1), good),
		"maximum zero":          semReq(20, semDecl("S", 0, 0), good),
		"maximum negative":      semReq(20, semDecl("S", 0, -1), good),
		"maximum too large":     semReq(20, semDecl("S", 0, 65536), good),
		"missing tasks":         `{"horizon":20,"semaphores":[` + semDecl("S", 0, 1) + `]}`,
		"empty tasks":           semReq(20, semDecl("S", 0, 1)),
		"duplicate id":          base(good + "," + mbTask("a", 1, 0, 20, mbRun(1))),
		"priority negative":     base(mbTask("a", -1, 0, 20, mbRun(1))),
		"priority too large":    base(mbTask("a", 256, 0, 20, mbRun(1))),
		"release at horizon":    base(mbTask("a", 0, 20, 21, mbRun(1))),
		"deadline at release":   base(mbTask("a", 0, 0, 0, mbRun(1))),
		"missing actions":       base(`{"id":"a","priority":0,"release":0,"deadline":1}`),
		"empty actions":         base(mbTask("a", 0, 0, 20)),
		"no verb":               base(mbTask("a", 0, 0, 20, `{}`)),
		"two verbs":             base(mbTask("a", 0, 0, 20, `{"run":1,"wait":"S"}`)),
		"wait and post":         base(mbTask("a", 0, 0, 20, `{"wait":"S","post":"S"}`)),
		"run zero":              base(mbTask("a", 0, 0, 20, mbRun(0))),
		"run negative":          base(mbTask("a", 0, 0, 20, mbRun(-1))),
		"run null":              base(mbTask("a", 0, 0, 20, `{"run":null}`)),
		"run wrong type":        base(mbTask("a", 0, 0, 20, `{"run":"1"}`)),
		"wait null":             base(mbTask("a", 0, 0, 20, `{"wait":null}`)),
		"wait wrong type":       base(mbTask("a", 0, 0, 20, `{"wait":1}`)),
		"wait undeclared":       base(mbTask("a", 0, 0, 20, semWait("Q"))),
		"post null":             base(mbTask("a", 0, 0, 20, `{"post":null}`)),
		"post wrong type":       base(mbTask("a", 0, 0, 20, `{"post":1}`)),
		"post undeclared":       base(mbTask("a", 0, 0, 20, semPost("Q"))),
		"run total overflow":    base(mbTask("a", 0, 0, 20, mbRun(maxInt64), mbRun(1))),
		"global run overflow":   base(mbTask("a", 0, 0, 20, mbRun(maxInt64)) + "," + mbTask("b", 0, 0, 20, mbRun(1))),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postSemaphore(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	bad400 := map[string]string{
		"unknown top key":       `{"horizon":20,"bogus":1,"semaphores":[],"tasks":[]}`,
		"unknown task key":      base(`{"id":"a","priority":0,"release":0,"deadline":1,"execution":1,"actions":[{"run":1}]}`),
		"unknown action":        base(mbTask("a", 0, 0, 20, `{"signal":"S"}`)),
		"unknown semaphore key": `{"horizon":20,"semaphores":[{"name":"S","initial":0,"maximum":1,"extra":1}],"tasks":[` + good + `]}`,
		"duplicate key":         `{"horizon":3,"horizon":4,"semaphores":[],"tasks":[]}`,
		"trailing token":        base(good) + ` garbage`,
		"syntax error":          `{`,
	}
	for name, body := range bad400 {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postSemaphore(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}
}

func TestSemaphoreBoundaryValuesAccepted(t *testing.T) {
	// Maximum 65535, initial equal to maximum and 1024 actions are in range.
	acts := make([]string, 0, 1024)
	for i := 0; i < 1023; i++ {
		acts = append(acts, semWait("S"))
	}
	acts = append(acts, mbRun(1))
	posts := make([]string, 0, 1024)
	for i := 0; i < 1023; i++ {
		posts = append(posts, semPost("S"))
	}
	posts = append(posts, mbRun(1))
	body := semReq(5, semDecl("S", 65535, 65535),
		mbTask("a", 0, 0, 5, acts...),
		mbTask("b", 1, 0, 5, posts...))
	if rec := postSemaphore(t, body, "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("boundary values rejected: %d %s", rec.Code, rec.Body.String())
	}

	// 1025 actions and 257 semaphores are out of range.
	acts = append(acts, mbRun(1))
	assertErrorCode(t, postSemaphore(t, semReq(5, semDecl("S", 0, 1), mbTask("a", 0, 0, 5, acts...)), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
	var decls []string
	for i := 0; i < 257; i++ {
		decls = append(decls, semDecl("s"+strconv.Itoa(i), 0, 1))
	}
	assertErrorCode(t, postSemaphore(t, semReq(5, strings.Join(decls, ","), mbTask("a", 0, 0, 5, mbRun(1))), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
}

func TestSemaphoreMethodAndMediaType(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, semaphoreAnalyzePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
	good := semReq(3, semDecl("S", 1, 1), mbTask("a", 0, 0, 3, mbRun(1)))
	assertErrorCode(t, postSemaphore(t, good, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postSemaphore(t, good, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	if rec := postSemaphore(t, good, "application/json; charset=utf-8"); rec.Code != http.StatusOK {
		t.Fatalf("charset parameter rejected: %d %s", rec.Code, rec.Body.String())
	}
}
