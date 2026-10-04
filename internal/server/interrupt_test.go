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

func postInterrupt(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, interruptAnalyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func interruptOK(t *testing.T, body string) interruptResponse {
	t.Helper()
	rec := postInterrupt(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp interruptResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func intReq(horizon int64, tasks, interrupts string) string {
	return `{"horizon":` + strconv.FormatInt(horizon, 10) +
		`,"tasks":[` + tasks + `],"interrupts":[` + interrupts + `]}`
}

func intTaskJSON(id string, prio, rel, dead int64, actions ...string) string {
	return `{"id":` + strconv.Quote(id) + `,"priority":` + strconv.FormatInt(prio, 10) +
		`,"release":` + strconv.FormatInt(rel, 10) +
		`,"deadline":` + strconv.FormatInt(dead, 10) +
		`,"actions":[` + strings.Join(actions, ",") + `]}`
}

func intIntrJSON(id string, prio, arr, exec, dead int64) string {
	return `{"id":` + strconv.Quote(id) + `,"priority":` + strconv.FormatInt(prio, 10) +
		`,"arrival":` + strconv.FormatInt(arr, 10) +
		`,"execution":` + strconv.FormatInt(exec, 10) +
		`,"deadline":` + strconv.FormatInt(dead, 10) + `}`
}

func intRun(n int64) string  { return `{"run":` + strconv.FormatInt(n, 10) + `}` }
func intCrit(n int64) string { return `{"critical":` + strconv.FormatInt(n, 10) + `}` }

func checkIntTimeline(t *testing.T, got, want []intTimelineInterval) {
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

func TestInterruptPreemptsTaskRun(t *testing.T) {
	// Any interrupt preempts a task's run action; the task resumes after.
	body := intReq(100,
		intTaskJSON("A", 1, 0, 100, intRun(10)),
		intIntrJSON("I1", 0, 3, 2, 50))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 12 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@12", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []intTimelineInterval{
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 0, End: 3},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 3, End: 5},
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 5, End: 12},
	})
	a := resp.TaskResults[0]
	if a.ID != "A" || a.Executed != 10 || a.Completion == nil || *a.Completion != 12 ||
		a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v", a)
	}
	i1 := resp.InterruptResults[0]
	if i1.ID != "I1" || i1.Executed != 2 || i1.Start == nil || *i1.Start != 3 ||
		i1.Completion == nil || *i1.Completion != 5 || i1.Latency == nil || *i1.Latency != 0 ||
		i1.DeadlineStatus != "met" {
		t.Fatalf("I1 result = %+v", i1)
	}
	if len(resp.CriticalSections) != 0 {
		t.Fatalf("criticalSections = %+v, want empty", resp.CriticalSections)
	}
}

func TestInterruptCriticalDefersInterrupt(t *testing.T) {
	// The critical section runs to its end even though I1 arrives mid-way.
	body := intReq(100,
		intTaskJSON("A", 1, 0, 100, intRun(2), intCrit(5), intRun(1)),
		intIntrJSON("I1", 0, 3, 2, 50))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 10 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@10", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []intTimelineInterval{
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 0, End: 2},
		{ActorType: "task", ActorID: "A", Mode: "critical", Start: 2, End: 7},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 7, End: 9},
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 9, End: 10},
	})
	a := resp.TaskResults[0]
	if a.Executed != 8 || a.Completion == nil || *a.Completion != 10 || a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v", a)
	}
	i1 := resp.InterruptResults[0]
	if i1.Start == nil || *i1.Start != 7 || i1.Latency == nil || *i1.Latency != 4 ||
		i1.Completion == nil || *i1.Completion != 9 {
		t.Fatalf("I1 result = %+v", i1)
	}
	if len(resp.CriticalSections) != 1 {
		t.Fatalf("criticalSections = %+v", resp.CriticalSections)
	}
	cs := resp.CriticalSections[0]
	if cs.TaskID != "A" || cs.ActionIndex != 1 || cs.Start == nil || *cs.Start != 2 ||
		cs.End == nil || *cs.End != 7 || cs.ObservedDuration != 5 || !cs.Completed {
		t.Fatalf("critical section = %+v", cs)
	}
}

func TestInterruptPriorityPreemption(t *testing.T) {
	// I2 (prio 1) preempts I1 (prio 2); I3 (prio 1) does not preempt I2; the
	// suspended I1 resumes once no higher-priority interrupt remains.
	body := intReq(100,
		intTaskJSON("A", 0, 0, 100, intRun(1)),
		strings.Join([]string{
			intIntrJSON("I1", 2, 0, 5, 100),
			intIntrJSON("I2", 1, 2, 2, 100),
			intIntrJSON("I3", 1, 3, 1, 100),
		}, ","))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 9 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@9", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []intTimelineInterval{
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 0, End: 2},
		{ActorType: "interrupt", ActorID: "I2", Mode: "run", Start: 2, End: 4},
		{ActorType: "interrupt", ActorID: "I3", Mode: "run", Start: 4, End: 5},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 5, End: 8},
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 8, End: 9},
	})
	i1, i2, i3 := resp.InterruptResults[0], resp.InterruptResults[1], resp.InterruptResults[2]
	if i1.Executed != 5 || i1.Completion == nil || *i1.Completion != 8 ||
		i1.Start == nil || *i1.Start != 0 || i1.Latency == nil || *i1.Latency != 0 {
		t.Fatalf("I1 result = %+v", i1)
	}
	if i2.Executed != 2 || i2.Completion == nil || *i2.Completion != 4 {
		t.Fatalf("I2 result = %+v", i2)
	}
	if i3.Executed != 1 || i3.Completion == nil || *i3.Completion != 5 ||
		i3.Start == nil || *i3.Start != 4 || i3.Latency == nil || *i3.Latency != 1 {
		t.Fatalf("I3 result = %+v", i3)
	}
}

func TestInterruptSameInstantTies(t *testing.T) {
	// Same arrival or release: input order decides; idle time is omitted.
	body := intReq(100,
		strings.Join([]string{
			intTaskJSON("T1", 1, 0, 100, intRun(1)),
			intTaskJSON("T2", 1, 0, 100, intRun(1)),
		}, ","),
		strings.Join([]string{
			intIntrJSON("I1", 1, 5, 1, 100),
			intIntrJSON("I2", 1, 5, 1, 100),
		}, ","))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 7 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@7", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []intTimelineInterval{
		{ActorType: "task", ActorID: "T1", Mode: "run", Start: 0, End: 1},
		{ActorType: "task", ActorID: "T2", Mode: "run", Start: 1, End: 2},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 5, End: 6},
		{ActorType: "interrupt", ActorID: "I2", Mode: "run", Start: 6, End: 7},
	})
}

func TestInterruptHorizonCutsCritical(t *testing.T) {
	// The critical section is truncated by the horizon; the interrupt never
	// starts, so start/completion/latency are all null.
	body := intReq(5,
		intTaskJSON("A", 0, 0, 1000, intCrit(10)),
		intIntrJSON("I1", 0, 1, 1, 4))
	resp := interruptOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@5", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []intTimelineInterval{
		{ActorType: "task", ActorID: "A", Mode: "critical", Start: 0, End: 5},
	})
	a := resp.TaskResults[0]
	if a.Executed != 5 || a.Completion != nil || a.DeadlineStatus != "pending" {
		t.Fatalf("A result = %+v", a)
	}
	i1 := resp.InterruptResults[0]
	if i1.Executed != 0 || i1.Start != nil || i1.Completion != nil || i1.Latency != nil ||
		i1.DeadlineStatus != "missed" {
		t.Fatalf("I1 result = %+v", i1)
	}
	if len(resp.CriticalSections) != 1 {
		t.Fatalf("criticalSections = %+v", resp.CriticalSections)
	}
	cs := resp.CriticalSections[0]
	if cs.TaskID != "A" || cs.ActionIndex != 0 || cs.Start == nil || *cs.Start != 0 ||
		cs.End != nil || cs.ObservedDuration != 5 || cs.Completed {
		t.Fatalf("critical section = %+v", cs)
	}
}

func TestInterruptUnstartedCriticalSection(t *testing.T) {
	// The task never reaches its critical action before the horizon.
	body := intReq(4,
		intTaskJSON("A", 0, 0, 1000, intRun(6), intCrit(3)),
		intIntrJSON("I1", 0, 0, 1, 1000))
	resp := interruptOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 4 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@4", resp.Status, resp.StoppedAt)
	}
	if len(resp.CriticalSections) != 1 {
		t.Fatalf("criticalSections = %+v", resp.CriticalSections)
	}
	cs := resp.CriticalSections[0]
	if cs.TaskID != "A" || cs.ActionIndex != 1 || cs.Start != nil || cs.End != nil ||
		cs.ObservedDuration != 0 || cs.Completed {
		t.Fatalf("critical section = %+v", cs)
	}
}

func TestInterruptDeadlineStatuses(t *testing.T) {
	// A completes after its deadline (missed); I1 meets its deadline.
	body := intReq(100,
		intTaskJSON("A", 0, 0, 3, intRun(5)),
		intIntrJSON("I1", 0, 0, 1, 2))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 6 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@6", resp.Status, resp.StoppedAt)
	}
	a := resp.TaskResults[0]
	if a.Completion == nil || *a.Completion != 6 || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
	i1 := resp.InterruptResults[0]
	if i1.Completion == nil || *i1.Completion != 1 || i1.DeadlineStatus != "met" {
		t.Fatalf("I1 result = %+v", i1)
	}
}

func TestInterruptAtCriticalBoundary(t *testing.T) {
	// An interrupt arriving exactly when the preceding run action ends (and
	// the critical action would begin) runs first: the critical section only
	// locks the CPU once it actually starts.
	body := intReq(100,
		intTaskJSON("A", 0, 0, 100, intRun(2), intCrit(3)),
		intIntrJSON("I1", 0, 2, 1, 100))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 6 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@6", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []intTimelineInterval{
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 0, End: 2},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 2, End: 3},
		{ActorType: "task", ActorID: "A", Mode: "critical", Start: 3, End: 6},
	})
	cs := resp.CriticalSections[0]
	if cs.Start == nil || *cs.Start != 3 || cs.End == nil || *cs.End != 6 ||
		cs.ObservedDuration != 3 || !cs.Completed {
		t.Fatalf("critical section = %+v", cs)
	}
}

func TestInterruptArrivesAtCriticalStart(t *testing.T) {
	// An interrupt already pending at the instant a critical action would
	// start wins the CPU; the critical section begins only after it.
	body := intReq(100,
		intTaskJSON("A", 0, 0, 100, intCrit(4)),
		intIntrJSON("I1", 0, 0, 2, 100))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 6 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@6", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []intTimelineInterval{
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 0, End: 2},
		{ActorType: "task", ActorID: "A", Mode: "critical", Start: 2, End: 6},
	})
}

func TestInterruptAdjacentRunMerge(t *testing.T) {
	// Consecutive run actions of one task merge into a single interval; a
	// lower-priority release does not split it.
	body := intReq(100,
		strings.Join([]string{
			intTaskJSON("A", 1, 0, 100, intRun(2), intRun(3)),
			intTaskJSON("B", 2, 1, 100, intRun(1)),
		}, ","),
		intIntrJSON("I1", 0, 10, 1, 100))
	resp := interruptOK(t, body)
	checkIntTimeline(t, resp.Timeline, []intTimelineInterval{
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 0, End: 5},
		{ActorType: "task", ActorID: "B", Mode: "run", Start: 5, End: 6},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 10, End: 11},
	})
	if resp.Status != "completed" || resp.StoppedAt != 11 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@11", resp.Status, resp.StoppedAt)
	}
}

func checkInterruptError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, wantStatus, rec.Body.String())
	}
	var er struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &er); err != nil {
		t.Fatalf("error response is not JSON: %v", err)
	}
	if er.Error.Code != wantCode {
		t.Fatalf("error code = %q, want %q", er.Error.Code, wantCode)
	}
}

func TestInterruptErrorContract(t *testing.T) {
	valid := intReq(100, intTaskJSON("A", 0, 0, 50, intRun(1)), intIntrJSON("I1", 0, 0, 1, 50))

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, interruptAnalyzePath, nil)
	Handler().ServeHTTP(rec, r)
	checkInterruptError(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
	if rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Allow = %q, want POST", rec.Header().Get("Allow"))
	}

	checkInterruptError(t, postInterrupt(t, valid, "text/plain"), http.StatusUnsupportedMediaType, "unsupported_media_type")
	checkInterruptError(t, postInterrupt(t, `{"horizon":`, "application/json"), http.StatusBadRequest, "invalid_json")
	checkInterruptError(t, postInterrupt(t, `{"horizon":100,"horizon":100,"tasks":[],"interrupts":[]}`, "application/json"), http.StatusBadRequest, "invalid_json")
	checkInterruptError(t, postInterrupt(t, `{"horizon":100,"tasks":[],"interrupts":[],"bogus":1}`, "application/json"), http.StatusBadRequest, "invalid_json")
	checkInterruptError(t, postInterrupt(t, `{"horizon":"100","tasks":[],"interrupts":[]}`, "application/json"), http.StatusUnprocessableEntity, "validation_failed")
}

func TestInterruptValidationFailures(t *testing.T) {
	maxStr := strconv.FormatInt(math.MaxInt64, 10)
	cases := []struct {
		name string
		body string
	}{
		{"missing horizon", `{"tasks":[],"interrupts":[]}`},
		{"zero horizon", intReq(0, intTaskJSON("A", 0, 0, 1, intRun(1)), intIntrJSON("I", 0, 0, 1, 1))},
		{"missing tasks", `{"horizon":10,"interrupts":[` + intIntrJSON("I", 0, 0, 1, 5) + `]}`},
		{"missing interrupts", `{"horizon":10,"tasks":[` + intTaskJSON("A", 0, 0, 5, intRun(1)) + `]}`},
		{"empty interrupts", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(1)), "")},
		{"empty actions", `{"horizon":10,"tasks":[{"id":"A","priority":0,"release":0,"deadline":5,"actions":[]}],"interrupts":[` + intIntrJSON("I", 0, 0, 1, 5) + `]}`},
		{"action without verb", intReq(10, intTaskJSON("A", 0, 0, 5, `{}`), intIntrJSON("I", 0, 0, 1, 5))},
		{"action with two verbs", intReq(10, intTaskJSON("A", 0, 0, 5, `{"run":1,"critical":1}`), intIntrJSON("I", 0, 0, 1, 5))},
		{"zero run", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(0)), intIntrJSON("I", 0, 0, 1, 5))},
		{"negative critical", intReq(10, intTaskJSON("A", 0, 0, 5, intCrit(-3)), intIntrJSON("I", 0, 0, 1, 5))},
		{"duplicate task id", intReq(10,
			strings.Join([]string{intTaskJSON("A", 0, 0, 5, intRun(1)), intTaskJSON("A", 0, 0, 5, intRun(1))}, ","),
			intIntrJSON("I", 0, 0, 1, 5))},
		{"duplicate interrupt id", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(1)),
			strings.Join([]string{intIntrJSON("I", 0, 0, 1, 5), intIntrJSON("I", 0, 1, 1, 5)}, ","))},
		{"interrupt priority too large", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(1)), intIntrJSON("I", 256, 0, 1, 5))},
		{"interrupt arrival negative", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(1)), intIntrJSON("I", 0, -1, 1, 5))},
		{"interrupt arrival at horizon", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(1)), intIntrJSON("I", 0, 10, 1, 15))},
		{"interrupt execution zero", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(1)), intIntrJSON("I", 0, 0, 0, 5))},
		{"interrupt deadline at arrival", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(1)), intIntrJSON("I", 0, 2, 1, 2))},
		{"task run total overflow", intReq(10,
			intTaskJSON("A", 0, 0, 5, `{"run":`+maxStr+`}`, intRun(1)),
			intIntrJSON("I", 0, 0, 1, 5))},
		{"global total overflow", intReq(10,
			intTaskJSON("A", 0, 0, 5, `{"run":`+maxStr+`}`),
			intIntrJSON("I", 0, 0, 1, 5))},
		{"interrupt execution overflow", intReq(10, intTaskJSON("A", 0, 0, 5, intRun(1)),
			strings.Join([]string{intIntrJSON("I1", 0, 0, math.MaxInt64, 5), intIntrJSON("I2", 0, 0, math.MaxInt64, 5)}, ","))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkInterruptError(t, postInterrupt(t, tc.body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}
}

func TestInterruptCountLimits(t *testing.T) {
	// 257 tasks and 4097 interrupts each fail validation; 256/4096 pass.
	mkTasks := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = intTaskJSON("T"+strconv.Itoa(i), 0, 0, 1, intRun(1))
		}
		return strings.Join(parts, ",")
	}
	mkInts := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = intIntrJSON("I"+strconv.Itoa(i), 0, 0, 1, 1)
		}
		return strings.Join(parts, ",")
	}
	checkInterruptError(t, postInterrupt(t, intReq(1, mkTasks(257), intIntrJSON("I", 0, 0, 1, 1)), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
	checkInterruptError(t, postInterrupt(t, intReq(1, intTaskJSON("A", 0, 0, 1, intRun(1)), mkInts(4097)), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
	resp := interruptOK(t, intReq(1, mkTasks(256), mkInts(4096)))
	if resp.Status != "horizon" || resp.StoppedAt != 1 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@1", resp.Status, resp.StoppedAt)
	}
}
