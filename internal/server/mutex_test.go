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

func postMutex(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, mutexAnalyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func mutexOK(t *testing.T, body string) mutexResponse {
	t.Helper()
	rec := postMutex(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp mutexResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func assertTimeline(t *testing.T, got []mutexTimelineInterval, want []mutexTimelineInterval) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("timeline = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v (full: %+v)", i, got[i], want[i], got)
		}
	}
}

func TestMutexPriorityInheritanceAndHandoff(t *testing.T) {
	// low holds m from 0; high releases at 2, preempts, then blocks on m.
	// low inherits 1 and finishes; the handoff unblocks high, which runs next.
	resp := mutexOK(t, `{
		"horizon": 50,
		"tasks": [
			{"id":"low","priority":5,"release":0,"deadline":40,
			 "actions":[{"lock":"m"},{"run":10},{"unlock":"m"}]},
			{"id":"high","priority":1,"release":2,"deadline":40,
			 "actions":[{"lock":"m"},{"run":3},{"unlock":"m"}]}
		]
	}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	if resp.DeadlockAt != nil || resp.Cycle != nil {
		t.Fatalf("deadlock fields = %v, %v, want both null", resp.DeadlockAt, resp.Cycle)
	}
	assertTimeline(t, resp.Timeline, []mutexTimelineInterval{
		{TaskID: "low", Start: 0, End: 2, EffectivePriority: 5},
		{TaskID: "low", Start: 2, End: 10, EffectivePriority: 1},
		{TaskID: "high", Start: 10, End: 13, EffectivePriority: 1},
	})
	if len(resp.Results) != 2 {
		t.Fatalf("results = %+v", resp.Results)
	}
	low, high := resp.Results[0], resp.Results[1]
	if low.Executed != 10 || low.Completion == nil || *low.Completion != 10 ||
		low.State != "completed" || low.BlockedOn != nil || low.DeadlineStatus != "met" {
		t.Fatalf("low result = %+v", low)
	}
	if high.Executed != 3 || high.Completion == nil || *high.Completion != 13 ||
		high.State != "completed" || high.BlockedOn != nil || high.DeadlineStatus != "met" {
		t.Fatalf("high result = %+v", high)
	}
}

func TestMutexDeadlockCycleAndStates(t *testing.T) {
	// A holds x and wants y; B holds y and wants x: cycle at 6. C releases
	// after the deadlock and stays unreleased.
	resp := mutexOK(t, `{
		"horizon": 100,
		"tasks": [
			{"id":"A","priority":2,"release":0,"deadline":50,
			 "actions":[{"lock":"x"},{"run":5},{"lock":"y"},{"unlock":"y"},{"unlock":"x"}]},
			{"id":"B","priority":1,"release":1,"deadline":50,
			 "actions":[{"lock":"y"},{"run":1},{"lock":"x"},{"unlock":"x"},{"unlock":"y"}]},
			{"id":"C","priority":0,"release":10,"deadline":200,
			 "actions":[{"run":1}]}
		]
	}`)
	if resp.Status != "deadlocked" {
		t.Fatalf("status = %q, want deadlocked", resp.Status)
	}
	if resp.DeadlockAt == nil || *resp.DeadlockAt != 6 {
		t.Fatalf("deadlockAt = %v, want 6", resp.DeadlockAt)
	}
	if len(resp.Cycle) != 2 || resp.Cycle[0] != "A" || resp.Cycle[1] != "B" {
		t.Fatalf("cycle = %v, want [A B]", resp.Cycle)
	}
	assertTimeline(t, resp.Timeline, []mutexTimelineInterval{
		{TaskID: "A", Start: 0, End: 1, EffectivePriority: 2},
		{TaskID: "B", Start: 1, End: 2, EffectivePriority: 1},
		{TaskID: "A", Start: 2, End: 6, EffectivePriority: 1},
	})
	a, b, c := resp.Results[0], resp.Results[1], resp.Results[2]
	if a.Executed != 5 || a.Completion != nil || a.State != "blocked" ||
		a.BlockedOn == nil || *a.BlockedOn != "y" || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
	if b.Executed != 1 || b.Completion != nil || b.State != "blocked" ||
		b.BlockedOn == nil || *b.BlockedOn != "x" || b.DeadlineStatus != "missed" {
		t.Fatalf("B result = %+v", b)
	}
	if c.Executed != 0 || c.Completion != nil || c.State != "unreleased" ||
		c.BlockedOn != nil || c.DeadlineStatus != "pending" {
		t.Fatalf("C result = %+v", c)
	}
}

func TestMutexDeadlockCycleRotatesToEarliestInput(t *testing.T) {
	// Each task grabs its first lock, is preempted mid-run by the next, and
	// ends up waiting on it: A -> lb -> B -> lc -> C -> la -> A. B's lock
	// attempt closes the cycle; the reported cycle follows the wait
	// direction starting from A, the earliest in input order.
	resp := mutexOK(t, `{
		"horizon": 100,
		"tasks": [
			{"id":"A","priority":3,"release":0,"deadline":100,
			 "actions":[{"lock":"la"},{"run":5},{"lock":"lb"},{"unlock":"lb"},{"unlock":"la"}]},
			{"id":"B","priority":2,"release":1,"deadline":100,
			 "actions":[{"lock":"lb"},{"run":5},{"lock":"lc"},{"unlock":"lc"},{"unlock":"lb"}]},
			{"id":"C","priority":1,"release":2,"deadline":100,
			 "actions":[{"lock":"lc"},{"run":5},{"lock":"la"},{"unlock":"la"},{"unlock":"lc"}]}
		]
	}`)
	if resp.Status != "deadlocked" {
		t.Fatalf("status = %q, want deadlocked", resp.Status)
	}
	if resp.DeadlockAt == nil || *resp.DeadlockAt != 15 {
		t.Fatalf("deadlockAt = %v, want 15", resp.DeadlockAt)
	}
	if len(resp.Cycle) != 3 || resp.Cycle[0] != "A" || resp.Cycle[1] != "B" || resp.Cycle[2] != "C" {
		t.Fatalf("cycle = %v, want [A B C]", resp.Cycle)
	}
	assertTimeline(t, resp.Timeline, []mutexTimelineInterval{
		{TaskID: "A", Start: 0, End: 1, EffectivePriority: 3},
		{TaskID: "B", Start: 1, End: 2, EffectivePriority: 2},
		{TaskID: "C", Start: 2, End: 7, EffectivePriority: 1},
		{TaskID: "A", Start: 7, End: 11, EffectivePriority: 1},
		{TaskID: "B", Start: 11, End: 15, EffectivePriority: 1},
	})
	wantBlockedOn := []string{"lb", "lc", "la"}
	for i, r := range resp.Results {
		if r.State != "blocked" || r.BlockedOn == nil || *r.BlockedOn != wantBlockedOn[i] {
			t.Fatalf("results[%d] = %+v, want blocked on %s", i, r, wantBlockedOn[i])
		}
		if r.Executed != 5 || r.DeadlineStatus != "missed" {
			t.Fatalf("results[%d] = %+v", i, r)
		}
	}
}

func TestMutexHandoffPicksHighestEffectivePriorityWaiter(t *testing.T) {
	// W1 (prio 3) blocks before W2 (prio 2); the handoff still goes to W2
	// because effective priority wins over blocking order. O's inherited
	// priority drops as waiters arrive, splitting its timeline segments.
	resp := mutexOK(t, `{
		"horizon": 50,
		"tasks": [
			{"id":"O","priority":5,"release":0,"deadline":50,
			 "actions":[{"lock":"m"},{"run":10},{"unlock":"m"}]},
			{"id":"W1","priority":3,"release":1,"deadline":50,
			 "actions":[{"lock":"m"},{"run":1},{"unlock":"m"}]},
			{"id":"W2","priority":2,"release":2,"deadline":50,
			 "actions":[{"lock":"m"},{"run":1},{"unlock":"m"}]}
		]
	}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	assertTimeline(t, resp.Timeline, []mutexTimelineInterval{
		{TaskID: "O", Start: 0, End: 1, EffectivePriority: 5},
		{TaskID: "O", Start: 1, End: 2, EffectivePriority: 3},
		{TaskID: "O", Start: 2, End: 10, EffectivePriority: 2},
		{TaskID: "W2", Start: 10, End: 11, EffectivePriority: 2},
		{TaskID: "W1", Start: 11, End: 12, EffectivePriority: 3},
	})
	o, w1, w2 := resp.Results[0], resp.Results[1], resp.Results[2]
	if o.Completion == nil || *o.Completion != 10 || o.DeadlineStatus != "met" {
		t.Fatalf("O result = %+v", o)
	}
	if w2.Completion == nil || *w2.Completion != 11 || w2.Executed != 1 {
		t.Fatalf("W2 result = %+v", w2)
	}
	if w1.Completion == nil || *w1.Completion != 12 || w1.Executed != 1 {
		t.Fatalf("W1 result = %+v", w1)
	}
}

func TestMutexSameEffectivePriorityDoesNotPreempt(t *testing.T) {
	// B releases while A runs at the same priority: no preemption, and the
	// adjacent same-priority segments of A merge into one interval.
	resp := mutexOK(t, `{
		"horizon": 20,
		"tasks": [
			{"id":"A","priority":2,"release":0,"deadline":20,"actions":[{"run":5}]},
			{"id":"B","priority":2,"release":1,"deadline":20,"actions":[{"run":2}]}
		]
	}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	assertTimeline(t, resp.Timeline, []mutexTimelineInterval{
		{TaskID: "A", Start: 0, End: 5, EffectivePriority: 2},
		{TaskID: "B", Start: 5, End: 7, EffectivePriority: 2},
	})
}

func TestMutexHorizonStatusAndResultStates(t *testing.T) {
	// A runs into the horizon holding m; B stays blocked; C is ready but
	// never runs. Deadline results follow the one-shot rules.
	resp := mutexOK(t, `{
		"horizon": 10,
		"tasks": [
			{"id":"A","priority":5,"release":0,"deadline":10,
			 "actions":[{"lock":"m"},{"run":50},{"unlock":"m"}]},
			{"id":"B","priority":1,"release":1,"deadline":8,
			 "actions":[{"lock":"m"},{"run":1},{"unlock":"m"}]},
			{"id":"C","priority":3,"release":2,"deadline":200,
			 "actions":[{"run":1}]}
		]
	}`)
	if resp.Status != "horizon" {
		t.Fatalf("status = %q, want horizon", resp.Status)
	}
	if resp.DeadlockAt != nil || resp.Cycle != nil {
		t.Fatalf("deadlock fields = %v, %v, want both null", resp.DeadlockAt, resp.Cycle)
	}
	assertTimeline(t, resp.Timeline, []mutexTimelineInterval{
		{TaskID: "A", Start: 0, End: 1, EffectivePriority: 5},
		{TaskID: "A", Start: 1, End: 10, EffectivePriority: 1},
	})
	a, b, c := resp.Results[0], resp.Results[1], resp.Results[2]
	if a.Executed != 10 || a.Completion != nil || a.State != "ready" ||
		a.BlockedOn != nil || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
	if b.Executed != 0 || b.Completion != nil || b.State != "blocked" ||
		b.BlockedOn == nil || *b.BlockedOn != "m" || b.DeadlineStatus != "missed" {
		t.Fatalf("B result = %+v", b)
	}
	if c.Executed != 0 || c.Completion != nil || c.State != "ready" ||
		c.BlockedOn != nil || c.DeadlineStatus != "pending" {
		t.Fatalf("C result = %+v", c)
	}
}

func TestMutexZeroTimeActionsAtHorizonBoundary(t *testing.T) {
	// The run ends exactly at the horizon; the trailing unlock and the
	// completion still take zero time and count.
	resp := mutexOK(t, `{
		"horizon": 5,
		"tasks": [
			{"id":"A","priority":0,"release":0,"deadline":5,
			 "actions":[{"lock":"m"},{"run":5},{"unlock":"m"}]}
		]
	}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	r := resp.Results[0]
	if r.Executed != 5 || r.Completion == nil || *r.Completion != 5 ||
		r.State != "completed" || r.DeadlineStatus != "met" {
		t.Fatalf("result = %+v", r)
	}

	// A task with no run actions completes at its first scheduling instant
	// and leaves no timeline entries.
	resp = mutexOK(t, `{
		"horizon": 10,
		"tasks": [
			{"id":"Z","priority":0,"release":0,"deadline":10,
			 "actions":[{"lock":"m"},{"unlock":"m"}]}
		]
	}`)
	if resp.Status != "completed" || len(resp.Timeline) != 0 {
		t.Fatalf("status = %q, timeline = %+v", resp.Status, resp.Timeline)
	}
	r = resp.Results[0]
	if r.Executed != 0 || r.Completion == nil || *r.Completion != 0 || r.State != "completed" {
		t.Fatalf("result = %+v", r)
	}
}

func TestMutexDeterministic(t *testing.T) {
	body := `{
		"horizon": 60,
		"tasks": [
			{"id":"O","priority":5,"release":0,"deadline":60,
			 "actions":[{"lock":"m"},{"run":10},{"unlock":"m"}]},
			{"id":"W1","priority":3,"release":1,"deadline":60,
			 "actions":[{"lock":"m"},{"run":1},{"unlock":"m"}]},
			{"id":"W2","priority":2,"release":2,"deadline":60,
			 "actions":[{"lock":"m"},{"run":1},{"unlock":"m"}]}
		]
	}`
	first := mutexOK(t, body)
	second := mutexOK(t, body)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic response:\n%s\nvs\n%s", a, b)
	}
}

func TestMutexValidationFailures(t *testing.T) {
	cases := map[string]string{
		"missing actions":      `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1}]}`,
		"null actions":         `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":null}]}`,
		"empty actions":        `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[]}]}`,
		"empty action":         `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{}]}]}`,
		"multi-key action":     `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":1,"lock":"m"}]}]}`,
		"null action":          `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[null]}]}`,
		"run zero":             `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":0}]}]}`,
		"run negative":         `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":-2}]}]}`,
		"run fractional":       `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":1.5}]}]}`,
		"run wrong type":       `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":"1"}]}]}`,
		"lock empty name":      `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"lock":""}]}]}`,
		"lock whitespace name": `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"lock":"a b"}]}]}`,
		"lock wrong type":      `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"lock":3}]}]}`,
		"relock held":          `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"lock":"m"},{"lock":"m"},{"unlock":"m"}]}]}`,
		"unlock unheld":        `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"unlock":"m"}]}]}`,
		"unlock after release": `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"lock":"m"},{"unlock":"m"},{"unlock":"m"}]}]}`,
		"held at end":          `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"lock":"m"},{"run":1}]}]}`,
		"run total overflow":   `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":9223372036854775807},{"run":1}]}]}`,
		"missing priority":     `{"horizon":10,"tasks":[{"id":"a","release":0,"deadline":1,"actions":[{"run":1}]}]}`,
		"release at horizon":   `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":10,"deadline":11,"actions":[{"run":1}]}]}`,
		"deadline at release":  `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":0,"actions":[{"run":1}]}]}`,
		"duplicate id": `{"horizon":10,"tasks":[` +
			`{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":1}]},` +
			`{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":1}]}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postMutex(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	// 1024 actions pass; 1025 fail.
	var many bytes.Buffer
	many.WriteString(`{"horizon":2000,"tasks":[{"id":"a","priority":0,"release":0,"deadline":2000,"actions":[`)
	for i := 0; i < maxActions; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(`{"run":1}`)
	}
	many.WriteString(`]}]}`)
	if rec := postMutex(t, many.String(), "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("1024 actions: status = %d, body: %s", rec.Code, rec.Body.String())
	}
	many.Reset()
	many.WriteString(`{"horizon":2000,"tasks":[{"id":"a","priority":0,"release":0,"deadline":2000,"actions":[`)
	for i := 0; i < maxActions+1; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(`{"run":1}`)
	}
	many.WriteString(`]}]}`)
	assertErrorCode(t, postMutex(t, many.String(), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")

	// A single run of math.MaxInt64 is valid (no overflow without a second
	// addend); execution is truncated to the horizon.
	resp := mutexOK(t, `{"horizon":7,"tasks":[{"id":"a","priority":0,"release":0,"deadline":8,"actions":[{"run":9223372036854775807}]}]}`)
	if resp.Status != "horizon" || resp.Results[0].Executed != 7 {
		t.Fatalf("maxint run: status = %q, result = %+v", resp.Status, resp.Results[0])
	}
}

func TestMutexErrorSemantics(t *testing.T) {
	// Non-POST is 405 with Allow: POST.
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, mutexAnalyzePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}

	body := `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"run":1}]}]}`
	// Missing and wrong media types are 415.
	assertErrorCode(t, postMutex(t, body, ""), http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postMutex(t, body, "text/plain"), http.StatusUnsupportedMediaType, "unsupported_media_type")

	// Malformed JSON, trailing content, duplicate and unknown keys are 400.
	for name, bad := range map[string]string{
		"syntax error":   `{"horizon":10,"tasks":[`,
		"trailing token": body + " x",
		"duplicate key":  `{"horizon":10,"horizon":10,"tasks":[]}`,
		"duplicate action key": `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,` +
			`"actions":[{"run":1,"run":2}]}]}`,
		"unknown top field":    `{"horizon":10,"tasks":[],"bogus":1}`,
		"unknown task field":   `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"execution":1,"actions":[{"run":1}]}]}`,
		"unknown action field": `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"sleep":1}]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postMutex(t, bad, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}

	// Media type parameters are accepted.
	if rec := postMutex(t, body, "application/json; charset=utf-8"); rec.Code != http.StatusOK {
		t.Fatalf("charset parameter: status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestMutexNoPartialResultsOnError(t *testing.T) {
	rec := postMutex(t, `{"horizon":10,"tasks":[{"id":"a","priority":0,"release":0,"deadline":1,"actions":[{"lock":"m"`, "application/json")
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_json")
	if strings.Contains(rec.Body.String(), "timeline") || strings.Contains(rec.Body.String(), "results") {
		t.Fatalf("error response leaked partial results: %s", rec.Body.String())
	}
}

// TestMutexLargeContention runs the maximum-priority fan-in on one mutex:
// w0 blocks behind the boosted owner, and after the handoff every remaining
// waiter acquires the free lock in priority order.
func TestMutexLargeContention(t *testing.T) {
	const n = 64
	var b bytes.Buffer
	b.WriteString(`{"horizon":100000,"tasks":[`)
	b.WriteString(`{"id":"owner","priority":200,"release":0,"deadline":100000,`)
	b.WriteString(`"actions":[{"lock":"m"},{"run":` + strconv.Itoa(n) + `},{"unlock":"m"}]}`)
	for i := 0; i < n; i++ {
		b.WriteString(`,{"id":"w` + strconv.Itoa(i) + `","priority":` + strconv.Itoa(i) +
			`,"release":1,"deadline":100000,"actions":[{"lock":"m"},{"run":1},{"unlock":"m"}]}`)
	}
	b.WriteString(`]}`)
	resp := mutexOK(t, b.String())
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	// Waiters run in priority order: w0 (prio 0) first, then w1, ... w63.
	for i := 0; i < n; i++ {
		want := int64(n + 1 + i)
		got := resp.Results[i+1].Completion
		if got == nil || *got != want {
			t.Fatalf("w%d completion = %v, want %d", i, got, want)
		}
	}
}
