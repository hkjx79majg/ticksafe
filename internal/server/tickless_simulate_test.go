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

func postTickless(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, ticklessSimulatePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func ticklessOK(t *testing.T, body string) ticklessSimulateResponse {
	t.Helper()
	rec := postTickless(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp ticklessSimulateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func ticklessTimerObj(id string, first, period int64) string {
	return `{"id":"` + id + `","first":` + strconv.FormatInt(first, 10) +
		`,"period":` + strconv.FormatInt(period, 10) + `}`
}

func TestTicklessOneShotExactWakeup(t *testing.T) {
	resp := ticklessOK(t, `{"horizon":100,"coalesceWindow":0,"maxSleep":100,"timers":[
		{"id":"A","first":10,"period":0}
	]}`)
	if resp.WakeupCount != 1 || len(resp.Wakeups) != 1 {
		t.Fatalf("wakeups = %+v, want one", resp.Wakeups)
	}
	w := resp.Wakeups[0]
	if w.At != 10 || len(w.Fired) != 1 {
		t.Fatalf("wakeup = %+v, want at 10 with one fired event", w)
	}
	if e := w.Fired[0]; e.ID != "A" || e.Event != 0 || e.ScheduledAt != 10 || e.Lateness != 0 {
		t.Fatalf("fired = %+v", e)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("results = %+v, want one entry", resp.Results)
	}
	if resp.Results[0] != (ticklessEventResult{Event: 0, ScheduledAt: 10, FiredAt: 10, Lateness: 0}) {
		t.Fatalf("result = %+v", resp.Results[0])
	}
}

func TestTicklessCoalesceWindow(t *testing.T) {
	// One-shots nominally due at 10 and 12; a window of 5 folds them into a
	// single wakeup at 15 with lateness 5 and 3.
	resp := ticklessOK(t, `{"horizon":100,"coalesceWindow":5,"maxSleep":100,"timers":[
		{"id":"A","first":10,"period":0},
		{"id":"B","first":12,"period":0}
	]}`)
	if resp.WakeupCount != 1 {
		t.Fatalf("wakeups = %+v, want a single coalesced wakeup", resp.Wakeups)
	}
	fired := resp.Wakeups[0].Fired
	if resp.Wakeups[0].At != 15 || len(fired) != 2 {
		t.Fatalf("wakeup = %+v, want at 15 with two events", resp.Wakeups[0])
	}
	if fired[0] != (ticklessFiredEvent{ID: "A", Event: 0, ScheduledAt: 10, Lateness: 5}) ||
		fired[1] != (ticklessFiredEvent{ID: "B", Event: 0, ScheduledAt: 12, Lateness: 3}) {
		t.Fatalf("fired = %+v", fired)
	}
	// Flat results, ordered by timer input order: A's single event then B's.
	if len(resp.Results) != 2 {
		t.Fatalf("results = %+v, want two entries", resp.Results)
	}
	if resp.Results[0] != (ticklessEventResult{Event: 0, ScheduledAt: 10, FiredAt: 15, Lateness: 5}) {
		t.Fatalf("A result = %+v", resp.Results[0])
	}
	if resp.Results[1] != (ticklessEventResult{Event: 0, ScheduledAt: 12, FiredAt: 15, Lateness: 3}) {
		t.Fatalf("B result = %+v", resp.Results[1])
	}
}

func TestTicklessMaxSleepEmptyWakeups(t *testing.T) {
	// The only event is nominally due at 100; maxSleep 30 forces empty
	// wakeups at 30, 60, 90 before the event fires at 100.
	resp := ticklessOK(t, `{"horizon":200,"coalesceWindow":0,"maxSleep":30,"timers":[
		{"id":"A","first":100,"period":0}
	]}`)
	if resp.WakeupCount != 4 || len(resp.Wakeups) != 4 {
		t.Fatalf("wakeups = %+v, want four", resp.Wakeups)
	}
	wantAt := []int64{30, 60, 90, 100}
	for i, at := range wantAt {
		if resp.Wakeups[i].At != at {
			t.Fatalf("wakeup %d at = %d, want %d (%+v)", i, resp.Wakeups[i].At, at, resp.Wakeups)
		}
	}
	for i := 0; i < 3; i++ {
		if len(resp.Wakeups[i].Fired) != 0 {
			t.Fatalf("wakeup %d must be empty: %+v", i, resp.Wakeups[i])
		}
	}
	last := resp.Wakeups[3]
	if len(last.Fired) != 1 || last.Fired[0].ID != "A" {
		t.Fatalf("final wakeup = %+v", last)
	}
	// Empty wakeups serialize as an explicit empty array, not null.
	raw := postTickless(t, `{"horizon":200,"coalesceWindow":0,"maxSleep":30,"timers":[
		{"id":"A","first":100,"period":0}
	]}`, "application/json")
	if !strings.Contains(raw.Body.String(), `"fired":[]`) {
		t.Fatalf("empty wakeup must serialize fired as []: %s", raw.Body.String())
	}
}

func TestTicklessPeriodicEventsAndNoDrift(t *testing.T) {
	// first=5,period=10,horizon=25 -> nominal events 5,15 (25 is excluded).
	// maxSleep large and window 0 wake exactly at each nominal time.
	resp := ticklessOK(t, `{"horizon":25,"coalesceWindow":0,"maxSleep":1000,"timers":[
		{"id":"A","first":5,"period":10}
	]}`)
	if resp.WakeupCount != 2 {
		t.Fatalf("wakeups = %+v", resp.Wakeups)
	}
	if resp.Wakeups[0].At != 5 || resp.Wakeups[1].At != 15 {
		t.Fatalf("wakeup times = %d,%d", resp.Wakeups[0].At, resp.Wakeups[1].At)
	}
	ev := resp.Results
	if len(ev) != 2 ||
		ev[0] != (ticklessEventResult{Event: 0, ScheduledAt: 5, FiredAt: 5, Lateness: 0}) ||
		ev[1] != (ticklessEventResult{Event: 1, ScheduledAt: 15, FiredAt: 15, Lateness: 0}) {
		t.Fatalf("events = %+v", ev)
	}
}

func TestTicklessPeriodicNominalTimesDoNotDrift(t *testing.T) {
	// first=0,period=10,window=8: event 0 is delivered late at 8, but event 1
	// stays nominally at 10 (delivered at 18); event 2 at 20 is delivered at
	// the horizon (25) rather than being shifted by the accumulated latency.
	resp := ticklessOK(t, `{"horizon":25,"coalesceWindow":8,"maxSleep":1000,"timers":[
		{"id":"A","first":0,"period":10}
	]}`)
	wantWakeups := []ticklessWakeup{
		{At: 8, Fired: []ticklessFiredEvent{{ID: "A", Event: 0, ScheduledAt: 0, Lateness: 8}}},
		{At: 18, Fired: []ticklessFiredEvent{{ID: "A", Event: 1, ScheduledAt: 10, Lateness: 8}}},
		{At: 25, Fired: []ticklessFiredEvent{{ID: "A", Event: 2, ScheduledAt: 20, Lateness: 5}}},
	}
	if len(resp.Wakeups) != len(wantWakeups) {
		t.Fatalf("wakeups = %+v, want %+v", resp.Wakeups, wantWakeups)
	}
	for i := range wantWakeups {
		if resp.Wakeups[i].At != wantWakeups[i].At ||
			len(resp.Wakeups[i].Fired) != len(wantWakeups[i].Fired) ||
			resp.Wakeups[i].Fired[0] != wantWakeups[i].Fired[0] {
			t.Fatalf("wakeup %d = %+v, want %+v", i, resp.Wakeups[i], wantWakeups[i])
		}
	}
}

func TestTicklessDeliveryAtHorizon(t *testing.T) {
	// Event nominally due at 95; maxSleep 30 lands sleeps at 30, 60, 90, and a
	// coalesce window of 10 then makes min(105, 120, 100) = horizon 100, so
	// the event is delivered at the horizon with lateness 5.
	resp := ticklessOK(t, `{"horizon":100,"coalesceWindow":10,"maxSleep":30,"timers":[
		{"id":"A","first":95,"period":0}
	]}`)
	wantEmpty := []int64{30, 60, 90}
	if resp.WakeupCount != len(wantEmpty)+1 {
		t.Fatalf("wakeups = %+v", resp.Wakeups)
	}
	for i, at := range wantEmpty {
		if resp.Wakeups[i].At != at || len(resp.Wakeups[i].Fired) != 0 {
			t.Fatalf("wakeup %d = %+v, want empty at %d", i, resp.Wakeups[i], at)
		}
	}
	last := resp.Wakeups[len(resp.Wakeups)-1]
	if last.At != 100 || len(last.Fired) != 1 {
		t.Fatalf("last wakeup = %+v, want delivery at horizon 100", last)
	}
	if e := last.Fired[0]; e.ScheduledAt != 95 || e.Lateness != 5 {
		t.Fatalf("fired = %+v, want lateness 5", e)
	}
	if ev := resp.Results[0]; ev.FiredAt != 100 || ev.Lateness != 5 {
		t.Fatalf("result = %+v", ev)
	}
}

func TestTicklessSameTimeOrdering(t *testing.T) {
	// Equal nominal times are delivered in timer input order. Horizon 40
	// admits A's events at 10 and 30; the latter ties with C's one-shot.
	resp := ticklessOK(t, `{"horizon":40,"coalesceWindow":0,"maxSleep":100,"timers":[
		{"id":"A","first":10,"period":20},
		{"id":"B","first":10,"period":0},
		{"id":"C","first":30,"period":0}
	]}`)
	if resp.WakeupCount != 2 {
		t.Fatalf("wakeups = %+v", resp.Wakeups)
	}
	fired := resp.Wakeups[0].Fired
	if len(fired) != 2 || fired[0].ID != "A" || fired[1].ID != "B" {
		t.Fatalf("fired order = %+v, want A then B", fired)
	}
	// A's next event at 30 coalesces with C at 30: A precedes C on input
	// order even though both are one-shot-style arrivals.
	fired = resp.Wakeups[1].Fired
	if resp.Wakeups[1].At != 30 || len(fired) != 2 || fired[0].ID != "A" || fired[1].ID != "C" {
		t.Fatalf("second wakeup = %+v", resp.Wakeups[1])
	}
	if e := fired[0]; e.Event != 1 || e.ScheduledAt != 30 {
		t.Fatalf("A event 1 = %+v", e)
	}
}

func TestTicklessEachEventDeliveredExactlyOnce(t *testing.T) {
	resp := ticklessOK(t, `{"horizon":50,"coalesceWindow":7,"maxSleep":13,"timers":[
		{"id":"A","first":0,"period":6},
		{"id":"B","first":3,"period":11},
		{"id":"C","first":49,"period":0}
	]}`)
	want := map[string]int{"A": 9, "B": 5, "C": 1} // A: 0..48, B: 3..47
	seen := make(map[string]map[int]bool)
	delivered := 0
	for _, w := range resp.Wakeups {
		for _, e := range w.Fired {
			if seen[e.ID] == nil {
				seen[e.ID] = make(map[int]bool)
			}
			if seen[e.ID][e.Event] {
				t.Fatalf("event %s/%d delivered twice", e.ID, e.Event)
			}
			seen[e.ID][e.Event] = true
			delivered++
		}
	}
	total := 0
	for id, n := range want {
		total += n
		if len(seen[id]) != n {
			t.Fatalf("timer %s delivered %d unique events, want %d", id, len(seen[id]), n)
		}
	}
	if delivered != total {
		t.Fatalf("delivered %d events, want %d", delivered, total)
	}
	// Flat results: timer A's range (9 events), then B's (5), then C's (1);
	// within each range the event number must be contiguous from 0.
	if len(resp.Results) != total {
		t.Fatalf("results has %d entries, want %d", len(resp.Results), total)
	}
	ranges := []struct {
		id    string
		count int
	}{{"A", want["A"]}, {"B", want["B"]}, {"C", want["C"]}}
	idx := 0
	for _, rg := range ranges {
		for k := 0; k < rg.count; k++ {
			e := resp.Results[idx]
			if e.Event != k {
				t.Fatalf("results[%d] (%s) event = %d, want %d", idx, rg.id, e.Event, k)
			}
			if !seen[rg.id][k] {
				t.Fatalf("result event %s/%d was never fired", rg.id, k)
			}
			idx++
		}
	}
}

func TestTicklessDeterministic(t *testing.T) {
	body := `{"horizon":60,"coalesceWindow":4,"maxSleep":9,"timers":[
		{"id":"A","first":2,"period":7},
		{"id":"B","first":0,"period":13}
	]}`
	first := postTickless(t, body, "application/json")
	second := postTickless(t, body, "application/json")
	if first.Body.String() != second.Body.String() {
		t.Fatalf("responses differ:\n%s\n%s", first.Body.String(), second.Body.String())
	}
}

func TestTicklessEventCapRejected(t *testing.T) {
	// first=0,period=1 over horizon 100001 yields 100001 events.
	body := `{"horizon":100001,"coalesceWindow":0,"maxSleep":1000000000000,"timers":[
		{"id":"A","first":0,"period":1}
	]}`
	assertErrorCode(t, postTickless(t, body, "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")

	// Exactly 100000 events is accepted and needs no more than 100000 wakeups
	// (a generous window coalesces them).
	ok := `{"horizon":100000,"coalesceWindow":1000000000000,"maxSleep":1000000000000,"timers":[
		{"id":"A","first":0,"period":1}
	]}`
	if rec := postTickless(t, ok, "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
}

func TestTicklessWakeupCapRejected(t *testing.T) {
	// A single one-shot at 100001 with maxSleep 1 forces empty wakeups at
	// 1..100001, i.e. 100001 actual wakeups with only one event.
	body := `{"horizon":100002,"coalesceWindow":0,"maxSleep":1,"timers":[
		{"id":"A","first":100001,"period":0}
	]}`
	assertErrorCode(t, postTickless(t, body, "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")

	// 100000 wakeups exactly (1..100000, the last one delivering the event).
	ok := `{"horizon":100001,"coalesceWindow":0,"maxSleep":1,"timers":[
		{"id":"A","first":100000,"period":0}
	]}`
	rec := postTickless(t, ok, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp ticklessSimulateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if resp.WakeupCount != 100000 {
		t.Fatalf("wakeupCount = %d, want 100000", resp.WakeupCount)
	}
}

func TestTicklessMethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, ticklessSimulatePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
}

func TestTicklessUnsupportedMediaType(t *testing.T) {
	body := `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[{"id":"a","first":1,"period":0}]}`
	assertErrorCode(t, postTickless(t, body, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postTickless(t, body, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	if rec := postTickless(t, body, "application/json; charset=utf-8"); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestTicklessInvalidJSON(t *testing.T) {
	cases := map[string]string{
		"empty":               "",
		"syntax error":        "{",
		"trailing token":      `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[]} garbage`,
		"unknown field":       `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[],"bogus":1}`,
		"unknown timer field": `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[{"id":"a","first":1,"period":0,"x":1}]}`,
		"duplicate top key":   `{"horizon":10,"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[]}`,
		"duplicate timer key": `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[{"id":"a","id":"b","first":1,"period":0}]}`,
		"malformed number":    `{"horizon":010,"coalesceWindow":0,"maxSleep":5,"timers":[]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postTickless(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}

	padded := `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[` +
		ticklessTimerObj("a", 1, 0) + strings.Repeat(" ", maxBodyBytes) + `]}`
	assertErrorCode(t, postTickless(t, padded, "application/json"),
		http.StatusBadRequest, "invalid_json")
}

func TestTicklessValidationFailures(t *testing.T) {
	goodTimer := ticklessTimerObj("a", 1, 0)
	wrap := func(timer string) string {
		return `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[` + timer + `]}`
	}
	cases := map[string]string{
		"json null body":           `null`,
		"json array body":          `[]`,
		"missing horizon":          `{"coalesceWindow":0,"maxSleep":5,"timers":[` + goodTimer + `]}`,
		"horizon null":             `{"horizon":null,"coalesceWindow":0,"maxSleep":5,"timers":[]}`,
		"horizon zero":             `{"horizon":0,"coalesceWindow":0,"maxSleep":5,"timers":[` + goodTimer + `]}`,
		"horizon negative":         `{"horizon":-1,"coalesceWindow":0,"maxSleep":5,"timers":[` + goodTimer + `]}`,
		"horizon too large":        `{"horizon":1000000000001,"coalesceWindow":0,"maxSleep":5,"timers":[` + goodTimer + `]}`,
		"horizon wrong type":       `{"horizon":"10","coalesceWindow":0,"maxSleep":5,"timers":[]}`,
		"missing coalesceWindow":   `{"horizon":10,"maxSleep":5,"timers":[` + goodTimer + `]}`,
		"coalesceWindow null":      `{"horizon":10,"coalesceWindow":null,"maxSleep":5,"timers":[]}`,
		"coalesceWindow negative":  `{"horizon":10,"coalesceWindow":-1,"maxSleep":5,"timers":[` + goodTimer + `]}`,
		"coalesceWindow too large": `{"horizon":10,"coalesceWindow":1000000000001,"maxSleep":5,"timers":[` + goodTimer + `]}`,
		"missing maxSleep":         `{"horizon":10,"coalesceWindow":0,"timers":[` + goodTimer + `]}`,
		"maxSleep null":            `{"horizon":10,"coalesceWindow":0,"maxSleep":null,"timers":[]}`,
		"maxSleep zero":            `{"horizon":10,"coalesceWindow":0,"maxSleep":0,"timers":[` + goodTimer + `]}`,
		"maxSleep negative":        `{"horizon":10,"coalesceWindow":0,"maxSleep":-1,"timers":[` + goodTimer + `]}`,
		"maxSleep too large":       `{"horizon":10,"coalesceWindow":0,"maxSleep":1000000000001,"timers":[` + goodTimer + `]}`,
		"missing timers":           `{"horizon":10,"coalesceWindow":0,"maxSleep":5}`,
		"timers null":              `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":null}`,
		"empty timers":             `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[]}`,
		"timers not an array":      `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":{}}`,
		"missing id":               wrap(`{"first":1,"period":0}`),
		"missing first":            wrap(`{"id":"a","period":0}`),
		"missing period":           wrap(`{"id":"a","first":1}`),
		"first null":               wrap(`{"id":"a","first":null,"period":0}`),
		"period null":              wrap(`{"id":"a","first":1,"period":null}`),
		"first negative":           wrap(`{"id":"a","first":-1,"period":0}`),
		"first at horizon":         wrap(`{"id":"a","first":10,"period":0}`),
		"first above horizon":      wrap(`{"id":"a","first":11,"period":0}`),
		"first wrong type":         wrap(`{"id":"a","first":"1","period":0}`),
		"first fractional":         wrap(`{"id":"a","first":0.5,"period":0}`),
		"period negative":          wrap(`{"id":"a","first":1,"period":-1}`),
		"empty id":                 wrap(`{"id":"","first":1,"period":0}`),
		"whitespace id":            wrap(`{"id":"a b","first":1,"period":0}`),
		"duplicate id":             `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[` + ticklessTimerObj("a", 1, 0) + "," + ticklessTimerObj("a", 2, 0) + `]}`,
		"horizon out of int64":     `{"horizon":99999999999999999999999,"coalesceWindow":0,"maxSleep":5,"timers":[]}`,
		"period out of int64":      wrap(`{"id":"a","first":1,"period":99999999999999999999999}`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postTickless(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	// coalesceWindow zero and a period beyond the horizon (single event) are
	// both legitimate.
	for _, body := range []string{
		`{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[` + ticklessTimerObj("a", 5, 1000000000000) + `]}`,
		`{"horizon":10,"coalesceWindow":1000000000000,"maxSleep":1,"timers":[` + ticklessTimerObj("a", 0, 0) + `]}`,
	} {
		if rec := postTickless(t, body, "application/json"); rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body: %s (%s)", rec.Code, rec.Body.String(), body)
		}
	}

	// 257 timers exceeds the timer-count bound.
	var many bytes.Buffer
	many.WriteString(`{"horizon":1000,"coalesceWindow":0,"maxSleep":1000,"timers":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(ticklessTimerObj("t"+strconv.Itoa(i), 1, 0))
	}
	many.WriteString(`]}`)
	assertErrorCode(t, postTickless(t, many.String(), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
}

func TestTicklessNoPartialResultsOnError(t *testing.T) {
	rec := postTickless(t, `{"horizon":10,"coalesceWindow":0,"maxSleep":5,"timers":[{"id":"a"`, "application/json")
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_json")
	if strings.Contains(rec.Body.String(), "wakeups") || strings.Contains(rec.Body.String(), "results") {
		t.Fatalf("error response leaked partial results: %s", rec.Body.String())
	}
}

func TestTicklessExistingEndpointsUnaffected(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d", rec.Code)
	}
}
