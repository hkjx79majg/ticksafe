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

func TestTicklessCoalescesEventsIntoOneWake(t *testing.T) {
	// coalesceWindow 5: A due at 0 and B due at 3 are both delivered at the
	// single wake at 5; maxSleep and the horizon do not bind.
	resp := ticklessOK(t, `{"horizon":100,"coalesceWindow":5,"maxSleep":100,"timers":[
		{"id":"A","first":0,"period":0},
		{"id":"B","first":3,"period":0}
	]}`)
	if resp.WakeupCount != 1 || len(resp.Wakeups) != 1 {
		t.Fatalf("wakeups = %+v, want a single wake at 5", resp.Wakeups)
	}
	w := resp.Wakeups[0]
	if w.At != 5 {
		t.Fatalf("wake at = %d, want 5", w.At)
	}
	// Delivery order is nominal time, then input order.
	if len(w.Fired) != 2 {
		t.Fatalf("fired = %+v, want 2 events", w.Fired)
	}
	if w.Fired[0] != (ticklessFiredEvent{ID: "A", Event: 0, ScheduledAt: 0, Lateness: 5}) {
		t.Fatalf("fired[0] = %+v, want A at 0 lateness 5", w.Fired[0])
	}
	if w.Fired[1] != (ticklessFiredEvent{ID: "B", Event: 0, ScheduledAt: 3, Lateness: 2}) {
		t.Fatalf("fired[1] = %+v, want B at 3 lateness 2", w.Fired[1])
	}
	if len(resp.Results) != 2 || resp.Results[0].ID != "A" || resp.Results[1].ID != "B" {
		t.Fatalf("results must follow timer input order: %+v", resp.Results)
	}
	if got := resp.Results[0].Events[0]; got != (ticklessEventResult{Event: 0, ScheduledAt: 0, FiredAt: 5, Lateness: 5}) {
		t.Fatalf("A result = %+v", got)
	}
	if got := resp.Results[1].Events[0]; got != (ticklessEventResult{Event: 0, ScheduledAt: 3, FiredAt: 5, Lateness: 2}) {
		t.Fatalf("B result = %+v", got)
	}
}

func TestTicklessEmptyMaxSleepWakeupsAreRetained(t *testing.T) {
	// maxSleep 3 with the only one-shot event due at 10 forces empty wakes at
	// 3, 6, 9 before the event fires at 10.
	resp := ticklessOK(t, `{"horizon":100,"coalesceWindow":0,"maxSleep":3,"timers":[
		{"id":"A","first":10,"period":0}
	]}`)
	wantAt := []int64{3, 6, 9, 10}
	if resp.WakeupCount != len(wantAt) || len(resp.Wakeups) != len(wantAt) {
		t.Fatalf("wakeups = %+v, want %v", resp.Wakeups, wantAt)
	}
	for i, at := range wantAt {
		if resp.Wakeups[i].At != at {
			t.Fatalf("wakeup[%d].At = %d, want %d", i, resp.Wakeups[i].At, at)
		}
	}
	for i := 0; i < 3; i++ {
		if len(resp.Wakeups[i].Fired) != 0 {
			t.Fatalf("wakeup at %d must be empty, got %+v", wantAt[i], resp.Wakeups[i].Fired)
		}
		// An empty wake still serializes as an array, not null.
		if resp.Wakeups[i].Fired == nil {
			t.Fatalf("empty fired list must marshal as [], not null")
		}
	}
	if got := resp.Wakeups[3].Fired; len(got) != 1 ||
		got[0] != (ticklessFiredEvent{ID: "A", Event: 0, ScheduledAt: 10, Lateness: 0}) {
		t.Fatalf("final fired = %+v, want A event 0 on time at 10", got)
	}
	if got := resp.Results[0].Events[0]; got != (ticklessEventResult{Event: 0, ScheduledAt: 10, FiredAt: 10, Lateness: 0}) {
		t.Fatalf("result = %+v", got)
	}
}

func TestTicklessPeriodicEventsDeriveFromFirstWithoutDrift(t *testing.T) {
	// first 2, period 5 over [0,20) expands to nominal times 2,7,12,17. With
	// coalesceWindow 3 the actual wakes land at 5,10,15,20, but scheduledAt
	// must stay anchored to first+k*period rather than slide with the delay.
	resp := ticklessOK(t, `{"horizon":20,"coalesceWindow":3,"maxSleep":100,"timers":[
		{"id":"A","first":2,"period":5}
	]}`)
	wantAt := []int64{5, 10, 15, 20}
	if resp.WakeupCount != 4 {
		t.Fatalf("wakeupCount = %d, want 4", resp.WakeupCount)
	}
	for i, at := range wantAt {
		if resp.Wakeups[i].At != at || len(resp.Wakeups[i].Fired) != 1 {
			t.Fatalf("wakeup[%d] = %+v, want one event at %d", i, resp.Wakeups[i], at)
		}
	}
	events := resp.Results[0].Events
	if len(events) != 4 {
		t.Fatalf("events = %+v, want 4", events)
	}
	wantScheduled := []int64{2, 7, 12, 17}
	for k := range events {
		want := ticklessEventResult{Event: k, ScheduledAt: wantScheduled[k], FiredAt: wantAt[k], Lateness: 3}
		if events[k] != want {
			t.Fatalf("events[%d] = %+v, want %+v (scheduledAt must not drift)", k, events[k], want)
		}
	}
}

func TestTicklessPeriodicExactWakesWithZeroWindow(t *testing.T) {
	resp := ticklessOK(t, `{"horizon":20,"coalesceWindow":0,"maxSleep":100,"timers":[
		{"id":"A","first":2,"period":5}
	]}`)
	wantAt := []int64{2, 7, 12, 17}
	if resp.WakeupCount != 4 {
		t.Fatalf("wakeupCount = %d, want 4", resp.WakeupCount)
	}
	for i, at := range wantAt {
		if resp.Wakeups[i].At != at {
			t.Fatalf("wakeup[%d].At = %d, want %d", i, resp.Wakeups[i].At, at)
		}
		if got := resp.Wakeups[i].Fired[0]; got.Lateness != 0 || got.ScheduledAt != at || got.Event != i {
			t.Fatalf("wakeup[%d] fired = %+v, want on-time event %d", i, got, i)
		}
	}
}

func TestTicklessEventBeforeHorizonCanFireAtHorizon(t *testing.T) {
	// Event nominally due at 8; the coalesced wake at 13 is clamped to the
	// horizon 10, so it is delivered at the horizon with lateness 2.
	resp := ticklessOK(t, `{"horizon":10,"coalesceWindow":5,"maxSleep":100,"timers":[
		{"id":"A","first":8,"period":0}
	]}`)
	if resp.WakeupCount != 1 || resp.Wakeups[0].At != 10 {
		t.Fatalf("wakeups = %+v, want single wake at horizon 10", resp.Wakeups)
	}
	if got := resp.Wakeups[0].Fired; len(got) != 1 ||
		got[0] != (ticklessFiredEvent{ID: "A", Event: 0, ScheduledAt: 8, Lateness: 2}) {
		t.Fatalf("fired = %+v, want A at 8 delivered at 10", got)
	}
	if got := resp.Results[0].Events[0]; got != (ticklessEventResult{Event: 0, ScheduledAt: 8, FiredAt: 10, Lateness: 2}) {
		t.Fatalf("result = %+v", got)
	}
}

func TestTicklessDeliveryOrdering(t *testing.T) {
	// A wide window coalesces everything into the horizon wake. Input order
	// is B(timer0), A(timer1), C(timer2); C is periodic at 1,3,5 and the
	// one-shots B,A are both due at 5. Delivery must be nominal time, then
	// timer input order, then event number.
	resp := ticklessOK(t, `{"horizon":6,"coalesceWindow":100,"maxSleep":100,"timers":[
		{"id":"B","first":5,"period":0},
		{"id":"A","first":5,"period":0},
		{"id":"C","first":1,"period":2}
	]}`)
	if resp.WakeupCount != 1 || resp.Wakeups[0].At != 6 {
		t.Fatalf("wakeups = %+v, want one wake at 6", resp.Wakeups)
	}
	wantFired := []ticklessFiredEvent{
		{ID: "C", Event: 0, ScheduledAt: 1, Lateness: 5},
		{ID: "C", Event: 1, ScheduledAt: 3, Lateness: 3},
		{ID: "B", Event: 0, ScheduledAt: 5, Lateness: 1},
		{ID: "A", Event: 0, ScheduledAt: 5, Lateness: 1},
		{ID: "C", Event: 2, ScheduledAt: 5, Lateness: 1},
	}
	if len(resp.Wakeups[0].Fired) != len(wantFired) {
		t.Fatalf("fired = %+v, want %+v", resp.Wakeups[0].Fired, wantFired)
	}
	for i := range wantFired {
		if resp.Wakeups[0].Fired[i] != wantFired[i] {
			t.Fatalf("fired[%d] = %+v, want %+v", i, resp.Wakeups[0].Fired[i], wantFired[i])
		}
	}
	// Results stay grouped by timer input order, events numbered from zero.
	wantResults := []ticklessTimerResult{
		{ID: "B", Events: []ticklessEventResult{{Event: 0, ScheduledAt: 5, FiredAt: 6, Lateness: 1}}},
		{ID: "A", Events: []ticklessEventResult{{Event: 0, ScheduledAt: 5, FiredAt: 6, Lateness: 1}}},
		{ID: "C", Events: []ticklessEventResult{
			{Event: 0, ScheduledAt: 1, FiredAt: 6, Lateness: 5},
			{Event: 1, ScheduledAt: 3, FiredAt: 6, Lateness: 3},
			{Event: 2, ScheduledAt: 5, FiredAt: 6, Lateness: 1},
		}},
	}
	if len(resp.Results) != len(wantResults) {
		t.Fatalf("results = %+v, want %+v", resp.Results, wantResults)
	}
	for i := range wantResults {
		if resp.Results[i].ID != wantResults[i].ID ||
			len(resp.Results[i].Events) != len(wantResults[i].Events) {
			t.Fatalf("results[%d] = %+v, want %+v", i, resp.Results[i], wantResults[i])
		}
		for k := range wantResults[i].Events {
			if resp.Results[i].Events[k] != wantResults[i].Events[k] {
				t.Fatalf("results[%d].Events[%d] = %+v, want %+v",
					i, k, resp.Results[i].Events[k], wantResults[i].Events[k])
			}
		}
	}
}

func TestTicklessWakeAtTimeZero(t *testing.T) {
	// horizon 1 forces first to 0; the earliest wake is scheduled at 0.
	resp := ticklessOK(t, `{"horizon":1,"coalesceWindow":0,"maxSleep":1,"timers":[
		{"id":"A","first":0,"period":0}
	]}`)
	if resp.WakeupCount != 1 || resp.Wakeups[0].At != 0 {
		t.Fatalf("wakeups = %+v, want one wake at 0", resp.Wakeups)
	}
	if got := resp.Wakeups[0].Fired[0]; got != (ticklessFiredEvent{ID: "A", Event: 0, ScheduledAt: 0, Lateness: 0}) {
		t.Fatalf("fired = %+v", got)
	}
}

func TestTicklessLargePeriodBeyondHorizonFiresOnce(t *testing.T) {
	// period has no upper bound beyond being a non-negative integer; a period
	// past the horizon makes a periodic timer behave like a one-shot.
	resp := ticklessOK(t, `{"horizon":10,"coalesceWindow":0,"maxSleep":100,"timers":[
		{"id":"A","first":3,"period":1000000000000}
	]}`)
	if resp.WakeupCount != 1 || len(resp.Results[0].Events) != 1 {
		t.Fatalf("resp = %+v, want one event", resp)
	}
	if got := resp.Results[0].Events[0]; got != (ticklessEventResult{Event: 0, ScheduledAt: 3, FiredAt: 3, Lateness: 0}) {
		t.Fatalf("event = %+v", got)
	}
}

func TestTicklessEventCountLimit(t *testing.T) {
	// Exactly 100000 events (and exactly 100000 precise wakes) is legal.
	resp := ticklessOK(t, `{"horizon":100000,"coalesceWindow":0,"maxSleep":1000000000000,"timers":[
		{"id":"A","first":0,"period":1}
	]}`)
	if resp.WakeupCount != 100000 {
		t.Fatalf("wakeupCount = %d, want 100000", resp.WakeupCount)
	}
	if got := len(resp.Results[0].Events); got != 100000 {
		t.Fatalf("events = %d, want 100000", got)
	}

	// One more event crosses the bound at validation time.
	assertErrorCode(t, postTickless(t, `{"horizon":100001,"coalesceWindow":0,"maxSleep":1000000000000,"timers":[
		{"id":"A","first":0,"period":1}
	]}`, "application/json"), http.StatusUnprocessableEntity, "validation_failed")

	// The bound counts events across timers.
	assertErrorCode(t, postTickless(t, `{"horizon":50001,"coalesceWindow":0,"maxSleep":1000000000000,"timers":[
		{"id":"A","first":0,"period":1},
		{"id":"B","first":0,"period":1}
	]}`, "application/json"), http.StatusUnprocessableEntity, "validation_failed")
}

func TestTicklessWakeupCountLimit(t *testing.T) {
	// maxSleep 1 advances one microsecond per wake. A one-shot at 100000 with
	// horizon 100001 produces 99999 empty wakes plus the firing wake = 100000.
	resp := ticklessOK(t, `{"horizon":100001,"coalesceWindow":0,"maxSleep":1,"timers":[
		{"id":"A","first":100000,"period":0}
	]}`)
	if resp.WakeupCount != 100000 {
		t.Fatalf("wakeupCount = %d, want 100000", resp.WakeupCount)
	}

	// Pushing the event to 100001 (horizon 100002) needs 100000 empty wakes
	// before the firing wake, i.e. 100001 actual wakeups: 422 even though the
	// event count is just one.
	assertErrorCode(t, postTickless(t, `{"horizon":100002,"coalesceWindow":0,"maxSleep":1,"timers":[
		{"id":"A","first":100001,"period":0}
	]}`, "application/json"), http.StatusUnprocessableEntity, "validation_failed")
}

func TestTicklessDeterministic(t *testing.T) {
	body := `{"horizon":50,"coalesceWindow":4,"maxSleep":7,"timers":[
		{"id":"A","first":0,"period":6},
		{"id":"B","first":2,"period":9},
		{"id":"C","first":13,"period":0}
	]}`
	first := postTickless(t, body, "application/json")
	second := postTickless(t, body, "application/json")
	if first.Code != http.StatusOK || first.Body.String() != second.Body.String() {
		t.Fatalf("same request produced different responses:\n%s\n%s",
			first.Body.String(), second.Body.String())
	}
}

func TestTicklessEveryEventDeliveredExactlyOnce(t *testing.T) {
	resp := ticklessOK(t, `{"horizon":40,"coalesceWindow":6,"maxSleep":5,"timers":[
		{"id":"A","first":0,"period":7},
		{"id":"B","first":3,"period":11},
		{"id":"C","first":39,"period":0}
	]}`)
	delivered := map[string]map[int]bool{}
	for _, w := range resp.Wakeups {
		for _, f := range w.Fired {
			if delivered[f.ID] == nil {
				delivered[f.ID] = map[int]bool{}
			}
			if delivered[f.ID][f.Event] {
				t.Fatalf("event %s/%d delivered more than once", f.ID, f.Event)
			}
			delivered[f.ID][f.Event] = true
		}
	}
	for _, tr := range resp.Results {
		for _, e := range tr.Events {
			if !delivered[tr.ID][e.Event] {
				t.Fatalf("event %s/%d never delivered", tr.ID, e.Event)
			}
		}
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
	body := `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[]}`
	assertErrorCode(t, postTickless(t, body, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postTickless(t, body, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	rec := postTickless(t, `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[`+
		ticklessTimerObj("a", 0, 0)+`]}`, "application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestTicklessInvalidJSON(t *testing.T) {
	cases := map[string]string{
		"empty":          "",
		"syntax error":   "{",
		"trailing token": `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[]} garbage`,
		"unknown field":  `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[],"bogus":1}`,
		"unknown timer field": `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[` +
			`{"id":"a","first":0,"period":1,"extra":2}]}`,
		"duplicate top key":   `{"horizon":10,"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[]}`,
		"duplicate timer key": `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[{"id":"a","id":"b","first":0,"period":1}]}`,
		"malformed number":    `{"horizon":010,"coalesceWindow":0,"maxSleep":1,"timers":[]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postTickless(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}

	padded := `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[` + ticklessTimerObj("a", 0, 1) +
		strings.Repeat(" ", maxBodyBytes) + `]}`
	assertErrorCode(t, postTickless(t, padded, "application/json"),
		http.StatusBadRequest, "invalid_json")
}

func TestTicklessValidationFailures(t *testing.T) {
	wrap := func(timer string) string {
		return `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[` + timer + `]}`
	}
	cases := map[string]string{
		"json null body":            `null`,
		"json array body":           `[]`,
		"missing horizon":           `{"coalesceWindow":0,"maxSleep":1,"timers":[` + ticklessTimerObj("a", 0, 1) + `]}`,
		"horizon null":              `{"horizon":null,"coalesceWindow":0,"maxSleep":1,"timers":[]}`,
		"horizon zero":              `{"horizon":0,"coalesceWindow":0,"maxSleep":1,"timers":[]}`,
		"horizon negative":          `{"horizon":-1,"coalesceWindow":0,"maxSleep":1,"timers":[]}`,
		"horizon too large":         `{"horizon":1000000000001,"coalesceWindow":0,"maxSleep":1,"timers":[]}`,
		"horizon wrong type":        `{"horizon":"10","coalesceWindow":0,"maxSleep":1,"timers":[]}`,
		"missing coalesceWindow":    `{"horizon":10,"maxSleep":1,"timers":[` + ticklessTimerObj("a", 0, 1) + `]}`,
		"coalesceWindow null":       `{"horizon":10,"coalesceWindow":null,"maxSleep":1,"timers":[]}`,
		"coalesceWindow negative":   `{"horizon":10,"coalesceWindow":-1,"maxSleep":1,"timers":[]}`,
		"coalesceWindow too large":  `{"horizon":10,"coalesceWindow":1000000000001,"maxSleep":1,"timers":[]}`,
		"coalesceWindow wrong type": `{"horizon":10,"coalesceWindow":"0","maxSleep":1,"timers":[]}`,
		"missing maxSleep":          `{"horizon":10,"coalesceWindow":0,"timers":[` + ticklessTimerObj("a", 0, 1) + `]}`,
		"maxSleep null":             `{"horizon":10,"coalesceWindow":0,"maxSleep":null,"timers":[]}`,
		"maxSleep zero":             `{"horizon":10,"coalesceWindow":0,"maxSleep":0,"timers":[]}`,
		"maxSleep negative":         `{"horizon":10,"coalesceWindow":0,"maxSleep":-1,"timers":[]}`,
		"maxSleep too large":        `{"horizon":10,"coalesceWindow":0,"maxSleep":1000000000001,"timers":[]}`,
		"maxSleep fractional":       `{"horizon":10,"coalesceWindow":0,"maxSleep":1.5,"timers":[]}`,
		"missing timers":            `{"horizon":10,"coalesceWindow":0,"maxSleep":1}`,
		"timers null":               `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":null}`,
		"empty timers":              `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[]}`,
		"timers not an array":       `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":{}}`,
		"missing id":                `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[{"first":0,"period":1}]}`,
		"missing first":             wrap(`{"id":"a","period":1}`),
		"missing period":            wrap(`{"id":"a","first":0}`),
		"null field":                wrap(`{"id":"a","first":0,"period":null}`),
		"wrong field type":          wrap(`{"id":"a","first":"0","period":1}`),
		"first fractional":          wrap(`{"id":"a","first":0.5,"period":1}`),
		"empty id":                  wrap(`{"id":"","first":0,"period":1}`),
		"whitespace id":             wrap(`{"id":"a b","first":0,"period":1}`),
		"duplicate id": `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[` +
			ticklessTimerObj("a", 0, 1) + "," + ticklessTimerObj("a", 2, 1) + `]}`,
		"first negative":      wrap(`{"id":"a","first":-1,"period":1}`),
		"first at horizon":    `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[{"id":"a","first":10,"period":1}]}`,
		"first above horizon": `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[{"id":"a","first":11,"period":1}]}`,
		"period negative":     wrap(`{"id":"a","first":0,"period":-1}`),
		"number out of int64": wrap(`{"id":"a","first":0,"period":99999999999999999999999}`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postTickless(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	// period 0 (one-shot) and coalesceWindow 0 are explicitly legal.
	if rec := postTickless(t, wrap(`{"id":"a","first":0,"period":0}`), "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("period 0 must be legal: status %d body %s", rec.Code, rec.Body.String())
	}

	// 257 timers exceeds the timer-count bound.
	var many bytes.Buffer
	many.WriteString(`{"horizon":1000,"coalesceWindow":0,"maxSleep":1,"timers":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(ticklessTimerObj("t"+strconv.Itoa(i), int64(i%1000), 0))
	}
	many.WriteString(`]}`)
	assertErrorCode(t, postTickless(t, many.String(), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
}

func TestTicklessNoPartialResultsOnError(t *testing.T) {
	rec := postTickless(t, `{"horizon":10,"coalesceWindow":0,"maxSleep":1,"timers":[{"id":"a"`, "application/json")
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_json")
	body := rec.Body.String()
	if strings.Contains(body, "wakeups") || strings.Contains(body, "results") {
		t.Fatalf("error response leaked partial results: %s", body)
	}
}
