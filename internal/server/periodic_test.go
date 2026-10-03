package server

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func postPeriodic(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, periodicAnalyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func periodicOK(t *testing.T, body string) periodicAnalyzeResponse {
	t.Helper()
	rec := postPeriodic(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp periodicAnalyzeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v: %s", err, rec.Body.String())
	}
	return resp
}

func periodicTaskObj(id string, prio, exec, period, deadline int64) string {
	return `{"id":"` + id + `","priority":` + strconv.FormatInt(prio, 10) +
		`,"execution":` + strconv.FormatInt(exec, 10) +
		`,"period":` + strconv.FormatInt(period, 10) +
		`,"deadline":` + strconv.FormatInt(deadline, 10) + `}`
}

func TestPeriodicFixedPointExample(t *testing.T) {
	// Three tasks with interference from two higher-priority tasks; the lowest
	// converges exactly on its deadline.
	body := `{"tasks":[` +
		periodicTaskObj("low", 2, 3, 10, 10) + "," +
		periodicTaskObj("hi", 0, 1, 4, 4) + "," +
		periodicTaskObj("mid", 1, 2, 6, 6) +
		"]}"
	resp := periodicOK(t, body)
	if !resp.Schedulable {
		t.Fatalf("schedulable = false, want true: %+v", resp)
	}
	want := []periodicTaskResult{
		{ID: "low", ResponseTime: int64Ptr(10), DeadlineStatus: "met"},
		{ID: "hi", ResponseTime: int64Ptr(1), DeadlineStatus: "met"},
		{ID: "mid", ResponseTime: int64Ptr(3), DeadlineStatus: "met"},
	}
	if len(resp.Results) != len(want) {
		t.Fatalf("results = %+v", resp.Results)
	}
	for i := range want {
		if !periodicResultEqual(resp.Results[i], want[i]) {
			t.Fatalf("results[%d] = %+v, want %+v", i, resp.Results[i], want[i])
		}
	}
}

func TestPeriodicMissedDeadlineAndSchedulable(t *testing.T) {
	// High: C=3/T=5; low: C=3/T=8,D=8. Low iterates 3 -> 6 -> 9 > 8.
	resp := periodicOK(t, `{"tasks":[`+
		periodicTaskObj("low", 1, 3, 8, 8)+","+
		periodicTaskObj("hi", 0, 3, 5, 5)+
		"]}")
	if resp.Schedulable {
		t.Fatalf("schedulable = true, want false")
	}
	if resp.Results[0].ResponseTime != nil || resp.Results[0].DeadlineStatus != "missed" {
		t.Fatalf("low result = %+v, want null/missed", resp.Results[0])
	}
	if resp.Results[1].ResponseTime == nil || *resp.Results[1].ResponseTime != 3 ||
		resp.Results[1].DeadlineStatus != "met" {
		t.Fatalf("hi result = %+v, want 3/met", resp.Results[1])
	}
}

func TestPeriodicResponseTimeNullWhenExecutionExceedsDeadline(t *testing.T) {
	resp := periodicOK(t, `{"tasks":[`+periodicTaskObj("a", 0, 10, 10, 5)+"]}")
	if resp.Schedulable {
		t.Fatalf("schedulable = true, want false")
	}
	r := resp.Results[0]
	if r.ResponseTime != nil || r.DeadlineStatus != "missed" {
		t.Fatalf("result = %+v, want null/missed", r)
	}
}

func TestPeriodicDeterministic(t *testing.T) {
	body := `{"tasks":[` +
		periodicTaskObj("a", 3, 7, 30, 20) + "," +
		periodicTaskObj("b", 0, 2, 7, 7) + "," +
		periodicTaskObj("c", 1, 4, 13, 13) + "," +
		periodicTaskObj("d", 2, 1, 5, 5) + "]}"
	first := periodicOK(t, body)
	second := periodicOK(t, body)
	firstBytes, _ := json.Marshal(first)
	secondBytes, _ := json.Marshal(second)
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("non-deterministic response:\n%s\n%s", firstBytes, secondBytes)
	}
}

func TestPeriodicHugeInterferenceTreatedAsMissed(t *testing.T) {
	// High task C=1e12,T=1 makes higher-priority utilization 1e12; the low
	// task can never converge. This must surface as a 200 with null
	// responseTime, never a wrap or a 5xx response.
	body := `{"tasks":[` +
		periodicTaskObj("low", 1, 1, 1000000000000, 1000000000000) + "," +
		periodicTaskObj("hi", 0, 1000000000000, 1, 1) + "]}"
	rec := postPeriodic(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp periodicAnalyzeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if resp.Schedulable {
		t.Fatalf("schedulable = true, want false")
	}
	for _, r := range resp.Results {
		if r.ResponseTime != nil {
			t.Fatalf("task %s responseTime = %d, want null", r.ID, *r.ResponseTime)
		}
	}
}

// TestRTAIterationNoWrap exercises the 128-bit widened arithmetic directly:
// ceil(1e12/1)*1e12 = 1e24, far beyond int64, must report overflow.
func TestRTAIterationNoWrap(t *testing.T) {
	if _, overflow := rtaIteration(1_000_000_000_000, 1, []periodicTask{
		{execution: 1_000_000_000_000, period: 1},
	}); !overflow {
		t.Fatal("expected overflow for 1e24 interference, got a wrapped value")
	}
	// Value fitting in int64 still returns normally.
	if got, overflow := rtaIteration(6, 3, []periodicTask{
		{execution: 1, period: 4},
		{execution: 2, period: 6},
	}); overflow || got != 7 {
		t.Fatalf("iteration = (%d, %v), want (7, false)", got, overflow)
	}
}

func TestPeriodicFullUtilizationTerminatesAndMisses(t *testing.T) {
	// High task has U = C/T = 1; a naive fixed point would walk one tick at
	// a time for 1e12 iterations. It must terminate promptly as missed
	// because f(w) > w for every w when higher-priority utilization is >= 1.
	body := `{"tasks":[` +
		periodicTaskObj("low", 1, 1, 1000000000000, 1000000000000) + "," +
		periodicTaskObj("hi", 0, 1, 1, 1) + "]}"
	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, periodicAnalyzePath, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		Handler().ServeHTTP(rec, r)
		recCh <- rec
	}()
	select {
	case rec := <-recCh:
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
		}
		var resp periodicAnalyzeResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("response is not JSON: %v", err)
		}
		if resp.Schedulable {
			t.Fatalf("schedulable = true, want false")
		}
		if resp.Results[1].ResponseTime == nil || *resp.Results[1].ResponseTime != 1 {
			t.Fatalf("hi result = %+v, want responseTime 1", resp.Results[1])
		}
		if resp.Results[0].ResponseTime != nil {
			t.Fatalf("low result = %+v, want null", resp.Results[0])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("analysis with higher-priority utilization of 1 did not terminate promptly")
	}
}

func TestPeriodicBoundaryValuesAndUnicode(t *testing.T) {
	body := `{"tasks":[` +
		periodicTaskObj("任務-α", 255, 1000000000000, 1000000000000, 1000000000000) +
		"]}"
	resp := periodicOK(t, body)
	r := resp.Results[0]
	if r.ResponseTime == nil || *r.ResponseTime != 1000000000000 || r.DeadlineStatus != "met" {
		t.Fatalf("result = %+v", r)
	}
}

func TestPeriodicTaskCountBounds(t *testing.T) {
	var many bytes.Buffer
	many.WriteString(`{"tasks":[`)
	for i := 0; i < 256; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(periodicTaskObj("t"+strconv.Itoa(i), int64(i), 1, 1000000, 1000000))
	}
	many.WriteString(`]}`)
	if rec := postPeriodic(t, many.String(), "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("256 tasks: status = %d, body: %s", rec.Code, rec.Body.String())
	}

	many.Reset()
	many.WriteString(`{"tasks":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(periodicTaskObj("t"+strconv.Itoa(i), int64(i%256), 1, 1000000, 1000000))
	}
	many.WriteString(`]}`)
	assertErrorCode(t, postPeriodic(t, many.String(), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
}

func TestPeriodicInvalidJSON(t *testing.T) {
	cases := map[string]string{
		"empty":              "",
		"syntax error":       "{",
		"trailing token":     `{"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2}]} garbage`,
		"unknown field":      `{"tasks":[],"bogus":1}`,
		"unknown task field": `{"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"x":1}]}`,
		"duplicate top key":  `{"tasks":[],"tasks":[]}`,
		"duplicate task key": `{"tasks":[{"id":"a","id":"b","priority":0,"execution":1,"period":2,"deadline":2}]}`,
		"malformed number":   `{"tasks":[{"id":"a","priority":0,"execution":01,"period":2,"deadline":2}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postPeriodic(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}

	// A body over 1 MiB is invalid_json even though its tokens are valid.
	big := strings.Repeat(" ", (1<<20)+1) + `{"tasks":[]}`
	assertErrorCode(t, postPeriodic(t, big, "application/json"),
		http.StatusBadRequest, "invalid_json")
}

func TestPeriodicValidationFailures(t *testing.T) {
	obj := func(overrides string) string {
		return `{"tasks":[{` + overrides + `}]}`
	}
	cases := map[string]string{
		"json null body":        `null`,
		"json array body":       `[]`,
		"missing tasks":         `{}`,
		"tasks null":            `{"tasks":null}`,
		"tasks not array":       `{"tasks":{}}`,
		"empty tasks":           `{"tasks":[]}`,
		"empty id":              obj(`"id":"","priority":0,"execution":1,"period":2,"deadline":2`),
		"whitespace id":         obj(`"id":"a b","priority":0,"execution":1,"period":2,"deadline":2`),
		"duplicate id":          `{"tasks":[` + periodicTaskObj("a", 0, 1, 2, 2) + "," + periodicTaskObj("a", 1, 1, 2, 2) + "]}",
		"duplicate priority":    `{"tasks":[` + periodicTaskObj("a", 0, 1, 2, 2) + "," + periodicTaskObj("b", 0, 1, 2, 2) + "]}",
		"missing priority":      obj(`"id":"a","execution":1,"period":2,"deadline":2`),
		"null priority":         obj(`"id":"a","priority":null,"execution":1,"period":2,"deadline":2`),
		"priority negative":     obj(`"id":"a","priority":-1,"execution":1,"period":2,"deadline":2`),
		"priority too large":    obj(`"id":"a","priority":256,"execution":1,"period":2,"deadline":2`),
		"missing execution":     obj(`"id":"a","priority":0,"period":2,"deadline":2`),
		"execution zero":        obj(`"id":"a","priority":0,"execution":0,"period":2,"deadline":2`),
		"execution negative":    obj(`"id":"a","priority":0,"execution":-1,"period":2,"deadline":2`),
		"execution too large":   obj(`"id":"a","priority":0,"execution":1000000000001,"period":1000000000001,"deadline":1000000000001`),
		"missing period":        obj(`"id":"a","priority":0,"execution":1,"deadline":2`),
		"period zero":           obj(`"id":"a","priority":0,"execution":1,"period":0,"deadline":0`),
		"period too large":      obj(`"id":"a","priority":0,"execution":1,"period":1000000000001,"deadline":1000000000000`),
		"missing deadline":      obj(`"id":"a","priority":0,"execution":1,"period":2`),
		"deadline zero":         obj(`"id":"a","priority":0,"execution":1,"period":2,"deadline":0`),
		"deadline after period": obj(`"id":"a","priority":0,"execution":1,"period":2,"deadline":3`),
		"wrong field type":      obj(`"id":"a","priority":"0","execution":1,"period":2,"deadline":2`),
		"float field":           obj(`"id":"a","priority":0,"execution":1.5,"period":2,"deadline":2`),
		"number out of int64":   obj(`"id":"a","priority":0,"execution":999999999999999999999999999999,"period":2,"deadline":2`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postPeriodic(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	// A 65-rune id fails; a 64-rune id with a multibyte rune succeeds.
	long65 := strings.Repeat("x", 65)
	assertErrorCode(t, postPeriodic(t,
		`{"tasks":[{"id":"`+long65+`","priority":0,"execution":1,"period":2,"deadline":2}]}`,
		"application/json"), http.StatusUnprocessableEntity, "validation_failed")
	id64 := strings.Repeat("z", 63) + "é"
	periodicOK(t, `{"tasks":[{"id":"`+id64+`","priority":0,"execution":1,"period":2,"deadline":2}]}`)
}

func TestPeriodicMethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, periodicAnalyzePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
}

func TestPeriodicUnsupportedMediaType(t *testing.T) {
	body := `{"tasks":[]}`
	assertErrorCode(t, postPeriodic(t, body, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postPeriodic(t, body, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	// Parameters on application/json are accepted.
	rec := postPeriodic(t,
		`{"tasks":[`+periodicTaskObj("a", 0, 1, 2, 2)+`]}`,
		"application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestPeriodicNoPartialResultsOnError(t *testing.T) {
	rec := postPeriodic(t, `{"tasks":[{"id":"a"`, "application/json")
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_json")
	if strings.Contains(rec.Body.String(), "responseTime") ||
		strings.Contains(rec.Body.String(), "schedulable") {
		t.Fatalf("error response leaked partial results: %s", rec.Body.String())
	}
}

func int64Ptr(v int64) *int64 { return &v }

// TestAnalyzePeriodicDifferential compares the fixed-point analysis against
// an independent tick-by-tick critical-instant simulation (synchronous
// release at t=0), comparing the first-job completion of every task with its
// deadline verdict.
func TestAnalyzePeriodicDifferential(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	for iter := 0; iter < 3000; iter++ {
		n := 1 + rng.Intn(6)
		tasks := make([]periodicTask, n)
		prios := rng.Perm(256)
		maxDeadline := int64(0)
		for i := 0; i < n; i++ {
			c := int64(1 + rng.Intn(6))
			period := c + int64(rng.Intn(12))
			deadline := int64(1 + rng.Intn(int(period)))
			if deadline < c && rng.Intn(2) == 0 {
				deadline = c
			}
			tasks[i] = periodicTask{
				index:     i,
				id:        "t" + strconv.Itoa(i),
				priority:  int64(prios[i]),
				execution: c,
				period:    period,
				deadline:  deadline,
			}
			if deadline > maxDeadline {
				maxDeadline = deadline
			}
		}

		got := analyzePeriodic(tasks)

		// Tick-by-tick reference: jobs release at 0, T, 2T, ...; run the
		// highest-priority ready job each tick.
		remaining := make([]int64, n)
		backlog := make([]int64, n)
		nextRelease := make([]int64, n)
		completion := make([]*int64, n)
		for now := int64(0); now < maxDeadline; now++ {
			for i := range tasks {
				for nextRelease[i] <= now {
					backlog[i]++
					nextRelease[i] += tasks[i].period
				}
			}
			best := -1
			for i := range tasks {
				if remaining[i] == 0 {
					if backlog[i] == 0 {
						continue
					}
					backlog[i]--
					remaining[i] = tasks[i].execution
				}
				if best == -1 || tasks[i].priority < tasks[best].priority {
					best = i
				}
			}
			if best == -1 {
				continue
			}
			remaining[best]--
			if remaining[best] == 0 && completion[best] == nil {
				done := now + 1
				completion[best] = &done
			}
		}

		for i, tk := range tasks {
			var want *int64
			if completion[i] != nil && *completion[i] <= tk.deadline {
				v := *completion[i]
				want = &v
			}
			g := got.Results[i]
			if (g.ResponseTime == nil) != (want == nil) ||
				(g.ResponseTime != nil && *g.ResponseTime != *want) {
				t.Fatalf("iter %d (seed %d) task %d (%+v): got response %v, want %v (sim completion %v)",
					iter, seed, i, tk, g.ResponseTime, want, completion[i])
			}
			wantStatus := "met"
			if want == nil {
				wantStatus = "missed"
			}
			if g.DeadlineStatus != wantStatus {
				t.Fatalf("iter %d (seed %d) task %d: status %q want %q",
					iter, seed, i, g.DeadlineStatus, wantStatus)
			}
		}
		allMet := true
		for _, r := range got.Results {
			if r.DeadlineStatus != "met" {
				allMet = false
			}
		}
		if got.Schedulable != allMet {
			t.Fatalf("iter %d (seed %d): schedulable=%v but statuses %+v", iter, seed, got.Schedulable, got.Results)
		}
	}
}

func periodicResultEqual(a, b periodicTaskResult) bool {
	if a.ID != b.ID || a.DeadlineStatus != b.DeadlineStatus {
		return false
	}
	if (a.ResponseTime == nil) != (b.ResponseTime == nil) {
		return false
	}
	return a.ResponseTime == nil || *a.ResponseTime == *b.ResponseTime
}
