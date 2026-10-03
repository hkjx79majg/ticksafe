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

func postPeriodicSimulate(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, periodicSimulatePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func periodicSimulateOK(t *testing.T, body string) periodicSimulateResponse {
	t.Helper()
	rec := postPeriodicSimulate(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp periodicSimulateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func simTaskObj(id string, prio, exec, period, deadline, offset int64) string {
	return `{"id":"` + id + `","priority":` + strconv.FormatInt(prio, 10) +
		`,"execution":` + strconv.FormatInt(exec, 10) +
		`,"period":` + strconv.FormatInt(period, 10) +
		`,"deadline":` + strconv.FormatInt(deadline, 10) +
		`,"offset":` + strconv.FormatInt(offset, 10) + `}`
}

func checkSimTimeline(t *testing.T, got []periodicSimInterval, want []periodicSimInterval) {
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

func checkJob(t *testing.T, got periodicSimJobResult, job int, release, absDeadline, executed, remaining int64, completion *int64, status string) {
	t.Helper()
	if got.Job != job || got.Release != release || got.AbsoluteDeadline != absDeadline ||
		got.Executed != executed || got.Remaining != remaining || got.DeadlineStatus != status {
		t.Fatalf("job = %+v, want job %d release %d absDeadline %d executed %d remaining %d %s",
			got, job, release, absDeadline, executed, remaining, status)
	}
	if (got.Completion == nil) != (completion == nil) {
		t.Fatalf("job %d completion = %v, want %v", job, got.Completion, completion)
	}
	if completion != nil && *got.Completion != *completion {
		t.Fatalf("job %d completion = %d, want %d", job, *got.Completion, *completion)
	}
}

func int64ptr(v int64) *int64 { return &v }

func TestPeriodicSimulatePreemptionAndOffsets(t *testing.T) {
	// A (low): C=2,T=6,D=6,offset 0 -> releases 0,6. B (high): C=1,T=4,D=4,
	// offset 1 -> releases 1,5,9. B preempts A at 1; A resumes when B idles.
	resp := periodicSimulateOK(t, `{"horizon":12,"tasks":[
		{"id":"A","priority":1,"execution":2,"period":6,"deadline":6,"offset":0},
		{"id":"B","priority":0,"execution":1,"period":4,"deadline":4,"offset":1}
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	checkSimTimeline(t, resp.Timeline, []periodicSimInterval{
		{TaskID: "A", Job: 0, Start: 0, End: 1},
		{TaskID: "B", Job: 0, Start: 1, End: 2},
		{TaskID: "A", Job: 0, Start: 2, End: 3},
		{TaskID: "B", Job: 1, Start: 5, End: 6},
		{TaskID: "A", Job: 1, Start: 6, End: 8},
		{TaskID: "B", Job: 2, Start: 9, End: 10},
	})
	if len(resp.Results) != 2 || resp.Results[0].ID != "A" || resp.Results[1].ID != "B" {
		t.Fatalf("results must stay in input order: %+v", resp.Results)
	}
	a := resp.Results[0].Jobs
	if len(a) != 2 {
		t.Fatalf("A jobs = %+v", a)
	}
	checkJob(t, a[0], 0, 0, 6, 2, 0, int64ptr(3), "met")
	checkJob(t, a[1], 1, 6, 12, 2, 0, int64ptr(8), "met")
	b := resp.Results[1].Jobs
	if len(b) != 3 {
		t.Fatalf("B jobs = %+v", b)
	}
	checkJob(t, b[0], 0, 1, 5, 1, 0, int64ptr(2), "met")
	checkJob(t, b[1], 1, 5, 9, 1, 0, int64ptr(6), "met")
	checkJob(t, b[2], 2, 9, 13, 1, 0, int64ptr(10), "met")
}

func TestPeriodicSimulateLowerPriorityReleaseDoesNotPreempt(t *testing.T) {
	// B releases at 1 while A runs, but B is lower priority: A keeps the
	// processor until it completes at 5, then B runs.
	resp := periodicSimulateOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":0,"execution":5,"period":10,"deadline":10,"offset":0},
		{"id":"B","priority":1,"execution":1,"period":10,"deadline":10,"offset":1}
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	checkSimTimeline(t, resp.Timeline, []periodicSimInterval{
		{TaskID: "A", Job: 0, Start: 0, End: 5},
		{TaskID: "B", Job: 0, Start: 5, End: 6},
	})
	checkJob(t, resp.Results[0].Jobs[0], 0, 0, 10, 5, 0, int64ptr(5), "met")
	checkJob(t, resp.Results[1].Jobs[0], 0, 1, 11, 1, 0, int64ptr(6), "met")
}

func TestPeriodicSimulateLateCompletionIsMissed(t *testing.T) {
	// C=5 with D=4: the job finishes at 5, past its absolute deadline 4.
	resp := periodicSimulateOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":0,"execution":5,"period":10,"deadline":4,"offset":0}
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	checkSimTimeline(t, resp.Timeline, []periodicSimInterval{
		{TaskID: "A", Job: 0, Start: 0, End: 5},
	})
	checkJob(t, resp.Results[0].Jobs[0], 0, 0, 4, 5, 0, int64ptr(5), "missed")
}

func TestPeriodicSimulateSameTaskJobsRunInOrder(t *testing.T) {
	// C=3,T=2,D=2: each job overruns into the next release; the new job of
	// the same task never preempts the running one.
	resp := periodicSimulateOK(t, `{"horizon":6,"tasks":[
		{"id":"A","priority":0,"execution":3,"period":2,"deadline":2,"offset":0}
	]}`)
	if resp.Status != "horizon" {
		t.Fatalf("status = %q, want horizon", resp.Status)
	}
	checkSimTimeline(t, resp.Timeline, []periodicSimInterval{
		{TaskID: "A", Job: 0, Start: 0, End: 3},
		{TaskID: "A", Job: 1, Start: 3, End: 6},
	})
	jobs := resp.Results[0].Jobs
	if len(jobs) != 3 {
		t.Fatalf("jobs = %+v", jobs)
	}
	checkJob(t, jobs[0], 0, 0, 2, 3, 0, int64ptr(3), "missed")
	checkJob(t, jobs[1], 1, 2, 4, 3, 0, int64ptr(6), "missed")
	// Job 2 never runs; its absolute deadline 6 is not after the horizon.
	checkJob(t, jobs[2], 2, 4, 6, 0, 3, nil, "missed")
}

func TestPeriodicSimulatePendingAndMissedUnfinished(t *testing.T) {
	// A's deadline falls beyond the horizon (pending); B's falls inside it
	// (missed). Both are unfinished when the horizon is reached.
	resp := periodicSimulateOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":1,"execution":100,"period":1000,"deadline":500,"offset":0},
		{"id":"B","priority":0,"execution":100,"period":1000,"deadline":8,"offset":0}
	]}`)
	if resp.Status != "horizon" {
		t.Fatalf("status = %q, want horizon", resp.Status)
	}
	checkSimTimeline(t, resp.Timeline, []periodicSimInterval{
		{TaskID: "B", Job: 0, Start: 0, End: 10},
	})
	checkJob(t, resp.Results[0].Jobs[0], 0, 0, 500, 0, 100, nil, "pending")
	checkJob(t, resp.Results[1].Jobs[0], 0, 0, 8, 10, 90, nil, "missed")
}

func TestPeriodicSimulateCompletionExactlyAtHorizon(t *testing.T) {
	resp := periodicSimulateOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":0,"execution":10,"period":100,"deadline":10,"offset":0}
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	checkJob(t, resp.Results[0].Jobs[0], 0, 0, 10, 10, 0, int64ptr(10), "met")
}

func TestPeriodicSimulateIdleGapOmitted(t *testing.T) {
	// Offset 4 leaves [0,4) idle; the gap between jobs is idle too.
	resp := periodicSimulateOK(t, `{"horizon":12,"tasks":[
		{"id":"A","priority":0,"execution":1,"period":5,"deadline":5,"offset":4}
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	checkSimTimeline(t, resp.Timeline, []periodicSimInterval{
		{TaskID: "A", Job: 0, Start: 4, End: 5},
		{TaskID: "A", Job: 1, Start: 9, End: 10},
	})
}

func TestPeriodicSimulateJobCountLimit(t *testing.T) {
	// Exactly 100000 jobs is legal and completes exactly at the horizon.
	resp := periodicSimulateOK(t, `{"horizon":100000,"tasks":[
		{"id":"A","priority":0,"execution":1,"period":1,"deadline":1,"offset":0}
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	if got := len(resp.Results[0].Jobs); got != 100000 {
		t.Fatalf("jobs = %d, want 100000", got)
	}

	// One more release crosses the limit; the sum across tasks also counts.
	assertErrorCode(t, postPeriodicSimulate(t, `{"horizon":100001,"tasks":[
		{"id":"A","priority":0,"execution":1,"period":1,"deadline":1,"offset":0}
	]}`, "application/json"), http.StatusUnprocessableEntity, "validation_failed")
	assertErrorCode(t, postPeriodicSimulate(t, `{"horizon":100000,"tasks":[
		{"id":"A","priority":0,"execution":1,"period":1,"deadline":1,"offset":0},
		{"id":"B","priority":1,"execution":1,"period":1,"deadline":1,"offset":0}
	]}`, "application/json"), http.StatusUnprocessableEntity, "validation_failed")
}

func TestPeriodicSimulateDeterministic(t *testing.T) {
	body := `{"horizon":50,"tasks":[
		{"id":"A","priority":2,"execution":3,"period":7,"deadline":7,"offset":1},
		{"id":"B","priority":0,"execution":1,"period":2,"deadline":2,"offset":0},
		{"id":"C","priority":1,"execution":2,"period":5,"deadline":5,"offset":3}
	]}`
	first := postPeriodicSimulate(t, body, "application/json")
	second := postPeriodicSimulate(t, body, "application/json")
	if first.Code != http.StatusOK || first.Body.String() != second.Body.String() {
		t.Fatalf("same request produced different responses:\n%s\n%s",
			first.Body.String(), second.Body.String())
	}
}

func TestPeriodicSimulateMethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, periodicSimulatePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
}

func TestPeriodicSimulateUnsupportedMediaType(t *testing.T) {
	body := `{"horizon":10,"tasks":[]}`
	assertErrorCode(t, postPeriodicSimulate(t, body, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postPeriodicSimulate(t, body, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	rec := postPeriodicSimulate(t,
		`{"horizon":10,"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"offset":0}]}`,
		"application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestPeriodicSimulateInvalidJSON(t *testing.T) {
	cases := map[string]string{
		"empty":              "",
		"syntax error":       "{",
		"trailing token":     `{"horizon":10,"tasks":[]} garbage`,
		"unknown field":      `{"horizon":10,"tasks":[],"bogus":1}`,
		"unknown task field": `{"horizon":10,"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"offset":0,"release":0}]}`,
		"duplicate top key":  `{"horizon":10,"horizon":10,"tasks":[]}`,
		"duplicate task key": `{"horizon":10,"tasks":[{"id":"a","id":"b","priority":0,"execution":1,"period":2,"deadline":2,"offset":0}]}`,
		"malformed number":   `{"horizon":010,"tasks":[]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postPeriodicSimulate(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}

	padded := `{"horizon":10,"tasks":[` + simTaskObj("a", 0, 1, 2, 2, 0) +
		strings.Repeat(" ", maxBodyBytes) + `]}`
	assertErrorCode(t, postPeriodicSimulate(t, padded, "application/json"),
		http.StatusBadRequest, "invalid_json")
}

func TestPeriodicSimulateValidationFailures(t *testing.T) {
	good := `{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"offset":0}`
	wrap := func(task string) string { return `{"horizon":10,"tasks":[` + task + `]}` }
	cases := map[string]string{
		"json null body":        `null`,
		"json array body":       `[]`,
		"missing horizon":       `{"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"offset":0}]}`,
		"horizon null":          `{"horizon":null,"tasks":[]}`,
		"horizon zero":          `{"horizon":0,"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"offset":0}]}`,
		"horizon negative":      `{"horizon":-1,"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"offset":0}]}`,
		"horizon too large":     `{"horizon":1000000000001,"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"offset":0}]}`,
		"horizon wrong type":    `{"horizon":"10","tasks":[]}`,
		"missing tasks":         `{"horizon":10}`,
		"tasks null":            `{"horizon":10,"tasks":null}`,
		"empty tasks":           `{"horizon":10,"tasks":[]}`,
		"tasks not an array":    `{"horizon":10,"tasks":{}}`,
		"missing id":            wrap(`{"priority":0,"execution":1,"period":2,"deadline":2,"offset":0}`),
		"missing priority":      wrap(`{"id":"a","execution":1,"period":2,"deadline":2,"offset":0}`),
		"missing execution":     wrap(`{"id":"a","priority":0,"period":2,"deadline":2,"offset":0}`),
		"missing period":        wrap(`{"id":"a","priority":0,"execution":1,"deadline":2,"offset":0}`),
		"missing deadline":      wrap(`{"id":"a","priority":0,"execution":1,"period":2,"offset":0}`),
		"missing offset":        wrap(`{"id":"a","priority":0,"execution":1,"period":2,"deadline":2}`),
		"null field":            wrap(strings.Replace(good, `"offset":0`, `"offset":null`, 1)),
		"wrong field type":      wrap(strings.Replace(good, `"priority":0`, `"priority":"0"`, 1)),
		"offset wrong type":     wrap(strings.Replace(good, `"offset":0`, `"offset":0.5`, 1)),
		"empty id":              wrap(strings.Replace(good, `"id":"a"`, `"id":""`, 1)),
		"whitespace id":         wrap(strings.Replace(good, `"id":"a"`, `"id":"a b"`, 1)),
		"duplicate id":          `{"horizon":10,"tasks":[` + simTaskObj("a", 0, 1, 2, 2, 0) + "," + simTaskObj("a", 1, 1, 2, 2, 0) + `]}`,
		"priority negative":     wrap(strings.Replace(good, `"priority":0`, `"priority":-1`, 1)),
		"priority too large":    wrap(strings.Replace(good, `"priority":0`, `"priority":256`, 1)),
		"duplicate priority":    `{"horizon":10,"tasks":[` + simTaskObj("a", 0, 1, 2, 2, 0) + "," + simTaskObj("b", 0, 1, 3, 3, 0) + `]}`,
		"execution zero":        wrap(strings.Replace(good, `"execution":1`, `"execution":0`, 1)),
		"execution negative":    wrap(strings.Replace(good, `"execution":1`, `"execution":-1`, 1)),
		"execution too large":   wrap(strings.Replace(good, `"execution":1`, `"execution":1000000000001`, 1)),
		"period zero":           wrap(strings.Replace(good, `"period":2`, `"period":0`, 1)),
		"period too large":      wrap(strings.Replace(good, `"period":2`, `"period":1000000000001`, 1)),
		"deadline zero":         wrap(strings.Replace(good, `"deadline":2`, `"deadline":0`, 1)),
		"deadline too large":    wrap(strings.Replace(good, `"deadline":2`, `"deadline":1000000000001`, 1)),
		"deadline after period": wrap(strings.Replace(good, `"deadline":2`, `"deadline":3`, 1)),
		"offset negative":       wrap(strings.Replace(good, `"offset":0`, `"offset":-1`, 1)),
		"offset equals period":  wrap(strings.Replace(good, `"offset":0`, `"offset":2`, 1)),
		"offset above period":   wrap(strings.Replace(good, `"offset":0`, `"offset":3`, 1)),
		"offset past horizon":   `{"horizon":5,"tasks":[{"id":"a","priority":0,"execution":1,"period":100,"deadline":100,"offset":5}]}`,
		"number out of int64":   wrap(strings.Replace(good, `"period":2`, `"period":99999999999999999999999`, 1)),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postPeriodicSimulate(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	// 257 tasks exceeds the task-count bound.
	var many bytes.Buffer
	many.WriteString(`{"horizon":1000,"tasks":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(simTaskObj("t"+strconv.Itoa(i), int64(i%256), 1, 1000, 1000, 0))
	}
	many.WriteString(`]}`)
	assertErrorCode(t, postPeriodicSimulate(t, many.String(), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
}

func TestPeriodicSimulateNoPartialResultsOnError(t *testing.T) {
	rec := postPeriodicSimulate(t, `{"horizon":10,"tasks":[{"id":"a"`, "application/json")
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_json")
	body := rec.Body.String()
	if strings.Contains(body, "timeline") || strings.Contains(body, "results") {
		t.Fatalf("error response leaked partial results: %s", body)
	}
}
