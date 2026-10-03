package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func postPeriodicSim(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, periodicSimPath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func periodicSimOK(t *testing.T, body string) periodicSimResponse {
	t.Helper()
	rec := postPeriodicSim(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp periodicSimResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func periodicSimTaskObj(id string, prio, exec, period, deadline int64, offset *int64) string {
	s := `{"id":"` + id + `","priority":` + strconv.FormatInt(prio, 10) +
		`,"execution":` + strconv.FormatInt(exec, 10) +
		`,"period":` + strconv.FormatInt(period, 10) +
		`,"deadline":` + strconv.FormatInt(deadline, 10)
	if offset != nil {
		s += `,"offset":` + strconv.FormatInt(*offset, 10)
	}
	return s + "}"
}

func intervals(ivs []periodicTimelineInterval) string {
	var b strings.Builder
	for _, iv := range ivs {
		fmt.Fprintf(&b, "%s#%d[%d,%d] ", iv.TaskID, iv.Job, iv.Start, iv.End)
	}
	return b.String()
}

func TestPeriodicSimSingleTask(t *testing.T) {
	// C=2,T=5,D=5, horizon=12: releases at 0,5,10; every job meets.
	resp := periodicSimOK(t, `{"horizon":12,"tasks":[
		{"id":"A","priority":0,"execution":2,"period":5,"deadline":5}
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	want := []periodicTimelineInterval{
		{TaskID: "A", Job: 0, Start: 0, End: 2},
		{TaskID: "A", Job: 1, Start: 5, End: 7},
		{TaskID: "A", Job: 2, Start: 10, End: 12},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s", intervals(resp.Timeline))
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline = %s", intervals(resp.Timeline))
		}
	}
	if len(resp.Results) != 1 || resp.Results[0].ID != "A" || len(resp.Results[0].Jobs) != 3 {
		t.Fatalf("results = %+v", resp.Results)
	}
	j0 := resp.Results[0].Jobs[0]
	if j0.Job != 0 || j0.Release != 0 || j0.AbsoluteDeadline != 5 ||
		j0.Executed != 2 || j0.Remaining != 0 || j0.Completion == nil || *j0.Completion != 2 ||
		j0.DeadlineStatus != "met" {
		t.Fatalf("job0 = %+v", j0)
	}
	j2 := resp.Results[0].Jobs[2]
	if j2.Job != 2 || j2.Release != 10 || j2.AbsoluteDeadline != 15 ||
		j0.Remaining != 0 || j2.Completion == nil || *j2.Completion != 12 ||
		j2.DeadlineStatus != "met" {
		t.Fatalf("job2 = %+v", j2)
	}
}

func TestPeriodicSimPreemptionSplitsIntervals(t *testing.T) {
	// A low C=3/T=10/D=10 releases at 0; B high C=1/T=4/D=4 with offset 2
	// releases at 2 and 6. B0 preempts A0 at t=2:
	// A[0,2], B0[2,3], A[3,4] (completes), idle, B1[6,7].
	off := int64(2)
	resp := periodicSimOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":1,"execution":3,"period":10,"deadline":10},
		`+periodicSimTaskObj("B", 0, 1, 4, 4, &off)+`
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q", resp.Status)
	}
	want := []periodicTimelineInterval{
		{TaskID: "A", Job: 0, Start: 0, End: 2},
		{TaskID: "B", Job: 0, Start: 2, End: 3},
		{TaskID: "A", Job: 0, Start: 3, End: 4},
		{TaskID: "B", Job: 1, Start: 6, End: 7},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s", intervals(resp.Timeline))
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline = %s", intervals(resp.Timeline))
		}
	}
	byID := map[string]periodicTaskSimResult{}
	for _, r := range resp.Results {
		byID[r.ID] = r
	}
	if len(byID["A"].Jobs) != 1 || byID["A"].Jobs[0].Executed != 3 ||
		byID["A"].Jobs[0].Remaining != 0 || *byID["A"].Jobs[0].Completion != 4 ||
		byID["A"].Jobs[0].DeadlineStatus != "met" {
		t.Fatalf("A = %+v", byID["A"])
	}
	if len(byID["B"].Jobs) != 2 {
		t.Fatalf("B jobs = %+v", byID["B"])
	}
	for _, j := range byID["B"].Jobs {
		if j.Completion == nil || j.DeadlineStatus != "met" {
			t.Fatalf("B job = %+v", j)
		}
	}
}

func TestPeriodicSimSameTaskJobsDoNotPreempt(t *testing.T) {
	// C=6 > T=D=4, horizon 10. J0 [0,6] late; J1 released at 4 waits and runs
	// [6,10] unfinished (deadline 8 -> missed); J2 released at 8 never runs
	// (deadline 12 > horizon -> pending).
	resp := periodicSimOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":0,"execution":6,"period":4,"deadline":4}
	]}`)
	if resp.Status != "horizon" {
		t.Fatalf("status = %q, want horizon", resp.Status)
	}
	want := []periodicTimelineInterval{
		{TaskID: "A", Job: 0, Start: 0, End: 6},
		{TaskID: "A", Job: 1, Start: 6, End: 10},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s", intervals(resp.Timeline))
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline = %s", intervals(resp.Timeline))
		}
	}
	jobs := resp.Results[0].Jobs
	if len(jobs) != 3 {
		t.Fatalf("jobs = %+v", jobs)
	}
	if jobs[0].Completion == nil || *jobs[0].Completion != 6 || jobs[0].DeadlineStatus != "missed" {
		t.Fatalf("j0 = %+v", jobs[0])
	}
	if jobs[1].Completion != nil || jobs[1].Executed != 4 || jobs[1].Remaining != 2 ||
		jobs[1].DeadlineStatus != "missed" {
		t.Fatalf("j1 = %+v", jobs[1])
	}
	if jobs[2].Completion != nil || jobs[2].Executed != 0 || jobs[2].Remaining != 6 ||
		jobs[2].AbsoluteDeadline != 12 || jobs[2].DeadlineStatus != "pending" {
		t.Fatalf("j2 = %+v", jobs[2])
	}
}

func TestPeriodicSimOffsetShiftsReleases(t *testing.T) {
	// offset=3, C=2,T=5: releases at 3,8 (13 excluded by horizon 12).
	off := int64(3)
	resp := periodicSimOK(t, `{"horizon":12,"tasks":[
		`+periodicSimTaskObj("A", 0, 2, 5, 5, &off)+`
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q", resp.Status)
	}
	want := []periodicTimelineInterval{
		{TaskID: "A", Job: 0, Start: 3, End: 5},
		{TaskID: "A", Job: 1, Start: 8, End: 10},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s", intervals(resp.Timeline))
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline = %s", intervals(resp.Timeline))
		}
	}
	if jobs := resp.Results[0].Jobs; len(jobs) != 2 ||
		jobs[0].Release != 3 || jobs[0].AbsoluteDeadline != 8 ||
		jobs[1].Release != 8 || jobs[1].AbsoluteDeadline != 13 {
		t.Fatalf("jobs = %+v", jobs)
	}
}

func TestPeriodicSimOverdueJobKeepsRunningAndMeansHorizon(t *testing.T) {
	// A C=6 low, B C=4 high, both T=D=8, horizon 16:
	// B0[0,4] A0[4,8] B1[8,12] A0[12,14] (late, deadline 8);
	// A1 released 8 runs [14,16] unfinished; its deadline 16 is at the
	// horizon -> missed.
	resp := periodicSimOK(t, `{"horizon":16,"tasks":[
		{"id":"A","priority":1,"execution":6,"period":8,"deadline":8},
		{"id":"B","priority":0,"execution":4,"period":8,"deadline":8}
	]}`)
	if resp.Status != "horizon" {
		t.Fatalf("status = %q, want horizon", resp.Status)
	}
	want := []periodicTimelineInterval{
		{TaskID: "B", Job: 0, Start: 0, End: 4},
		{TaskID: "A", Job: 0, Start: 4, End: 8},
		{TaskID: "B", Job: 1, Start: 8, End: 12},
		{TaskID: "A", Job: 0, Start: 12, End: 14},
		{TaskID: "A", Job: 1, Start: 14, End: 16},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s", intervals(resp.Timeline))
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline = %s", intervals(resp.Timeline))
		}
	}
	byID := map[string]periodicTaskSimResult{}
	for _, r := range resp.Results {
		byID[r.ID] = r
	}
	a0 := byID["A"].Jobs[0]
	if a0.Completion == nil || *a0.Completion != 14 || a0.DeadlineStatus != "missed" {
		t.Fatalf("A0 = %+v", a0)
	}
	a1 := byID["A"].Jobs[1]
	if a1.Completion != nil || a1.Executed != 2 || a1.Remaining != 4 ||
		a1.AbsoluteDeadline != 16 || a1.DeadlineStatus != "missed" {
		t.Fatalf("A1 = %+v", a1)
	}
}

func TestPeriodicSimFinishingExactlyAtHorizonIsCompleted(t *testing.T) {
	resp := periodicSimOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":0,"execution":5,"period":5,"deadline":5}
	]}`)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed", resp.Status)
	}
	j1 := resp.Results[0].Jobs[1]
	if j1.Completion == nil || *j1.Completion != 10 {
		t.Fatalf("j1 = %+v, want completion 10", j1)
	}
}

func TestPeriodicSimReleaseExactlyAtHorizonIsExcluded(t *testing.T) {
	// offset 0, T=5, horizon 10: releases 0 and 5 only.
	resp := periodicSimOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":0,"execution":1,"period":5,"deadline":5}
	]}`)
	if jobs := resp.Results[0].Jobs; len(jobs) != 2 || jobs[1].Release != 5 {
		t.Fatalf("jobs = %+v", jobs)
	}
}

func TestPeriodicSimJobCap(t *testing.T) {
	// One task with period 1 releases exactly `horizon` jobs.
	body := func(h int64) string {
		return `{"horizon":` + strconv.FormatInt(h, 10) + `,"tasks":[
			{"id":"A","priority":0,"execution":1,"period":1,"deadline":1}
		]}`
	}
	assertErrorCode(t, postPeriodicSim(t, body(100_001), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
	resp := periodicSimOK(t, body(100_000))
	if len(resp.Results[0].Jobs) != 100_000 {
		t.Fatalf("jobs = %d, want 100000", len(resp.Results[0].Jobs))
	}

	// The cap counts jobs across all tasks: 2*50001 = 100002.
	resp2 := postPeriodicSim(t, `{"horizon":100001,"tasks":[
		{"id":"A","priority":0,"execution":1,"period":2,"deadline":2},
		{"id":"B","priority":1,"execution":1,"period":2,"deadline":2}
	]}`, "application/json")
	assertErrorCode(t, resp2, http.StatusUnprocessableEntity, "validation_failed")
}

func TestPeriodicSimPriorityWinsOverInputOrder(t *testing.T) {
	// Priorities are unique across tasks: even though A is listed first and
	// both jobs release at 0, the higher-priority B runs first.
	resp := periodicSimOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":1,"execution":4,"period":10,"deadline":10},
		{"id":"B","priority":0,"execution":3,"period":10,"deadline":10}
	]}`)
	want := []periodicTimelineInterval{
		{TaskID: "B", Job: 0, Start: 0, End: 3},
		{TaskID: "A", Job: 0, Start: 3, End: 7},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %s", intervals(resp.Timeline))
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline = %s", intervals(resp.Timeline))
		}
	}
}

func TestPeriodicSimTimelineOmitsIdleAndMergesAcrossReleases(t *testing.T) {
	// offset=8, C=2,T=20, horizon 10: nothing ready for [0,8); the single
	// job runs [8,10], finishing exactly at the horizon.
	off := int64(8)
	resp := periodicSimOK(t, `{"horizon":10,"tasks":[
		`+periodicSimTaskObj("A", 0, 2, 20, 20, &off)+`
	]}`)
	if resp.Status != "completed" || len(resp.Timeline) != 1 ||
		resp.Timeline[0] != (periodicTimelineInterval{TaskID: "A", Job: 0, Start: 8, End: 10}) {
		t.Fatalf("status=%q timeline=%s", resp.Status, intervals(resp.Timeline))
	}
}

func TestPeriodicSimOffsetBoundaries(t *testing.T) {
	// horizon=2, period=3: offset 1 is legal (< period, < horizon); offset 2
	// equals the horizon and fails.
	off := int64(1)
	resp := periodicSimOK(t, `{"horizon":2,"tasks":[
		`+periodicSimTaskObj("A", 0, 1, 3, 3, &off)+`
	]}`)
	if jobs := resp.Results[0].Jobs; len(jobs) != 1 || jobs[0].Release != 1 {
		t.Fatalf("jobs = %+v", jobs)
	}
	bad := postPeriodicSim(t, `{"horizon":2,"tasks":[
		`+periodicSimTaskObj("A", 0, 1, 3, 3, func() *int64 { v := int64(2); return &v }())+`
	]}`, "application/json")
	assertErrorCode(t, bad, http.StatusUnprocessableEntity, "validation_failed")
}

func TestPeriodicSimPerformanceAtJobCap(t *testing.T) {
	// 100000 jobs: the simulation must be event-driven, not tick-by-tick.
	body := `{"horizon":100000,"tasks":[
		{"id":"A","priority":0,"execution":1,"period":1,"deadline":1}
	]}`
	start := time.Now()
	rec := postPeriodicSim(t, body, "application/json")
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("simulation of 100000 jobs took %s", elapsed)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}

	// Interleaved two-task case: 25000 + 25000 = 50000 jobs with preemption
	// between the two priority levels.
	body = `{"horizon":50000,"tasks":[
		{"id":"A","priority":1,"execution":1,"period":2,"deadline":2},
		{"id":"B","priority":0,"execution":1,"period":2,"deadline":2}
	]}`
	rec = postPeriodicSim(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestPeriodicSimLateReleasedJobsAppearInResults(t *testing.T) {
	// High-priority A runs back-to-back through the whole horizon; B's
	// releases at 0,4,8 never execute. B0 deadline 4 and B1 deadline 8 are
	// missed; B2's deadline 12 is beyond the horizon -> pending.
	resp := periodicSimOK(t, `{"horizon":10,"tasks":[
		{"id":"A","priority":0,"execution":5,"period":5,"deadline":5},
		{"id":"B","priority":1,"execution":1,"period":4,"deadline":4}
	]}`)
	if resp.Status != "horizon" {
		t.Fatalf("status = %q, want horizon", resp.Status)
	}
	b := resp.Results[1]
	if b.ID != "B" || len(b.Jobs) != 3 {
		t.Fatalf("B results = %+v", b)
	}
	wantStatus := []string{"missed", "missed", "pending"}
	for k, j := range b.Jobs {
		if j.Executed != 0 || j.Remaining != 1 || j.Completion != nil ||
			j.DeadlineStatus != wantStatus[k] {
			t.Fatalf("starved job %d = %+v, want %s", k, j, wantStatus[k])
		}
	}
}

func TestPeriodicSimDeterministic(t *testing.T) {
	body := `{"horizon":37,"tasks":[
		{"id":"A","priority":2,"execution":3,"period":7,"deadline":7},
		{"id":"B","priority":0,"execution":2,"period":5,"deadline":5},
		{"id":"C","priority":1,"execution":1,"period":3,"deadline":3,"offset":1}
	]}`
	first := periodicSimOK(t, body)
	second := periodicSimOK(t, body)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic responses:\n%s\n%s", a, b)
	}
}

func TestPeriodicSimMethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, periodicSimPath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
}

func TestPeriodicSimUnsupportedMediaType(t *testing.T) {
	body := `{"horizon":1,"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2}]}`
	assertErrorCode(t, postPeriodicSim(t, body, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postPeriodicSim(t, body, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	rec := postPeriodicSim(t, body, "application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestPeriodicSimInvalidJSON(t *testing.T) {
	cases := map[string]string{
		"empty":                "",
		"syntax error":         "{",
		"trailing token":       `{"horizon":1,"tasks":[]} garbage`,
		"unknown top field":    `{"horizon":1,"tasks":[],"bogus":1}`,
		"unknown task field":   `{"horizon":1,"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"release":0}]}`,
		"duplicate top key":    `{"horizon":1,"horizon":2,"tasks":[]}`,
		"duplicate task key":   `{"horizon":1,"tasks":[{"id":"a","id":"b","priority":0,"execution":1,"period":2,"deadline":2}]}`,
		"duplicate offset key": `{"horizon":1,"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"offset":0,"offset":1}]}`,
		"malformed number":     `{"horizon":1,"tasks":[{"id":"a","priority":0,"execution":01,"period":2,"deadline":2}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postPeriodicSim(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}
	objs := periodicSimTaskObj("a", 0, 1, 2, 2, nil)
	padded := `{"horizon":100,"tasks":[` + objs + strings.Repeat(" ", maxBodyBytes) + `]}`
	assertErrorCode(t, postPeriodicSim(t, padded, "application/json"),
		http.StatusBadRequest, "invalid_json")
}

func TestPeriodicSimValidationFailures(t *testing.T) {
	good := `{"id":"a","priority":0,"execution":1,"period":2,"deadline":2}`
	wrap := func(task string) string { return `{"horizon":10,"tasks":[` + task + `]}` }
	cases := map[string]string{
		"json null body":       `null`,
		"json array body":      `[]`,
		"missing horizon":      `{"tasks":[]}`,
		"horizon null":         `{"horizon":null,"tasks":[]}`,
		"horizon zero":         `{"horizon":0,"tasks":[]}`,
		"horizon negative":     `{"horizon":-1,"tasks":[]}`,
		"horizon too large":    `{"horizon":1000000000001,"tasks":[]}`,
		"missing tasks":        `{"horizon":10}`,
		"tasks null":           `{"horizon":10,"tasks":null}`,
		"empty tasks":          `{"horizon":10,"tasks":[]}`,
		"tasks not an array":   `{"horizon":10,"tasks":{}}`,
		"missing id":           `{"horizon":10,"tasks":[{"priority":0,"execution":1,"period":2,"deadline":2}]}`,
		"missing priority":     `{"horizon":10,"tasks":[{"id":"a","execution":1,"period":2,"deadline":2}]}`,
		"missing execution":    `{"horizon":10,"tasks":[{"id":"a","priority":0,"period":2,"deadline":2}]}`,
		"missing period":       `{"horizon":10,"tasks":[{"id":"a","priority":0,"execution":1,"deadline":2}]}`,
		"missing deadline":     `{"horizon":10,"tasks":[{"id":"a","priority":0,"execution":1,"period":2}]}`,
		"null priority":        wrap(`{"id":"a","priority":null,"execution":1,"period":2,"deadline":2}`),
		"null offset":          wrap(strings.Replace(good, `"deadline":2`, `"deadline":2,"offset":null`, 1)),
		"offset wrong type":    wrap(strings.Replace(good, `"deadline":2`, `"deadline":2,"offset":"0"`, 1)),
		"offset negative":      wrap(strings.Replace(good, `"deadline":2`, `"deadline":2,"offset":-1`, 1)),
		"offset equals period": wrap(strings.Replace(good, `"deadline":2`, `"deadline":2,"offset":2`, 1)),
		"offset above period":  wrap(strings.Replace(good, `"deadline":2`, `"deadline":2,"offset":3`, 1)),
		"offset equals horizon": `{"horizon":2,"tasks":[` +
			strings.Replace(good, `"deadline":2`, `"deadline":2,"offset":2`, 1) + `]}`,
		"duplicate priority": `{"horizon":10,"tasks":[` +
			periodicSimTaskObj("a", 0, 1, 2, 2, nil) + "," +
			periodicSimTaskObj("b", 0, 1, 3, 3, nil) + `]}`,
		"deadline after period": wrap(strings.Replace(good, `"deadline":2`, `"deadline":3`, 1)),
		"number out of int64":   wrap(strings.Replace(good, `"period":2`, `"period":99999999999999999999999`, 1)),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postPeriodicSim(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	// 257 tasks fail; 256 tasks with one job each must still succeed.
	var many bytes.Buffer
	many.WriteString(`{"horizon":1000,"tasks":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(periodicSimTaskObj("t"+strconv.Itoa(i), int64(i), 1, 2000, 2000, nil))
	}
	many.WriteString(`]}`)
	assertErrorCode(t, postPeriodicSim(t, many.String(), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")

	var okBuf bytes.Buffer
	okBuf.WriteString(`{"horizon":1000,"tasks":[`)
	for i := 0; i < 256; i++ {
		if i > 0 {
			okBuf.WriteByte(',')
		}
		okBuf.WriteString(periodicSimTaskObj("t"+strconv.Itoa(i), int64(i), 1, 2000, 2000, nil))
	}
	okBuf.WriteString(`]}`)
	periodicSimOK(t, okBuf.String())
}

func TestPeriodicSimNoPartialResultsOnError(t *testing.T) {
	rec := postPeriodicSim(t, `{"horizon":10,"tasks":[{"id":"a"`, "application/json")
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_json")
	if strings.Contains(rec.Body.String(), "timeline") || strings.Contains(rec.Body.String(), "results") {
		t.Fatalf("error response leaked partial results: %s", rec.Body.String())
	}
}

// refJob is the state of one job in the tick-by-tick reference simulation.
type refJob struct {
	task       int
	job        int64
	release    int64
	executed   int64
	remain     int64
	completion int64
}

// tickReference runs a plain one-microsecond-tick simulation with the same
// scheduling rules and returns the merged timeline plus per-task job results.
func tickReference(tasks []periodicTask, horizon int64) ([]periodicTimelineInterval, [][]periodicJobResult) {
	nt := len(tasks)
	var jobs []*refJob
	released := make([][]*refJob, nt)
	cursor := make([]int64, nt)
	remaining := make([]int, nt)
	for i, t := range tasks {
		remaining[i] = int((horizon - t.offset + t.period - 1) / t.period)
	}
	expand := func(upto int64) {
		for i := 0; i < nt; i++ {
			for remaining[i] > 0 {
				rt := tasks[i].offset + cursor[i]*tasks[i].period
				if rt > upto {
					break
				}
				j := &refJob{task: i, job: cursor[i], release: rt,
					remain: tasks[i].execution, completion: -1}
				jobs = append(jobs, j)
				released[i] = append(released[i], j)
				cursor[i]++
				remaining[i]--
			}
		}
	}
	type tick struct {
		at   int64
		task int
		job  int64
	}
	var ticks []tick
	current := (*refJob)(nil)
	for now := int64(0); now < horizon; now++ {
		expand(now)
		// Best other ready job; the running job is preempted only by a
		// strictly higher-priority one.
		var best *refJob
		for _, j := range jobs {
			if j.remain <= 0 || j.release > now || j == current {
				continue
			}
			if best == nil ||
				tasks[j.task].priority < tasks[best.task].priority ||
				(tasks[j.task].priority == tasks[best.task].priority &&
					(j.release < best.release ||
						(j.release == best.release && (j.task < best.task ||
							(j.task == best.task && j.job < best.job))))) {
				best = j
			}
		}
		if current == nil || current.remain <= 0 {
			current = best
		} else if best != nil && tasks[best.task].priority < tasks[current.task].priority {
			current = best
		}
		if current == nil {
			continue // idle tick: no timeline interval, clock still advances
		}
		ticks = append(ticks, tick{at: now, task: current.task, job: current.job})
		current.executed++
		current.remain--
		if current.remain == 0 {
			current.completion = now + 1
			current = nil
		}
	}
	expand(horizon - 1)

	var tl []periodicTimelineInterval
	for _, tk := range ticks {
		if last := len(tl) - 1; last >= 0 &&
			tl[last].TaskID == tasks[tk.task].id && tl[last].Job == tk.job &&
			tl[last].End == tk.at {
			tl[last].End = tk.at + 1
			continue
		}
		tl = append(tl, periodicTimelineInterval{
			TaskID: tasks[tk.task].id, Job: tk.job, Start: tk.at, End: tk.at + 1,
		})
	}

	results := make([][]periodicJobResult, nt)
	for i, t := range tasks {
		for _, j := range released[i] {
			deadline := j.release + t.deadline
			var c *int64
			var status string
			if j.completion >= 0 {
				v := j.completion
				c = &v
				if v <= deadline {
					status = "met"
				} else {
					status = "missed"
				}
			} else if deadline <= horizon {
				status = "missed"
			} else {
				status = "pending"
			}
			results[i] = append(results[i], periodicJobResult{
				Job: j.job, Release: j.release, AbsoluteDeadline: deadline,
				Executed: j.executed, Remaining: j.remain,
				Completion: c, DeadlineStatus: status,
			})
		}
	}
	return tl, results
}

// TestPeriodicSimDifferential compares the event-driven simulation against a
// tick-by-tick reference over randomized task sets.
func TestPeriodicSimDifferential(t *testing.T) {
	seed := rand.New(rand.NewSource(0x5112))
	for iter := 0; iter < 400; iter++ {
		horizon := int64(1 + seed.Intn(120))
		n := 1 + seed.Intn(5)
		prios := seed.Perm(256)[:n]
		tasks := make([]periodicTask, n)
		var total int64
		for i := 0; i < n; i++ {
			period := int64(1 + seed.Intn(40))
			off := int64(seed.Intn(int(period)))
			if off >= horizon {
				off = horizon - 1
				if off >= period {
					off = period - 1
				}
			}
			execution := int64(1 + seed.Intn(15))
			deadline := int64(1 + seed.Intn(int(period)))
			tasks[i] = periodicTask{
				id:        "t" + strconv.Itoa(i),
				priority:  int64(prios[i]),
				execution: execution,
				period:    period,
				deadline:  deadline,
				offset:    off,
			}
			total += (horizon - off + period - 1) / period
		}
		if total > 5000 {
			continue
		}

		got := simulatePeriodic(tasks, horizon)
		wantTL, wantJobs := tickReference(tasks, horizon)

		if len(got.Timeline) != len(wantTL) {
			t.Fatalf("iter %d h=%d tasks=%+v\ntimeline got %s\nwant %s",
				iter, horizon, tasks, intervals(got.Timeline), intervals(wantTL))
		}
		for i := range wantTL {
			if got.Timeline[i] != wantTL[i] {
				t.Fatalf("iter %d h=%d tasks=%+v\ntimeline got %s\nwant %s",
					iter, horizon, tasks, intervals(got.Timeline), intervals(wantTL))
			}
		}

		if len(got.Results) != len(wantJobs) {
			t.Fatalf("iter %d: result groups got %d want %d", iter, len(got.Results), len(wantJobs))
		}
		for i := range wantJobs {
			gj, wj := got.Results[i].Jobs, wantJobs[i]
			if got.Results[i].ID != tasks[i].id || len(gj) != len(wj) {
				t.Fatalf("iter %d task %d: %+v", iter, i, got.Results[i])
			}
			for k := range wj {
				g, w := gj[k], wj[k]
				if g.Job != w.Job || g.Release != w.Release ||
					g.AbsoluteDeadline != w.AbsoluteDeadline ||
					g.Executed != w.Executed || g.Remaining != w.Remaining ||
					(g.Completion == nil) != (w.Completion == nil) ||
					(g.Completion != nil && *g.Completion != *w.Completion) ||
					g.DeadlineStatus != w.DeadlineStatus {
					t.Fatalf("iter %d h=%d task %d job %d:\n got %+v\nwant %+v\ntasks=%+v",
						iter, horizon, i, k, g, w, tasks)
				}
			}
		}

		allDone := true
		for _, js := range wantJobs {
			for _, j := range js {
				if j.Completion == nil {
					allDone = false
				}
			}
		}
		wantStatus := "horizon"
		if allDone {
			wantStatus = "completed"
		}
		if got.Status != wantStatus {
			t.Fatalf("iter %d: status got %s want %s", iter, got.Status, wantStatus)
		}
	}
}
