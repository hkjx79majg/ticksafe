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

func mtask(id string, prio, rel, dead int64, actions ...string) string {
	return `{"id":"` + id + `","priority":` + strconv.FormatInt(prio, 10) +
		`,"release":` + strconv.FormatInt(rel, 10) +
		`,"deadline":` + strconv.FormatInt(dead, 10) +
		`,"actions":[` + strings.Join(actions, ",") + `]}`
}

func mrun(n int64) string     { return `{"run":` + strconv.FormatInt(n, 10) + `}` }
func mlock(m string) string   { return `{"lock":"` + m + `"}` }
func munlock(m string) string { return `{"unlock":"` + m + `"}` }

func TestMutexPriorityInheritance(t *testing.T) {
	// A (prio 2) holds M from t=0. B (prio 0) blocks on M at t=3: A inherits
	// prio 0, so C (prio 1, released at 5) cannot preempt. At t=6 M hands off
	// to B; B finishes, then C runs.
	body := `{"horizon":30,"tasks":[` +
		mtask("A", 2, 0, 30, mlock("M"), mrun(6), munlock("M")) + "," +
		mtask("B", 0, 3, 30, mlock("M"), mrun(2), munlock("M")) + "," +
		mtask("C", 1, 5, 30, mrun(3)) + "]}"
	resp := mutexOK(t, body)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed: %+v", resp.Status, resp)
	}
	want := []mutexTimelineInterval{
		{TaskID: "A", Start: 0, End: 3, EffectivePriority: 2},
		{TaskID: "A", Start: 3, End: 6, EffectivePriority: 0},
		{TaskID: "B", Start: 6, End: 8, EffectivePriority: 0},
		{TaskID: "C", Start: 8, End: 11, EffectivePriority: 1},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %+v, want %+v", resp.Timeline, want)
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v", i, resp.Timeline[i], want[i])
		}
	}
	a, b, c := resp.Results[0], resp.Results[1], resp.Results[2]
	if a.State != "completed" || a.Executed != 6 || a.Completion == nil || *a.Completion != 6 || a.BlockedOn != nil {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "completed" || b.Executed != 2 || b.Completion == nil || *b.Completion != 8 {
		t.Fatalf("B result = %+v", b)
	}
	if c.State != "completed" || *c.Completion != 11 {
		t.Fatalf("C result = %+v", c)
	}
}

func TestMutexIndirectInheritance(t *testing.T) {
	// A(p3) takes Q; B(p2) takes M then blocks on Q; C(p0) blocks on M.
	// A must transitively inherit 0, so D(p1) released at t=4 cannot preempt.
	body := `{"horizon":40,"tasks":[` +
		mtask("A", 3, 0, 40, mlock("Q"), mrun(8), munlock("Q")) + "," +
		mtask("B", 2, 1, 40, mlock("M"), mlock("Q"), mrun(1), munlock("Q"), munlock("M")) + "," +
		mtask("C", 0, 3, 40, mlock("M"), mrun(1), munlock("M")) + "," +
		mtask("D", 1, 4, 40, mrun(2)) + "]}"
	resp := mutexOK(t, body)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed: %+v", resp.Status, resp.Timeline)
	}
	var aRuns []mutexTimelineInterval
	for _, iv := range resp.Timeline {
		if iv.TaskID == "A" {
			aRuns = append(aRuns, iv)
		}
	}
	wantA := []mutexTimelineInterval{
		{TaskID: "A", Start: 0, End: 1, EffectivePriority: 3},
		{TaskID: "A", Start: 1, End: 3, EffectivePriority: 2},
		{TaskID: "A", Start: 3, End: 8, EffectivePriority: 0},
	}
	if len(aRuns) != len(wantA) {
		t.Fatalf("A runs = %+v, want %+v (all: %+v)", aRuns, wantA, resp.Timeline)
	}
	for i := range wantA {
		if aRuns[i] != wantA[i] {
			t.Fatalf("A run[%d] = %+v, want %+v (all: %+v)", i, aRuns[i], wantA[i], resp.Timeline)
		}
	}
	// At t=8 A unlocks Q and completes; B is handed Q while still holding M,
	// so B keeps effective priority 0 and runs before C: B [8,9), then
	// unlocks M at t=9, C [9,10), D [10,12).
	wantOrder := []string{"A", "B", "C", "D"}
	var gotOrder []string
	for _, iv := range resp.Timeline {
		if len(gotOrder) == 0 || gotOrder[len(gotOrder)-1] != iv.TaskID {
			gotOrder = append(gotOrder, iv.TaskID)
		}
	}
	if strings.Join(gotOrder, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("run order = %v, want %v; timeline %+v", gotOrder, wantOrder, resp.Timeline)
	}
}

func TestMutexHandoffTieBreaks(t *testing.T) {
	// X holds M. Two prio-0 waiters block at different times; the earlier
	// blocker wins on unlock even though it appears later in the input.
	body := `{"horizon":40,"tasks":[` +
		mtask("X", 3, 0, 40, mlock("M"), mrun(8), munlock("M"), mrun(1)) + "," +
		mtask("late", 0, 6, 40, mlock("M"), mrun(1), munlock("M")) + "," +
		mtask("early", 0, 2, 40, mlock("M"), mrun(1), munlock("M")) + "]}"
	resp := mutexOK(t, body)
	if resp.Status != "completed" {
		t.Fatalf("status = %q: %+v", resp.Status, resp.Timeline)
	}
	var firstAfterX string
	for _, iv := range resp.Timeline {
		if iv.Start == 8 && iv.End == 9 {
			firstAfterX = iv.TaskID
		}
	}
	if firstAfterX != "early" {
		t.Fatalf("interval [8,9) ran %q, want early: %+v", firstAfterX, resp.Timeline)
	}
}

func TestMutexDeadlockCycle(t *testing.T) {
	// gamma(p2, rel 0) takes B at t=0. alpha(p0, rel 1) preempts, takes A,
	// runs [1,2), blocks on B at t=2. gamma resumes [2,3), then blocks on A
	// at t=3 -> cycle. beta(p1) stays ready. Cycle starts at the earliest
	// input-order member (alpha) and follows the wait direction.
	body := `{"horizon":20,"tasks":[` +
		mtask("alpha", 0, 1, 20, mlock("A"), mrun(1), mlock("B"), mrun(1), munlock("B"), munlock("A")) + "," +
		mtask("beta", 3, 0, 20, mrun(5)) + "," +
		mtask("gamma", 2, 0, 20, mlock("B"), mrun(2), mlock("A"), mrun(1), munlock("A"), munlock("B")) + "]}"
	resp := mutexOK(t, body)
	if resp.Status != "deadlocked" || resp.DeadlockAt == nil || *resp.DeadlockAt != 3 {
		t.Fatalf("status=%q deadlockAt=%v, want deadlocked @3; %+v", resp.Status, resp.DeadlockAt, resp.Timeline)
	}
	wantCycle := []string{"alpha", "gamma"}
	if strings.Join(resp.Cycle, ",") != strings.Join(wantCycle, ",") {
		t.Fatalf("cycle = %v, want %v", resp.Cycle, wantCycle)
	}
	if a := resp.Results[0]; a.State != "blocked" || a.BlockedOn == nil || *a.BlockedOn != "B" {
		t.Fatalf("alpha result = %+v", a)
	}
	if g := resp.Results[2]; g.State != "blocked" || g.BlockedOn == nil || *g.BlockedOn != "A" {
		t.Fatalf("gamma result = %+v", g)
	}
	if b := resp.Results[1]; b.State != "ready" || b.BlockedOn != nil {
		t.Fatalf("beta result = %+v", b)
	}
}

func TestMutexDeadlockCycleStartsAtEarliest(t *testing.T) {
	// Staggered builds: z(p2,rel0) takes mc and is preempted mid-run by
	// b(p1,rel2); b takes mb, blocks on mc at t=4; a(p0,rel4) takes ma,
	// blocks on mb at t=6; z resumes, finishes its run and blocks on ma at
	// t=8. Wait edges a->b, b->z, z->a: cycle starts at earliest input a.
	body := `{"horizon":20,"tasks":[` +
		mtask("a", 0, 4, 20, mlock("ma"), mrun(2), mlock("mb"), mrun(1), munlock("mb"), munlock("ma")) + "," +
		mtask("b", 1, 2, 20, mlock("mb"), mrun(2), mlock("mc"), mrun(1), munlock("mc"), munlock("mb")) + "," +
		mtask("z", 2, 0, 20, mlock("mc"), mrun(4), mlock("ma"), mrun(1), munlock("ma"), munlock("mc")) + "]}"
	resp := mutexOK(t, body)
	if resp.Status != "deadlocked" || resp.DeadlockAt == nil || *resp.DeadlockAt != 8 {
		t.Fatalf("status = %q at %v, want deadlocked @8: %+v", resp.Status, resp.DeadlockAt, resp.Timeline)
	}
	wantCycle := []string{"a", "b", "z"}
	if strings.Join(resp.Cycle, ",") != strings.Join(wantCycle, ",") {
		t.Fatalf("cycle = %v, want %v", resp.Cycle, wantCycle)
	}
}

func TestMutexHorizonStatesAndDeadlines(t *testing.T) {
	// A(p2) takes M at t=0; B(p0) releases at t=1 and blocks on M, so A
	// inherits 0 and runs to the horizon (deadline == horizon, not completed
	// -> missed). B stays blocked (deadline 5 missed). C(p1) releases at t=2,
	// never runs, deadline beyond horizon -> pending; D(p3) released at 0
	// never runs -> pending.
	body := `{"horizon":10,"tasks":[` +
		mtask("A", 2, 0, 10, mlock("M"), mrun(20), munlock("M")) + "," +
		mtask("B", 0, 1, 5, mlock("M"), mrun(1), munlock("M")) + "," +
		mtask("C", 1, 2, 50, mrun(1)) + "," +
		mtask("D", 3, 0, 50, mrun(1)) + "]}"
	resp := mutexOK(t, body)
	if resp.Status != "horizon" {
		t.Fatalf("status = %q, want horizon", resp.Status)
	}
	a, b, c, d := resp.Results[0], resp.Results[1], resp.Results[2], resp.Results[3]
	if a.State != "ready" || a.Executed != 10 || a.Completion != nil || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "blocked" || b.BlockedOn == nil || *b.BlockedOn != "M" || b.Executed != 0 ||
		b.Completion != nil || b.DeadlineStatus != "missed" {
		t.Fatalf("B result = %+v", b)
	}
	if c.State != "ready" || c.BlockedOn != nil || c.Executed != 0 || c.DeadlineStatus != "pending" {
		t.Fatalf("C result = %+v", c)
	}
	if d.State != "ready" || d.DeadlineStatus != "pending" {
		t.Fatalf("D result = %+v", d)
	}
	if resp.DeadlockAt != nil || resp.Cycle != nil {
		t.Fatalf("non-deadlock payload leaked: %+v", resp)
	}
}

func TestMutexCompletionAtHorizon(t *testing.T) {
	// Run ending exactly at the horizon followed by unlock completes the task.
	body := `{"horizon":5,"tasks":[` +
		mtask("A", 0, 0, 5, mlock("M"), mrun(5), munlock("M")) + "]}"
	resp := mutexOK(t, body)
	if resp.Status != "completed" {
		t.Fatalf("status = %q, want completed: %+v", resp.Status, resp)
	}
	r := resp.Results[0]
	if r.State != "completed" || r.Completion == nil || *r.Completion != 5 || r.DeadlineStatus != "met" {
		t.Fatalf("result = %+v", r)
	}
	if len(resp.Timeline) != 1 || resp.Timeline[0] != (mutexTimelineInterval{TaskID: "A", Start: 0, End: 5, EffectivePriority: 0}) {
		t.Fatalf("timeline = %+v", resp.Timeline)
	}
}

func TestMutexNoMergeOnPriorityChange(t *testing.T) {
	// Adjacent runs of the same task must not merge across an effective
	// priority change.
	body := `{"horizon":20,"tasks":[` +
		mtask("A", 2, 0, 20, mlock("M"), mrun(10), munlock("M")) + "," +
		mtask("B", 0, 4, 20, mlock("M"), mrun(1), munlock("M")) + "]}"
	resp := mutexOK(t, body)
	want := []mutexTimelineInterval{
		{TaskID: "A", Start: 0, End: 4, EffectivePriority: 2},
		{TaskID: "A", Start: 4, End: 10, EffectivePriority: 0},
		{TaskID: "B", Start: 10, End: 11, EffectivePriority: 0},
	}
	if len(resp.Timeline) != len(want) {
		t.Fatalf("timeline = %+v, want %+v", resp.Timeline, want)
	}
	for i := range want {
		if resp.Timeline[i] != want[i] {
			t.Fatalf("timeline[%d] = %+v, want %+v", i, resp.Timeline[i], want[i])
		}
	}
}

func TestMutexSameInstantHandoffPreemption(t *testing.T) {
	// L(p3) takes m1 at t=0 and runs. M(p1) blocks on m1 at t=1, so L
	// inherits 1 and keeps running past the release of H(p0) at t=9. H
	// acquires m0 free at t=9, then blocks on m1: L inherits 0. L finishes
	// its run at t=10 and (still the current task) unlocks m0 then m1 at the
	// same instant; m1 hands off to M, whose immediate unlock hands m1 to H.
	// H (p0) becomes runnable at t=10 and must preempt M (p1) at that same
	// instant; M waits until H blocks again on m0 (which m0's owner L has
	// already released, so H takes it and runs).
	body := `{"horizon":30,"tasks":[` +
		mtask("L", 3, 0, 30,
			mlock("m1"), mrun(4), mlock("m0"), mrun(6), munlock("m0"), munlock("m1"), mrun(4)) + "," +
		mtask("H", 0, 9, 30, mlock("m0"), mlock("m1"), mrun(5), munlock("m1"), munlock("m0")) + "," +
		mtask("M", 1, 1, 30, mlock("m1"), munlock("m1"), mrun(2)) + "]}"
	resp := mutexOK(t, body)
	if resp.Status != "completed" {
		t.Fatalf("status = %q: %+v", resp.Status, resp.Timeline)
	}
	// M must not run any [10,11) tick; H runs [10,15).
	for _, iv := range resp.Timeline {
		if iv.TaskID == "M" {
			if iv.Start < 15 {
				t.Fatalf("M ran before H's same-instant handoff: %+v", resp.Timeline)
			}
		}
	}
	var hRun *mutexTimelineInterval
	for i := range resp.Timeline {
		if resp.Timeline[i].TaskID == "H" {
			hRun = &resp.Timeline[i]
			break
		}
	}
	if hRun == nil || hRun.Start != 10 || hRun.End != 15 || hRun.EffectivePriority != 0 {
		t.Fatalf("H first run = %+v, want [10,15) eff 0; all %+v", hRun, resp.Timeline)
	}
}

func TestMutexDeterministic(t *testing.T) {
	body := `{"horizon":30,"tasks":[` +
		mtask("A", 2, 0, 30, mlock("M"), mrun(6), munlock("M"), mrun(3)) + "," +
		mtask("B", 1, 2, 30, mlock("M"), mrun(2), munlock("M")) + "," +
		mtask("C", 0, 5, 30, mrun(4)) + "]}"
	first := mutexOK(t, body)
	second := mutexOK(t, body)
	fj, _ := json.Marshal(first)
	sj, _ := json.Marshal(second)
	if !bytes.Equal(fj, sj) {
		t.Fatalf("non-deterministic response:\n%s\n%s", fj, sj)
	}
}

func TestMutexValidationFailures(t *testing.T) {
	base := func(inner string) string {
		return `{"horizon":20,"tasks":[` + inner + `]}`
	}
	good := mtask("a", 0, 0, 20, mlock("M"), mrun(1), munlock("M"))
	cases := map[string]string{
		"json null body":       `null`,
		"missing horizon":      `{"tasks":[` + good + `]}`,
		"missing tasks":        `{"horizon":3}`,
		"zero horizon":         `{"horizon":0,"tasks":[]}`,
		"horizon too large":    `{"horizon":1000000000001,"tasks":[]}`,
		"empty tasks":          `{"horizon":3,"tasks":[]}`,
		"empty id":             base(mtask("", 0, 0, 1, mrun(1))),
		"duplicate id":         base(mtask("a", 0, 0, 20, mrun(1)) + "," + mtask("a", 1, 0, 20, mrun(1))),
		"priority negative":    base(mtask("a", -1, 0, 20, mrun(1))),
		"priority too large":   base(mtask("a", 256, 0, 20, mrun(1))),
		"release at horizon":   base(mtask("a", 0, 20, 21, mrun(1))),
		"deadline at release":  base(mtask("a", 0, 0, 0, mrun(1))),
		"missing priority":     base(`{"id":"a","release":0,"deadline":1,"actions":[` + mrun(1) + `]}`),
		"missing actions":      base(`{"id":"a","priority":0,"release":0,"deadline":1}`),
		"empty actions":        base(mtask("a", 0, 0, 20)),
		"no verb":              base(mtask("a", 0, 0, 20, `{}`)),
		"two verbs":            base(mtask("a", 0, 0, 20, `{"run":1,"lock":"M"}`)),
		"run zero":             base(mtask("a", 0, 0, 20, mrun(0))),
		"run negative":         base(mtask("a", 0, 0, 20, `{"run":-1}`)),
		"run null":             base(mtask("a", 0, 0, 20, `{"run":null}`)),
		"lock empty name":      base(mtask("a", 0, 0, 20, mlock(""), mrun(1))),
		"lock whitespace":      base(mtask("a", 0, 0, 20, mlock("a b"), mrun(1))),
		"duplicate acquire":    base(mtask("a", 0, 0, 20, mlock("M"), mlock("M"), mrun(1), munlock("M"))),
		"unlock not held":      base(mtask("a", 0, 0, 20, mrun(1), munlock("M"))),
		"still holding":        base(mtask("a", 0, 0, 20, mlock("M"), mrun(1))),
		"wrong run type":       base(mtask("a", 0, 0, 20, `{"run":"1"}`)),
		"lock name not string": base(mtask("a", 0, 0, 20, `{"lock":1}`)),
		"unlock null":          base(mtask("a", 0, 0, 20, `{"unlock":null}`)),
		"run total overflow":   base(mtask("a", 0, 0, 20, mrun(maxInt64), mrun(1))),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postMutex(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	bad400 := map[string]string{
		"unknown top key": `{"horizon":20,"bogus":1,"tasks":[` + good + `]}`,
		"execution key":   base(`{"id":"a","priority":0,"release":0,"execution":1,"deadline":1,"actions":[` + mrun(1) + `]}`),
		"unknown action":  base(mtask("a", 0, 0, 20, `{"sleep":1}`)),
		"trailing token":  base(good) + ` garbage`,
		"duplicate key":   `{"horizon":3,"horizon":4,"tasks":[` + good + `]}`,
		"syntax error":    `{`,
	}
	for name, body := range bad400 {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postMutex(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}
}

func TestMutexActionCountBounds(t *testing.T) {
	mk := func(count int) string {
		acts := make([]string, count)
		for i := range acts {
			acts[i] = mrun(int64(1 + i%3))
		}
		return `{"horizon":100000,"tasks":[` + mtask("a", 0, 0, 200000, acts...) + `]}`
	}
	if rec := postMutex(t, mk(1024), "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("1024 actions: status = %d, body %s", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, postMutex(t, mk(1025), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")

	// Task count matches the one-shot bound of 256.
	var many []string
	for i := 0; i < 257; i++ {
		many = append(many, mtask("t"+strconv.Itoa(i), 0, 0, 200000, mrun(1)))
	}
	assertErrorCode(t, postMutex(t, `{"horizon":100000,"tasks":[`+strings.Join(many, ",")+`]}`,
		"application/json"), http.StatusUnprocessableEntity, "validation_failed")
}

func TestMutexMethodAndMediaType(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, mutexAnalyzePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
	assertErrorCode(t, postMutex(t, `{"horizon":3,"tasks":[]}`, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postMutex(t, `{"horizon":3,"tasks":[]}`, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	rec := postMutex(t, `{"horizon":3,"tasks":[`+
		mtask("a", 0, 0, 20, mrun(1))+`]}`, "application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("charset parameter rejected: %d %s", rec.Code, rec.Body.String())
	}
}

// ---- Differential test against an independent tick-by-tick reference ------

type refTask struct {
	id                      string
	prio, release, deadline int64
	acts                    []mutexAction
}

type refSim struct {
	tasks     []refTask
	n         int
	horizon   int64
	owner     map[string]int
	waiters   map[string][]int
	held      []map[string]bool
	blockedM  []string
	blockTime []int64
	pc        []int
	rem       []int64
	released  []bool
	done      []bool
	executed  []int64
	finish    []*int64
	eff       []int64
	now       int64
	current   int
	segs      []mutexSegment
	dead      bool
	deadAt    int64
	cycle     []int
}

func refRun(tasks []refTask, horizon int64) mutexResponse {
	r := &refSim{
		tasks: tasks, n: len(tasks), horizon: horizon,
		owner: map[string]int{}, waiters: map[string][]int{},
		current: -1,
	}
	r.held = make([]map[string]bool, r.n)
	r.blockedM = make([]string, r.n)
	r.blockTime = make([]int64, r.n)
	r.pc = make([]int, r.n)
	r.rem = make([]int64, r.n)
	r.released = make([]bool, r.n)
	r.done = make([]bool, r.n)
	r.executed = make([]int64, r.n)
	r.finish = make([]*int64, r.n)
	r.eff = make([]int64, r.n)
	for i := range tasks {
		r.held[i] = map[string]bool{}
		r.eff[i] = tasks[i].prio
	}

	for t := int64(0); t < horizon; t++ {
		r.now = t
		for i := 0; i < r.n; i++ {
			if tasks[i].release == t {
				r.released[i] = true
			}
		}
		if !r.settlePoint() {
			return r.build()
		}
		if r.current == -1 {
			continue
		}
		cur := r.current
		r.rem[cur]--
		r.executed[cur]++
		r.segs = append(r.segs, mutexSegment{index: cur, start: t, end: t + 1, eff: r.eff[cur]})
		if r.rem[cur] == 0 {
			// Keep current set: at the next instant settlePoint drains this
			// task's zero-duration chain before any (re)scheduling, matching
			// "current task's zero-time actions first, then schedule".
			r.pc[cur]++
		}
	}

	// Boundary instant at t == horizon: drain the just-finished task's
	// zero-duration chain once (matches the boundary rule).
	if r.current != -1 {
		i := r.current
		if !r.done[i] && r.blockedM[i] == "" && r.rem[i] == 0 {
			r.now = horizon
			if r.settle(i) {
				r.dead = true
				r.deadAt = horizon
			}
		}
	}
	return r.build()
}

// settlePoint performs all zero-time resolution at the current instant and
// leaves r.current on the task that owns the CPU during the next tick.
func (r *refSim) settlePoint() bool {
	for {
		if r.current != -1 {
			if r.settle(r.current) {
				r.dead = true
				r.deadAt = r.now
				return false
			}
			if r.done[r.current] || r.blockedM[r.current] != "" || r.rem[r.current] == 0 {
				r.current = -1
			}
		}
		best := r.pick()
		if r.current == -1 {
			if best == -1 {
				return true
			}
			r.current = best
			if r.settle(best) {
				r.dead = true
				r.deadAt = r.now
				return false
			}
			if r.done[best] || r.blockedM[best] != "" || r.rem[best] == 0 {
				r.current = -1
				continue
			}
		}
		// Same-instant preemption: the selected task's zero-duration chain
		// can hand a mutex to a strictly higher effective-priority task.
		resolved := false
		for !resolved {
			b := r.pick()
			if b != -1 && b != r.current && r.eff[b] < r.eff[r.current] {
				r.current = b
				if r.settle(b) {
					r.dead = true
					r.deadAt = r.now
					return false
				}
				if r.done[b] || r.blockedM[b] != "" || r.rem[b] == 0 {
					r.current = -1
					break
				}
				continue
			}
			resolved = true
		}
		if r.current == -1 {
			continue
		}
		return true
	}
}

func (r *refSim) settle(i int) bool {
	tk := r.tasks[i]
	for r.pc[i] < len(tk.acts) {
		a := tk.acts[r.pc[i]]
		switch a.kind {
		case mutexActRun:
			if r.rem[i] == 0 {
				r.rem[i] = a.run
			}
			return false
		case mutexActLock:
			if owner, ok := r.owner[a.name]; ok {
				if owner == i {
					r.pc[i]++
					continue
				}
				cycle := r.findCycle(i, owner)
				r.blockedM[i] = a.name
				r.blockTime[i] = r.now
				r.waiters[a.name] = append(r.waiters[a.name], i)
				if cycle != nil {
					r.cycle = cycle
					return true
				}
				r.recompute()
				return false
			}
			r.owner[a.name] = i
			r.held[i][a.name] = true
			r.pc[i]++
		case mutexActUnlock:
			r.give(a.name, i)
			r.pc[i]++
		}
	}
	r.done[i] = true
	at := r.now
	r.finish[i] = &at
	return false
}

func (r *refSim) findCycle(waiter, holder int) []int {
	chain := []int{holder}
	cur := holder
	for {
		if r.blockedM[cur] == "" {
			return nil
		}
		next := r.owner[r.blockedM[cur]]
		if next == waiter {
			break
		}
		chain = append(chain, next)
		cur = next
	}
	cycle := append([]int{waiter}, chain...)
	first := 0
	for k := 1; k < len(cycle); k++ {
		if cycle[k] < cycle[first] {
			first = k
		}
	}
	return append(append([]int(nil), cycle[first:]...), cycle[:first]...)
}

func (r *refSim) give(m string, from int) {
	ws := r.waiters[m]
	delete(r.held[from], m)
	if len(ws) == 0 {
		delete(r.owner, m)
		r.recompute()
		return
	}
	w := ws[0]
	for _, x := range ws[1:] {
		if r.eff[x] < r.eff[w] ||
			(r.eff[x] == r.eff[w] && r.blockTime[x] < r.blockTime[w]) ||
			(r.eff[x] == r.eff[w] && r.blockTime[x] == r.blockTime[w] && x < w) {
			w = x
		}
	}
	r.owner[m] = w
	r.held[w][m] = true
	var rest []int
	for _, x := range ws {
		if x != w {
			rest = append(rest, x)
		}
	}
	r.waiters[m] = rest
	r.blockedM[w] = ""
	r.recompute()
}

func (r *refSim) recompute() {
	color := make([]int, r.n)
	var walk func(int) int64
	walk = func(i int) int64 {
		if color[i] == 2 {
			return r.eff[i]
		}
		color[i] = 1
		best := r.tasks[i].prio
		for m := range r.held[i] {
			for _, w := range r.waiters[m] {
				if v := walk(w); v < best {
					best = v
				}
			}
		}
		color[i] = 2
		r.eff[i] = best
		return best
	}
	for i := 0; i < r.n; i++ {
		walk(i)
	}
}

func (r *refSim) pick() int {
	best := -1
	for i := 0; i < r.n; i++ {
		if !r.released[i] || r.done[i] || r.blockedM[i] != "" {
			continue
		}
		if best == -1 {
			best = i
			continue
		}
		a, b := r.tasks[i], r.tasks[best]
		if r.eff[i] < r.eff[best] ||
			(r.eff[i] == r.eff[best] && a.release < b.release) ||
			(r.eff[i] == r.eff[best] && a.release == b.release && i < best) {
			best = i
		}
	}
	return best
}

func (r *refSim) build() mutexResponse {
	resp := mutexResponse{Timeline: []mutexTimelineInterval{}}
	for _, seg := range r.segs {
		last := len(resp.Timeline) - 1
		if last >= 0 &&
			resp.Timeline[last].TaskID == r.tasks[seg.index].id &&
			resp.Timeline[last].EffectivePriority == seg.eff &&
			resp.Timeline[last].End == seg.start {
			resp.Timeline[last].End = seg.end
			continue
		}
		resp.Timeline = append(resp.Timeline, mutexTimelineInterval{
			TaskID: r.tasks[seg.index].id, Start: seg.start, End: seg.end, EffectivePriority: seg.eff,
		})
	}
	resp.Results = make([]mutexTaskResult, r.n)
	allDone := true
	for i, tk := range r.tasks {
		state := "ready"
		switch {
		case r.done[i]:
			state = "completed"
		case r.blockedM[i] != "":
			state = "blocked"
		case !r.released[i]:
			state = "unreleased"
		}
		if state != "completed" {
			allDone = false
		}
		var bo *string
		if state == "blocked" {
			m := r.blockedM[i]
			bo = &m
		}
		var ds string
		switch c := r.finish[i]; {
		case c != nil && *c <= tk.deadline:
			ds = "met"
		case c != nil:
			ds = "missed"
		case tk.deadline <= r.horizon:
			ds = "missed"
		default:
			ds = "pending"
		}
		resp.Results[i] = mutexTaskResult{
			Executed: r.executed[i], Completion: r.finish[i],
			State: state, BlockedOn: bo, DeadlineStatus: ds,
		}
	}
	switch {
	case r.dead:
		resp.Status = "deadlocked"
	case allDone:
		resp.Status = "completed"
	default:
		resp.Status = "horizon"
	}
	if r.dead {
		at := r.deadAt
		resp.DeadlockAt = &at
		resp.Cycle = make([]string, len(r.cycle))
		for k, idx := range r.cycle {
			resp.Cycle[k] = r.tasks[idx].id
		}
	}
	return resp
}

func TestMutexDifferential(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	names := []string{"m0", "m1", "m2"}
	for iter := 0; iter < 2000; iter++ {
		n := 1 + rng.Intn(5)
		horizon := int64(2 + rng.Intn(24))
		got := make([]mutexTask, n)
		ref := make([]refTask, n)
		used := map[string]bool{}
		for i := 0; i < n; i++ {
			var id string
			for {
				id = "t" + strconv.Itoa(rng.Intn(40))
				if !used[id] {
					used[id] = true
					break
				}
			}
			prio := int64(rng.Intn(4))
			rel := int64(rng.Intn(int(horizon)))
			dead := rel + int64(1+rng.Intn(30))
			nacts := 1 + rng.Intn(8)
			acts := make([]mutexAction, 0, nacts+3)
			var open []string
			for k := 0; k < nacts; k++ {
				choice := rng.Intn(10)
				if choice < 6 {
					acts = append(acts, mutexAction{kind: mutexActRun, run: int64(1 + rng.Intn(5))})
					continue
				}
				if choice < 8 {
					m := names[rng.Intn(len(names))]
					held := false
					for _, h := range open {
						if h == m {
							held = true
						}
					}
					if held {
						acts = append(acts, mutexAction{kind: mutexActRun, run: 1})
					} else {
						acts = append(acts, mutexAction{kind: mutexActLock, name: m})
						open = append(open, m)
					}
					continue
				}
				if len(open) == 0 {
					acts = append(acts, mutexAction{kind: mutexActRun, run: 1})
				} else {
					j := rng.Intn(len(open))
					m := open[j]
					open = append(open[:j], open[j+1:]...)
					acts = append(acts, mutexAction{kind: mutexActUnlock, name: m})
				}
			}
			// Close any still-held mutexes so the program is statically valid.
			for j := len(open) - 1; j >= 0; j-- {
				acts = append(acts, mutexAction{kind: mutexActUnlock, name: open[j]})
			}
			got[i] = mutexTask{id: id, priority: prio, release: rel, deadline: dead, actions: acts}
			ref[i] = refTask{id: id, prio: prio, release: rel, deadline: dead, acts: acts}
		}

		want := refRun(ref, horizon)
		resp := simulateMutex(got, horizon)

		if resp.Status != want.Status {
			t.Fatalf("iter %d seed %d: status %q vs %q\ngot tl %+v", iter, seed, resp.Status, want.Status, resp.Timeline)
		}
		if len(resp.Timeline) != len(want.Timeline) {
			t.Fatalf("iter %d seed %d: timeline len %d vs %d\ngot %+v\nref %+v",
				iter, seed, len(resp.Timeline), len(want.Timeline), resp.Timeline, want.Timeline)
		}
		for k := range want.Timeline {
			if resp.Timeline[k] != want.Timeline[k] {
				t.Fatalf("iter %d seed %d: tl[%d] %+v vs %+v", iter, seed, k, resp.Timeline[k], want.Timeline[k])
			}
		}
		for i := range want.Results {
			g, w := resp.Results[i], want.Results[i]
			if g.Executed != w.Executed || g.State != w.State || g.DeadlineStatus != w.DeadlineStatus {
				t.Fatalf("iter %d seed %d: %s result %+v vs %+v", iter, seed, ref[i].id, g, w)
			}
			if (g.Completion == nil) != (w.Completion == nil) ||
				(g.Completion != nil && *g.Completion != *w.Completion) {
				t.Fatalf("iter %d seed %d: %s completion %v vs %v", iter, seed, ref[i].id, g.Completion, w.Completion)
			}
			if (g.BlockedOn == nil) != (w.BlockedOn == nil) ||
				(g.BlockedOn != nil && *g.BlockedOn != *w.BlockedOn) {
				t.Fatalf("iter %d seed %d: %s blockedOn %v vs %v", iter, seed, ref[i].id, g.BlockedOn, w.BlockedOn)
			}
		}
		if (resp.DeadlockAt == nil) != (want.DeadlockAt == nil) ||
			(resp.DeadlockAt != nil && *resp.DeadlockAt != *want.DeadlockAt) {
			t.Fatalf("iter %d seed %d: deadlockAt %v vs %v", iter, seed, resp.DeadlockAt, want.DeadlockAt)
		}
		if strings.Join(resp.Cycle, ",") != strings.Join(want.Cycle, ",") {
			t.Fatalf("iter %d seed %d: cycle %v vs %v", iter, seed, resp.Cycle, want.Cycle)
		}
	}
}
