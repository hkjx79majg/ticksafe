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

func intReq(horizon int64, tasks string, interrupts ...string) string {
	return `{"horizon":` + strconv.FormatInt(horizon, 10) +
		`,"tasks":[` + tasks + `],"interrupts":[` + strings.Join(interrupts, ",") + `]}`
}

func intRun(n int64) string      { return `{"run":` + strconv.FormatInt(n, 10) + `}` }
func intCritical(n int64) string { return `{"critical":` + strconv.FormatInt(n, 10) + `}` }

func checkIntTimeline(t *testing.T, got, want []interruptTimelineInterval) {
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

func TestInterruptPreemptsRun(t *testing.T) {
	// The interrupt arrives while A runs, executes immediately, then A resumes.
	body := intReq(50,
		intTaskJSON("A", 1, 0, 50, intRun(10)),
		intIntrJSON("I1", 0, 3, 2, 10))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 12 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@12", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []interruptTimelineInterval{
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
		i1.Latency == nil || *i1.Latency != 0 || i1.Completion == nil || *i1.Completion != 5 ||
		i1.DeadlineStatus != "met" {
		t.Fatalf("I1 result = %+v", i1)
	}
	if len(resp.CriticalSections) != 0 {
		t.Fatalf("criticalSections = %+v, want empty", resp.CriticalSections)
	}
}

func TestInterruptCriticalNonPreemptible(t *testing.T) {
	// The interrupt arriving inside A's critical section waits for it to end.
	body := intReq(50,
		intTaskJSON("A", 1, 0, 50, intCritical(5), intRun(2)),
		intIntrJSON("I1", 0, 2, 1, 10))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 8 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@8", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []interruptTimelineInterval{
		{ActorType: "task", ActorID: "A", Mode: "critical", Start: 0, End: 5},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 5, End: 6},
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 6, End: 8},
	})
	if len(resp.CriticalSections) != 1 {
		t.Fatalf("criticalSections = %+v", resp.CriticalSections)
	}
	cs := resp.CriticalSections[0]
	if cs.TaskID != "A" || cs.ActionIndex != 0 || cs.Start == nil || *cs.Start != 0 ||
		cs.End == nil || *cs.End != 5 || cs.ObservedDuration != 5 || !cs.Completed {
		t.Fatalf("critical section = %+v", cs)
	}
	i1 := resp.InterruptResults[0]
	if i1.Start == nil || *i1.Start != 5 || i1.Latency == nil || *i1.Latency != 3 {
		t.Fatalf("I1 result = %+v", i1)
	}
}

func TestInterruptPriorityAndResume(t *testing.T) {
	// I2 preempts I1 (strictly smaller priority); I3 at equal priority to I2
	// does not preempt; I1 resumes once higher-priority interrupts finish.
	body := intReq(100,
		intTaskJSON("A", 0, 0, 100, intRun(1)),
		intIntrJSON("I1", 2, 0, 5, 100),
		intIntrJSON("I2", 1, 1, 2, 100),
		intIntrJSON("I3", 1, 2, 1, 100))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 9 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@9", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []interruptTimelineInterval{
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 0, End: 1},
		{ActorType: "interrupt", ActorID: "I2", Mode: "run", Start: 1, End: 3},
		{ActorType: "interrupt", ActorID: "I3", Mode: "run", Start: 3, End: 4},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 4, End: 8},
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 8, End: 9},
	})
	i1, i2, i3 := resp.InterruptResults[0], resp.InterruptResults[1], resp.InterruptResults[2]
	if i1.Executed != 5 || i1.Completion == nil || *i1.Completion != 8 || *i1.Start != 0 || *i1.Latency != 0 {
		t.Fatalf("I1 result = %+v", i1)
	}
	if i2.Completion == nil || *i2.Completion != 3 || *i2.Start != 1 || *i2.Latency != 0 {
		t.Fatalf("I2 result = %+v", i2)
	}
	if i3.Completion == nil || *i3.Completion != 4 || *i3.Start != 3 || *i3.Latency != 1 {
		t.Fatalf("I3 result = %+v", i3)
	}
}

func TestInterruptSameArrivalInputOrder(t *testing.T) {
	// Equal-priority interrupts arriving together run in input order.
	body := intReq(10,
		intTaskJSON("A", 0, 0, 10, intRun(1)),
		intIntrJSON("I1", 0, 0, 1, 10),
		intIntrJSON("I2", 0, 0, 1, 10))
	resp := interruptOK(t, body)
	checkIntTimeline(t, resp.Timeline, []interruptTimelineInterval{
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 0, End: 1},
		{ActorType: "interrupt", ActorID: "I2", Mode: "run", Start: 1, End: 2},
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 2, End: 3},
	})
	if resp.Status != "completed" || resp.StoppedAt != 3 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@3", resp.Status, resp.StoppedAt)
	}
}

func TestInterruptHorizonTruncatesCritical(t *testing.T) {
	// The critical section is cut by the horizon; the interrupt never starts.
	body := intReq(5,
		intTaskJSON("A", 0, 0, 100, intCritical(10)),
		intIntrJSON("I1", 0, 1, 1, 100))
	resp := interruptOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@5", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []interruptTimelineInterval{
		{ActorType: "task", ActorID: "A", Mode: "critical", Start: 0, End: 5},
	})
	cs := resp.CriticalSections[0]
	if cs.Start == nil || *cs.Start != 0 || cs.End != nil || cs.ObservedDuration != 5 || cs.Completed {
		t.Fatalf("critical section = %+v, want truncated at horizon", cs)
	}
	a := resp.TaskResults[0]
	if a.Executed != 5 || a.Completion != nil || a.DeadlineStatus != "pending" {
		t.Fatalf("A result = %+v", a)
	}
	i1 := resp.InterruptResults[0]
	if i1.Executed != 0 || i1.Start != nil || i1.Latency != nil || i1.Completion != nil ||
		i1.DeadlineStatus != "pending" {
		t.Fatalf("I1 result = %+v", i1)
	}
}

func TestInterruptDeadlineStatuses(t *testing.T) {
	// I1 finishes late (missed); A never finishes and its deadline is inside
	// the horizon (missed); B never finishes with a deadline past it (pending).
	body := intReq(20,
		intTaskJSON("A", 2, 0, 5, intRun(100))+
			","+intTaskJSON("B", 3, 0, 50, intRun(100)),
		intIntrJSON("I1", 0, 0, 10, 3))
	resp := interruptOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 20 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@20", resp.Status, resp.StoppedAt)
	}
	i1 := resp.InterruptResults[0]
	if i1.Completion == nil || *i1.Completion != 10 || i1.DeadlineStatus != "missed" {
		t.Fatalf("I1 result = %+v", i1)
	}
	a, b := resp.TaskResults[0], resp.TaskResults[1]
	if a.Executed != 10 || a.Completion != nil || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
	if b.Executed != 0 || b.Completion != nil || b.DeadlineStatus != "pending" {
		t.Fatalf("B result = %+v", b)
	}
}

func TestInterruptTaskPreemptionAndModes(t *testing.T) {
	// B preempts A's run; A's critical and run actions stay separate timeline
	// entries even when adjacent; consecutive run actions merge.
	body := intReq(50,
		intTaskJSON("A", 2, 0, 50, intRun(3), intCritical(2), intRun(1))+
			","+intTaskJSON("B", 1, 1, 50, intRun(1)),
		intIntrJSON("I1", 0, 10, 1, 50))
	resp := interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 11 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@11", resp.Status, resp.StoppedAt)
	}
	checkIntTimeline(t, resp.Timeline, []interruptTimelineInterval{
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 0, End: 1},
		{ActorType: "task", ActorID: "B", Mode: "run", Start: 1, End: 2},
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 2, End: 4},
		{ActorType: "task", ActorID: "A", Mode: "critical", Start: 4, End: 6},
		{ActorType: "task", ActorID: "A", Mode: "run", Start: 6, End: 7},
		{ActorType: "interrupt", ActorID: "I1", Mode: "run", Start: 10, End: 11},
	})
	a := resp.TaskResults[0]
	if a.Executed != 6 || a.Completion == nil || *a.Completion != 7 {
		t.Fatalf("A result = %+v", a)
	}
	cs := resp.CriticalSections[0]
	if cs.ActionIndex != 1 || cs.Start == nil || *cs.Start != 4 || cs.End == nil || *cs.End != 6 ||
		cs.ObservedDuration != 2 || !cs.Completed {
		t.Fatalf("critical section = %+v", cs)
	}
}

func TestInterruptUnstartedCriticalSection(t *testing.T) {
	// A's critical action is never reached: null, null, 0, false.
	body := intReq(10,
		intTaskJSON("A", 1, 0, 10, intRun(20), intCritical(3)),
		intIntrJSON("I1", 0, 0, 1, 10))
	resp := interruptOK(t, body)
	cs := resp.CriticalSections[0]
	if cs.TaskID != "A" || cs.ActionIndex != 1 || cs.Start != nil || cs.End != nil ||
		cs.ObservedDuration != 0 || cs.Completed {
		t.Fatalf("critical section = %+v, want unstarted", cs)
	}
}

func TestInterruptValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing horizon", `{"tasks":[{"id":"A","priority":0,"release":0,"deadline":1,"actions":[{"run":1}]}],"interrupts":[{"id":"I","priority":0,"arrival":0,"execution":1,"deadline":1}]}`},
		{"zero horizon", intReq(0, intTaskJSON("A", 0, 0, 1, intRun(1)), intIntrJSON("I", 0, 0, 1, 1))},
		{"no tasks", `{"horizon":10,"tasks":[],"interrupts":[{"id":"I","priority":0,"arrival":0,"execution":1,"deadline":1}]}`},
		{"no interrupts", `{"horizon":10,"tasks":[{"id":"A","priority":0,"release":0,"deadline":1,"actions":[{"run":1}]}],"interrupts":[]}`},
		{"missing interrupts", `{"horizon":10,"tasks":[{"id":"A","priority":0,"release":0,"deadline":1,"actions":[{"run":1}]}]}`},
		{"duplicate task id", intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1))+","+intTaskJSON("A", 1, 0, 9, intRun(1)), intIntrJSON("I", 0, 0, 1, 9))},
		{"duplicate interrupt id", intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1)), intIntrJSON("I", 0, 0, 1, 9), intIntrJSON("I", 1, 0, 1, 9))},
		{"task id with space", intReq(10, intTaskJSON("A B", 0, 0, 9, intRun(1)), intIntrJSON("I", 0, 0, 1, 9))},
		{"priority too large", intReq(10, intTaskJSON("A", 256, 0, 9, intRun(1)), intIntrJSON("I", 0, 0, 1, 9))},
		{"release at horizon", intReq(10, intTaskJSON("A", 0, 10, 11, intRun(1)), intIntrJSON("I", 0, 0, 1, 9))},
		{"deadline not after release", intReq(10, intTaskJSON("A", 0, 2, 2, intRun(1)), intIntrJSON("I", 0, 0, 1, 9))},
		{"empty actions", `{"horizon":10,"tasks":[{"id":"A","priority":0,"release":0,"deadline":9,"actions":[]}],"interrupts":[{"id":"I","priority":0,"arrival":0,"execution":1,"deadline":9}]}`},
		{"action with both verbs", `{"horizon":10,"tasks":[{"id":"A","priority":0,"release":0,"deadline":9,"actions":[{"run":1,"critical":1}]}],"interrupts":[{"id":"I","priority":0,"arrival":0,"execution":1,"deadline":9}]}`},
		{"action with no verb", `{"horizon":10,"tasks":[{"id":"A","priority":0,"release":0,"deadline":9,"actions":[{}]}],"interrupts":[{"id":"I","priority":0,"arrival":0,"execution":1,"deadline":9}]}`},
		{"zero run", intReq(10, intTaskJSON("A", 0, 0, 9, intRun(0)), intIntrJSON("I", 0, 0, 1, 9))},
		{"negative critical", intReq(10, intTaskJSON("A", 0, 0, 9, intCritical(-1)), intIntrJSON("I", 0, 0, 1, 9))},
		{"interrupt arrival at horizon", intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1)), intIntrJSON("I", 0, 10, 1, 11))},
		{"interrupt zero execution", intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1)), intIntrJSON("I", 0, 0, 0, 9))},
		{"interrupt deadline not after arrival", intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1)), intIntrJSON("I", 0, 3, 1, 3))},
		{"interrupt priority negative", intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1)), intIntrJSON("I", -1, 0, 1, 9))},
		{"interrupt id empty", intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1)), intIntrJSON("", 0, 0, 1, 9))},
		{"run total overflow", intReq(10,
			intTaskJSON("A", 0, 0, 9, intRun(math.MaxInt64))+","+intTaskJSON("B", 1, 0, 9, intRun(1)),
			intIntrJSON("I", 0, 0, 1, 9))},
		{"execution total overflow", intReq(10,
			intTaskJSON("A", 0, 0, 9, intRun(1)),
			intIntrJSON("I1", 0, 0, math.MaxInt64, 9), intIntrJSON("I2", 1, 0, 1, 9))},
		{"non-integer run", `{"horizon":10,"tasks":[{"id":"A","priority":0,"release":0,"deadline":9,"actions":[{"run":1.5}]}],"interrupts":[{"id":"I","priority":0,"arrival":0,"execution":1,"deadline":9}]}`},
		{"wrong type", `{"horizon":"10","tasks":[],"interrupts":[]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := postInterrupt(t, tc.body, "application/json")
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422; body: %s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "validation_failed") {
				t.Fatalf("body = %s, want validation_failed", rec.Body.String())
			}
		})
	}
}

func TestInterruptTooMany(t *testing.T) {
	var tasks []string
	for i := 0; i < 257; i++ {
		tasks = append(tasks, intTaskJSON("T"+strconv.Itoa(i), 0, 0, 9, intRun(1)))
	}
	body := intReq(10, strings.Join(tasks, ","), intIntrJSON("I", 0, 0, 1, 9))
	if rec := postInterrupt(t, body, "application/json"); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("257 tasks: status = %d, want 422", rec.Code)
	}

	var intrs []string
	for i := 0; i < 4097; i++ {
		intrs = append(intrs, intIntrJSON("I"+strconv.Itoa(i), 0, 0, 1, 9))
	}
	body = intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1)), intrs...)
	if rec := postInterrupt(t, body, "application/json"); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("4097 interrupts: status = %d, want 422", rec.Code)
	}

	var actions []string
	for i := 0; i < 1025; i++ {
		actions = append(actions, intRun(1))
	}
	body = intReq(2000, intTaskJSON("A", 0, 0, 1999, actions...), intIntrJSON("I", 0, 0, 1, 9))
	if rec := postInterrupt(t, body, "application/json"); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("1025 actions: status = %d, want 422", rec.Code)
	}
}

func TestInterruptErrorContract(t *testing.T) {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, interruptAnalyzePath, nil)
	Handler().ServeHTTP(rec, r)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET: status = %d allow = %q", rec.Code, rec.Header().Get("Allow"))
	}

	valid := intReq(10, intTaskJSON("A", 0, 0, 9, intRun(1)), intIntrJSON("I", 0, 0, 1, 9))
	if rec := postInterrupt(t, valid, "text/plain"); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain: status = %d, want 415", rec.Code)
	}
	if rec := postInterrupt(t, valid, ""); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("no content type: status = %d, want 415", rec.Code)
	}
	if rec := postInterrupt(t, `{"horizon":`, "application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad syntax: status = %d, want 400", rec.Code)
	}
	if rec := postInterrupt(t, valid+` {}`, "application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("trailing content: status = %d, want 400", rec.Code)
	}
	if rec := postInterrupt(t, `{"horizon":10,"horizon":10,"tasks":[],"interrupts":[]}`, "application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("duplicate key: status = %d, want 400", rec.Code)
	}
	if rec := postInterrupt(t, `{"horizon":10,"tasks":[],"interrupts":[],"extra":1}`, "application/json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: status = %d, want 400", rec.Code)
	}
}

func TestInterruptHorizonBoundary(t *testing.T) {
	// An interrupt completing exactly at the horizon counts as completed; a
	// critical section that never starts reports nulls.
	body := intReq(10,
		intTaskJSON("A", 1, 5, 20, intCritical(10)),
		intIntrJSON("I1", 0, 0, 10, 10))
	resp := interruptOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 10 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@10", resp.Status, resp.StoppedAt)
	}
	i1 := resp.InterruptResults[0]
	if i1.Completion == nil || *i1.Completion != 10 || i1.DeadlineStatus != "met" {
		t.Fatalf("I1 result = %+v", i1)
	}
	cs := resp.CriticalSections[0]
	if cs.Start != nil || cs.End != nil || cs.ObservedDuration != 0 || cs.Completed {
		t.Fatalf("critical section = %+v, want unstarted", cs)
	}

	// A task's last action ending exactly at the horizon completes there.
	body = intReq(10,
		intTaskJSON("A", 1, 0, 10, intRun(4), intCritical(3)),
		intIntrJSON("I1", 0, 1, 3, 10))
	resp = interruptOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 10 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@10", resp.Status, resp.StoppedAt)
	}
	a := resp.TaskResults[0]
	if a.Completion == nil || *a.Completion != 10 || a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v", a)
	}
	cs = resp.CriticalSections[0]
	if cs.Start == nil || *cs.Start != 7 || cs.End == nil || *cs.End != 10 ||
		cs.ObservedDuration != 3 || !cs.Completed {
		t.Fatalf("critical section = %+v, want [7,10) completed", cs)
	}
}

func TestInterruptStateless(t *testing.T) {
	body := intReq(50,
		intTaskJSON("A", 1, 0, 50, intCritical(5), intRun(2)),
		intIntrJSON("I1", 0, 2, 1, 10))
	first := interruptOK(t, body)
	second := interruptOK(t, body)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if string(a) != string(b) {
		t.Fatalf("repeated analysis differs:\n%s\n%s", a, b)
	}
}
