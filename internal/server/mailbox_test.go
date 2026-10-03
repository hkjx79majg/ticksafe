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

func postMailbox(t *testing.T, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, mailboxAnalyzePath, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	Handler().ServeHTTP(rec, r)
	return rec
}

func mailboxOK(t *testing.T, body string) mboxResponse {
	t.Helper()
	rec := postMailbox(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp mboxResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func mbDecl(name string, cap int64) string {
	return `{"name":` + strconv.Quote(name) + `,"capacity":` + strconv.FormatInt(cap, 10) + `}`
}

func mbReq(horizon int64, mailboxes string, tasks ...string) string {
	return `{"horizon":` + strconv.FormatInt(horizon, 10) +
		`,"mailboxes":[` + mailboxes + `],"tasks":[` + strings.Join(tasks, ",") + `]}`
}

func mbTask(id string, prio, rel, dead int64, actions ...string) string {
	return `{"id":` + strconv.Quote(id) + `,"priority":` + strconv.FormatInt(prio, 10) +
		`,"release":` + strconv.FormatInt(rel, 10) +
		`,"deadline":` + strconv.FormatInt(dead, 10) +
		`,"actions":[` + strings.Join(actions, ",") + `]}`
}

func mbRun(n int64) string { return `{"run":` + strconv.FormatInt(n, 10) + `}` }
func mbSend(m, msg string) string {
	return `{"send":{"mailbox":` + strconv.Quote(m) + `,"message":` + strconv.Quote(msg) + `}}`
}
func mbRecv(m string) string { return `{"receive":` + strconv.Quote(m) + `}` }

func checkTimeline(t *testing.T, got []mboxTimelineInterval, want []mboxTimelineInterval) {
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

func TestMailboxFIFOQueue(t *testing.T) {
	// A enqueues x,y at t=0 and runs; B (lower priority) waits for the CPU,
	// then dequeues both in FIFO order at t=3.
	body := mbReq(50, mbDecl("M", 2),
		mbTask("A", 0, 0, 50, mbSend("M", "x"), mbSend("M", "y"), mbRun(3)),
		mbTask("B", 1, 1, 50, mbRecv("M"), mbRecv("M"), mbRun(2)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@5", resp.Status, resp.StoppedAt)
	}
	checkTimeline(t, resp.Timeline, []mboxTimelineInterval{
		{TaskID: "A", Start: 0, End: 3},
		{TaskID: "B", Start: 3, End: 5},
	})
	a, b := resp.Results[0], resp.Results[1]
	if a.State != "completed" || a.Executed != 3 || a.Completion == nil || *a.Completion != 3 ||
		a.BlockedAction != nil || a.BlockedOn != nil || len(a.Received) != 0 || a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "completed" || b.Executed != 2 || b.Completion == nil || *b.Completion != 5 {
		t.Fatalf("B result = %+v", b)
	}
	want := []mboxReceived{{Mailbox: "M", Message: "x", Time: 3}, {Mailbox: "M", Message: "y", Time: 3}}
	if len(b.Received) != len(want) {
		t.Fatalf("B received = %+v, want %+v", b.Received, want)
	}
	for i := range want {
		if b.Received[i] != want[i] {
			t.Fatalf("B received[%d] = %+v, want %+v", i, b.Received[i], want[i])
		}
	}
}

func TestMailboxDirectDelivery(t *testing.T) {
	// B blocks on an empty mailbox at t=0; A's send at t=1 is delivered
	// directly to B without entering the queue (B gets it at time 1). The
	// unblocked B has the higher priority and preempts A at that instant.
	body := mbReq(20, mbDecl("M", 1),
		mbTask("B", 0, 0, 20, mbRecv("M"), mbRun(2)),
		mbTask("A", 1, 1, 20, mbSend("M", "hï🌍"), mbRun(1)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 4 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@4", resp.Status, resp.StoppedAt)
	}
	checkTimeline(t, resp.Timeline, []mboxTimelineInterval{
		{TaskID: "B", Start: 1, End: 3},
		{TaskID: "A", Start: 3, End: 4},
	})
	b := resp.Results[0]
	if len(b.Received) != 1 || b.Received[0] != (mboxReceived{Mailbox: "M", Message: "hï🌍", Time: 1}) {
		t.Fatalf("B received = %+v", b.Received)
	}
	if b.State != "completed" || b.BlockedAction != nil || b.BlockedOn != nil {
		t.Fatalf("B result = %+v", b)
	}
}

func TestMailboxBlockedSenderRefill(t *testing.T) {
	// Capacity 1: A's second send blocks. B's receive takes "a" and refills
	// the tail with A's blocked "b", unblocking A; A (higher priority) then
	// preempts B at the same instant.
	body := mbReq(20, mbDecl("M", 1),
		mbTask("A", 0, 0, 20, mbSend("M", "a"), mbSend("M", "b"), mbRun(1)),
		mbTask("B", 1, 0, 20, mbRecv("M"), mbRecv("M"), mbRun(2)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 3 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@3", resp.Status, resp.StoppedAt)
	}
	checkTimeline(t, resp.Timeline, []mboxTimelineInterval{
		{TaskID: "A", Start: 0, End: 1},
		{TaskID: "B", Start: 1, End: 3},
	})
	a, b := resp.Results[0], resp.Results[1]
	if a.State != "completed" || a.Executed != 1 || a.Completion == nil || *a.Completion != 1 {
		t.Fatalf("A result = %+v", a)
	}
	want := []mboxReceived{{Mailbox: "M", Message: "a", Time: 0}, {Mailbox: "M", Message: "b", Time: 0}}
	if len(b.Received) != len(want) {
		t.Fatalf("B received = %+v, want %+v", b.Received, want)
	}
	for i := range want {
		if b.Received[i] != want[i] {
			t.Fatalf("B received[%d] = %+v, want %+v", i, b.Received[i], want[i])
		}
	}
}

func TestMailboxReceiverTieByInputOrder(t *testing.T) {
	// Y (higher priority) blocks first, X blocks second, both at t=0. The
	// same-instant tie is broken by input order, so X (earlier in the input)
	// receives the message even though Y blocked first. X then preempts S
	// (higher priority) at t=0; Y never gets a message and the simulation
	// stalls once nothing can run.
	body := mbReq(20, mbDecl("M", 2),
		mbTask("X", 1, 0, 20, mbRecv("M"), mbRun(5)),
		mbTask("Y", 0, 0, 20, mbRecv("M"), mbRun(5)),
		mbTask("S", 2, 0, 20, mbSend("M", "m"), mbRun(1)))
	resp := mailboxOK(t, body)
	if resp.Status != "stalled" || resp.StoppedAt != 6 {
		t.Fatalf("status=%q stoppedAt=%d, want stalled@6", resp.Status, resp.StoppedAt)
	}
	checkTimeline(t, resp.Timeline, []mboxTimelineInterval{
		{TaskID: "X", Start: 0, End: 5},
		{TaskID: "S", Start: 5, End: 6},
	})
	x, y, s := resp.Results[0], resp.Results[1], resp.Results[2]
	if x.State != "completed" || len(x.Received) != 1 ||
		x.Received[0] != (mboxReceived{Mailbox: "M", Message: "m", Time: 0}) {
		t.Fatalf("X result = %+v", x)
	}
	if y.State != "blocked" || y.BlockedAction == nil || *y.BlockedAction != "receive" ||
		y.BlockedOn == nil || *y.BlockedOn != "M" || len(y.Received) != 0 || y.DeadlineStatus != "missed" {
		t.Fatalf("Y result = %+v", y)
	}
	if s.State != "completed" || s.Executed != 1 || s.Completion == nil || *s.Completion != 6 {
		t.Fatalf("S result = %+v", s)
	}
}

func TestMailboxStalledReceivers(t *testing.T) {
	// Both tasks block on an empty mailbox at t=0 with nothing left to
	// release: stalled immediately.
	body := mbReq(10, mbDecl("M", 1),
		mbTask("A", 0, 0, 5, mbRecv("M")),
		mbTask("B", 1, 0, 50, mbRecv("M")))
	resp := mailboxOK(t, body)
	if resp.Status != "stalled" || resp.StoppedAt != 0 {
		t.Fatalf("status=%q stoppedAt=%d, want stalled@0", resp.Status, resp.StoppedAt)
	}
	if len(resp.Timeline) != 0 {
		t.Fatalf("timeline = %+v, want empty", resp.Timeline)
	}
	a, b := resp.Results[0], resp.Results[1]
	if a.State != "blocked" || a.BlockedAction == nil || *a.BlockedAction != "receive" ||
		a.BlockedOn == nil || *a.BlockedOn != "M" || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "blocked" || b.DeadlineStatus != "pending" {
		t.Fatalf("B result = %+v", b)
	}
}

func TestMailboxStalledSender(t *testing.T) {
	// Capacity 1 fills after the first send; the second send blocks forever.
	body := mbReq(10, mbDecl("M", 1),
		mbTask("A", 0, 0, 5, mbSend("M", "a"), mbSend("M", "b")))
	resp := mailboxOK(t, body)
	if resp.Status != "stalled" || resp.StoppedAt != 0 {
		t.Fatalf("status=%q stoppedAt=%d, want stalled@0", resp.Status, resp.StoppedAt)
	}
	a := resp.Results[0]
	if a.State != "blocked" || a.BlockedAction == nil || *a.BlockedAction != "send" ||
		a.BlockedOn == nil || *a.BlockedOn != "M" || a.Executed != 0 || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
}

func TestMailboxHorizon(t *testing.T) {
	body := mbReq(10, mbDecl("M", 1),
		mbTask("A", 0, 0, 10, mbRun(100)))
	resp := mailboxOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 10 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@10", resp.Status, resp.StoppedAt)
	}
	checkTimeline(t, resp.Timeline, []mboxTimelineInterval{{TaskID: "A", Start: 0, End: 10}})
	a := resp.Results[0]
	if a.State != "ready" || a.Executed != 10 || a.Completion != nil || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v", a)
	}
}

func TestMailboxRunEndsAtHorizon(t *testing.T) {
	// B blocks on the empty mailbox at t=0. A's run ends exactly at the
	// horizon; only A's trailing zero-duration send is processed, delivered
	// directly to B. B is unblocked but does not run at the horizon.
	body := mbReq(5, mbDecl("M", 1),
		mbTask("B", 0, 0, 5, mbRecv("M"), mbRun(1)),
		mbTask("A", 1, 0, 5, mbRun(5), mbSend("M", "z")))
	resp := mailboxOK(t, body)
	if resp.Status != "horizon" || resp.StoppedAt != 5 {
		t.Fatalf("status=%q stoppedAt=%d, want horizon@5", resp.Status, resp.StoppedAt)
	}
	checkTimeline(t, resp.Timeline, []mboxTimelineInterval{{TaskID: "A", Start: 0, End: 5}})
	b, a := resp.Results[0], resp.Results[1]
	if a.State != "completed" || a.Completion == nil || *a.Completion != 5 || a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v", a)
	}
	if b.State != "ready" || b.Executed != 0 || b.Completion != nil || b.DeadlineStatus != "missed" {
		t.Fatalf("B result = %+v", b)
	}
	if len(b.Received) != 1 || b.Received[0] != (mboxReceived{Mailbox: "M", Message: "z", Time: 5}) {
		t.Fatalf("B received = %+v", b.Received)
	}
}

func TestMailboxIdleJumpThenRefill(t *testing.T) {
	// A blocks on a full mailbox at t=0; the CPU idles until B releases at
	// t=5, whose receive unblocks A. A (higher priority) preempts at t=5.
	body := mbReq(30, mbDecl("M", 1),
		mbTask("A", 0, 0, 30, mbSend("M", "a"), mbSend("M", "b"), mbRun(1)),
		mbTask("B", 1, 5, 30, mbRecv("M"), mbRun(1)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 7 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@7", resp.Status, resp.StoppedAt)
	}
	checkTimeline(t, resp.Timeline, []mboxTimelineInterval{
		{TaskID: "A", Start: 5, End: 6},
		{TaskID: "B", Start: 6, End: 7},
	})
	b := resp.Results[1]
	if len(b.Received) != 1 || b.Received[0] != (mboxReceived{Mailbox: "M", Message: "a", Time: 5}) {
		t.Fatalf("B received = %+v", b.Received)
	}
}

func TestMailboxSamePriorityNoPreemption(t *testing.T) {
	// B (same priority, released later) never preempts A; C (strictly higher)
	// preempts at its release.
	body := mbReq(20, mbDecl("M", 1),
		mbTask("A", 1, 0, 20, mbRun(10)),
		mbTask("B", 1, 2, 20, mbRun(1)),
		mbTask("C", 0, 4, 20, mbRun(2)))
	resp := mailboxOK(t, body)
	if resp.Status != "completed" || resp.StoppedAt != 13 {
		t.Fatalf("status=%q stoppedAt=%d, want completed@13", resp.Status, resp.StoppedAt)
	}
	checkTimeline(t, resp.Timeline, []mboxTimelineInterval{
		{TaskID: "A", Start: 0, End: 4},
		{TaskID: "C", Start: 4, End: 6},
		{TaskID: "A", Start: 6, End: 12},
		{TaskID: "B", Start: 12, End: 13},
	})
}

func TestMailboxDeterministic(t *testing.T) {
	body := mbReq(30, mbDecl("M", 1)+","+mbDecl("N", 2),
		mbTask("A", 2, 0, 30, mbSend("M", "a"), mbSend("M", "b"), mbRun(3), mbSend("N", "c")),
		mbTask("B", 1, 2, 30, mbRecv("M"), mbRun(2), mbSend("N", "d")),
		mbTask("C", 0, 5, 30, mbRecv("N"), mbRecv("N"), mbRun(1)))
	first := mailboxOK(t, body)
	second := mailboxOK(t, body)
	fj, _ := json.Marshal(first)
	sj, _ := json.Marshal(second)
	if !bytes.Equal(fj, sj) {
		t.Fatalf("non-deterministic response:\n%s\n%s", fj, sj)
	}
}

func TestMailboxValidationFailures(t *testing.T) {
	good := mbTask("a", 0, 0, 20, mbSend("M", "x"), mbRecv("M"))
	base := func(inner string) string { return mbReq(20, mbDecl("M", 2), inner) }
	cases := map[string]string{
		"missing horizon":      `{"mailboxes":[` + mbDecl("M", 1) + `],"tasks":[` + good + `]}`,
		"zero horizon":         mbReq(0, mbDecl("M", 1), good),
		"horizon too large":    mbReq(1000000000001, mbDecl("M", 1), good),
		"missing mailboxes":    `{"horizon":20,"tasks":[` + good + `]}`,
		"empty mailboxes":      mbReq(20, "", good),
		"mailbox empty name":   mbReq(20, mbDecl("", 1), good),
		"mailbox whitespace":   mbReq(20, mbDecl("a b", 1), good),
		"mailbox dup name":     mbReq(20, mbDecl("M", 1)+","+mbDecl("M", 2), good),
		"capacity missing":     `{"horizon":20,"mailboxes":[{"name":"M"}],"tasks":[` + good + `]}`,
		"capacity zero":        mbReq(20, mbDecl("M", 0), good),
		"capacity negative":    mbReq(20, mbDecl("M", -1), good),
		"capacity too large":   mbReq(20, mbDecl("M", 65536), good),
		"missing tasks":        `{"horizon":20,"mailboxes":[` + mbDecl("M", 1) + `]}`,
		"empty tasks":          mbReq(20, mbDecl("M", 1)),
		"duplicate id":         base(good + "," + mbTask("a", 1, 0, 20, mbRun(1))),
		"priority negative":    base(mbTask("a", -1, 0, 20, mbRun(1))),
		"priority too large":   base(mbTask("a", 256, 0, 20, mbRun(1))),
		"release at horizon":   base(mbTask("a", 0, 20, 21, mbRun(1))),
		"deadline at release":  base(mbTask("a", 0, 0, 0, mbRun(1))),
		"missing actions":      base(`{"id":"a","priority":0,"release":0,"deadline":1}`),
		"empty actions":        base(mbTask("a", 0, 0, 20)),
		"no verb":              base(mbTask("a", 0, 0, 20, `{}`)),
		"two verbs":            base(mbTask("a", 0, 0, 20, `{"run":1,"receive":"M"}`)),
		"send and receive":     base(mbTask("a", 0, 0, 20, `{"send":{"mailbox":"M","message":"x"},"receive":"M"}`)),
		"run zero":             base(mbTask("a", 0, 0, 20, mbRun(0))),
		"run negative":         base(mbTask("a", 0, 0, 20, mbRun(-1))),
		"run null":             base(mbTask("a", 0, 0, 20, `{"run":null}`)),
		"run wrong type":       base(mbTask("a", 0, 0, 20, `{"run":"1"}`)),
		"receive null":         base(mbTask("a", 0, 0, 20, `{"receive":null}`)),
		"receive wrong type":   base(mbTask("a", 0, 0, 20, `{"receive":1}`)),
		"receive undeclared":   base(mbTask("a", 0, 0, 20, mbRecv("Q"))),
		"send null":            base(mbTask("a", 0, 0, 20, `{"send":null}`)),
		"send wrong type":      base(mbTask("a", 0, 0, 20, `{"send":"M"}`)),
		"send undeclared":      base(mbTask("a", 0, 0, 20, mbSend("Q", "x"))),
		"send missing mailbox": base(mbTask("a", 0, 0, 20, `{"send":{"message":"x"}}`)),
		"send missing message": base(mbTask("a", 0, 0, 20, `{"send":{"mailbox":"M"}}`)),
		"message empty":        base(mbTask("a", 0, 0, 20, mbSend("M", ""))),
		"message too long":     base(mbTask("a", 0, 0, 20, mbSend("M", strings.Repeat("é", 257)))),
		"run total overflow":   base(mbTask("a", 0, 0, 20, mbRun(maxInt64), mbRun(1))),
		"global run overflow":  base(mbTask("a", 0, 0, 20, mbRun(maxInt64)) + "," + mbTask("b", 0, 0, 20, mbRun(1))),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postMailbox(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	bad400 := map[string]string{
		"unknown top key":  `{"horizon":20,"bogus":1,"mailboxes":[],"tasks":[]}`,
		"unknown task key": base(`{"id":"a","priority":0,"release":0,"deadline":1,"execution":1,"actions":[{"run":1}]}`),
		"unknown action":   base(mbTask("a", 0, 0, 20, `{"sleep":1}`)),
		"unknown send key": base(mbTask("a", 0, 0, 20, `{"send":{"mailbox":"M","message":"x","extra":1}}`)),
		"unknown mbox key": `{"horizon":20,"mailboxes":[{"name":"M","capacity":1,"extra":1}],"tasks":[` + good + `]}`,
		"duplicate key":    `{"horizon":3,"horizon":4,"mailboxes":[],"tasks":[]}`,
		"trailing token":   base(good) + ` garbage`,
		"syntax error":     `{`,
	}
	for name, body := range bad400 {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postMailbox(t, body, "application/json"),
				http.StatusBadRequest, "invalid_json")
		})
	}
}

func TestMailboxBoundaryValuesAccepted(t *testing.T) {
	// Capacity 65535, 256-rune message and 1024 actions are all in range.
	acts := make([]string, 0, 1024)
	for i := 0; i < 1023; i++ {
		acts = append(acts, mbSend("M", strings.Repeat("é", 256)))
	}
	acts = append(acts, mbRun(1))
	body := mbReq(5, mbDecl("M", 65535), mbTask("a", 0, 0, 5, acts...))
	if rec := postMailbox(t, body, "application/json"); rec.Code != http.StatusOK {
		t.Fatalf("boundary values rejected: %d %s", rec.Code, rec.Body.String())
	}

	// 1025 actions and 257 mailboxes are out of range.
	acts = append(acts, mbRun(1))
	assertErrorCode(t, postMailbox(t, mbReq(5, mbDecl("M", 1), mbTask("a", 0, 0, 5, acts...)), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
	var decls []string
	for i := 0; i < 257; i++ {
		decls = append(decls, mbDecl("m"+strconv.Itoa(i), 1))
	}
	assertErrorCode(t, postMailbox(t, mbReq(5, strings.Join(decls, ","), mbTask("a", 0, 0, 5, mbRun(1))), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")
}

func TestMailboxMethodAndMediaType(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(method, mailboxAnalyzePath, nil))
		assertErrorCode(t, rec, http.StatusMethodNotAllowed, "method_not_allowed")
		if got := rec.Header().Get("Allow"); got != http.MethodPost {
			t.Fatalf("Allow = %q, want POST", got)
		}
	}
	good := mbReq(3, mbDecl("M", 1), mbTask("a", 0, 0, 3, mbRun(1)))
	assertErrorCode(t, postMailbox(t, good, ""),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	assertErrorCode(t, postMailbox(t, good, "text/plain"),
		http.StatusUnsupportedMediaType, "unsupported_media_type")
	if rec := postMailbox(t, good, "application/json; charset=utf-8"); rec.Code != http.StatusOK {
		t.Fatalf("charset parameter rejected: %d %s", rec.Code, rec.Body.String())
	}
}

// ---- Differential test against an independent tick-by-tick reference ------

type mbRefTask struct {
	id                      string
	prio, release, deadline int64
	acts                    []mboxAction
}

type mbRefSim struct {
	tasks    []mbRefTask
	n        int
	horizon  int64
	capacity map[string]int64

	queue     map[string][]string
	senders   map[string][]int
	receivers map[string][]int

	blockedWhat []string
	blockedM    []string
	blockTime   []int64
	pc          []int
	rem         []int64
	released    []bool
	done        []bool
	executed    []int64
	finish      []*int64
	received    [][]mboxReceived
	now         int64
	current     int
	segs        []mboxSegment
	stoppedAt   int64
}

func mbRefRun(tasks []mbRefTask, capacity map[string]int64, horizon int64) mboxResponse {
	r := &mbRefSim{
		tasks: tasks, n: len(tasks), horizon: horizon, capacity: capacity,
		queue: map[string][]string{}, senders: map[string][]int{}, receivers: map[string][]int{},
		current: -1,
	}
	r.blockedWhat = make([]string, r.n)
	r.blockedM = make([]string, r.n)
	r.blockTime = make([]int64, r.n)
	r.pc = make([]int, r.n)
	r.rem = make([]int64, r.n)
	r.released = make([]bool, r.n)
	r.done = make([]bool, r.n)
	r.executed = make([]int64, r.n)
	r.finish = make([]*int64, r.n)
	r.received = make([][]mboxReceived, r.n)

	stopped := false
	for t := int64(0); t < horizon && !stopped; t++ {
		r.now = t
		for i := 0; i < r.n; i++ {
			if tasks[i].release == t {
				r.released[i] = true
			}
		}
		r.settlePoint()
		if r.current == -1 {
			// Idle tick: with no future release nothing can change any more.
			future := false
			for i := 0; i < r.n; i++ {
				if tasks[i].release > t {
					future = true
					break
				}
			}
			if !future {
				r.stoppedAt = t
				stopped = true
			}
			continue
		}
		cur := r.current
		r.rem[cur]--
		r.executed[cur]++
		r.segs = append(r.segs, mboxSegment{index: cur, start: t, end: t + 1})
		if r.rem[cur] == 0 {
			// Keep current set: at the next instant settlePoint drains this
			// task's zero-duration chain before any (re)scheduling.
			r.pc[cur]++
		}
	}
	if !stopped {
		// Boundary instant at t == horizon: drain the just-finished task's
		// zero-duration chain once (matches the boundary rule).
		r.stoppedAt = horizon
		r.now = horizon
		if r.current != -1 {
			i := r.current
			if !r.done[i] && r.blockedM[i] == "" && r.rem[i] == 0 {
				r.settle(i)
			}
		}
	}
	return r.build()
}

// settlePoint performs all zero-time resolution at the current instant and
// leaves r.current on the task that owns the CPU during the next tick.
func (r *mbRefSim) settlePoint() {
	for {
		if r.current != -1 {
			r.settle(r.current)
			if r.done[r.current] || r.blockedM[r.current] != "" || r.rem[r.current] == 0 {
				r.current = -1
			}
		}
		if r.current == -1 {
			best := r.pick()
			if best == -1 {
				return
			}
			r.current = best
			r.settle(best)
			if r.done[best] || r.blockedM[best] != "" || r.rem[best] == 0 {
				r.current = -1
				continue
			}
		}
		// Same-instant preemption: the selected task's zero-duration chain can
		// unblock a strictly higher-priority task.
		resolved := false
		for !resolved {
			b := r.pick()
			if b != -1 && b != r.current && r.tasks[b].prio < r.tasks[r.current].prio {
				r.current = b
				r.settle(b)
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
		return
	}
}

func (r *mbRefSim) settle(i int) {
	tk := r.tasks[i]
	for r.pc[i] < len(tk.acts) {
		a := tk.acts[r.pc[i]]
		switch a.kind {
		case mboxActRun:
			if r.rem[i] == 0 {
				r.rem[i] = a.run
			}
			return
		case mboxActSend:
			m := a.mailbox
			if rs := r.receivers[m]; len(rs) > 0 {
				w := earliestBlocked(rs, r.blockTime)
				r.receivers[m] = removeBlocked(rs, w)
				r.unblock(w)
				r.received[w] = append(r.received[w], mboxReceived{Mailbox: m, Message: a.message, Time: r.now})
				r.pc[i]++
				continue
			}
			if int64(len(r.queue[m])) < r.capacity[m] {
				r.queue[m] = append(r.queue[m], a.message)
				r.pc[i]++
				continue
			}
			r.block(i, "send", m)
			return
		case mboxActReceive:
			m := a.mailbox
			q := r.queue[m]
			if len(q) == 0 {
				r.block(i, "receive", m)
				return
			}
			r.queue[m] = q[1:]
			r.received[i] = append(r.received[i], mboxReceived{Mailbox: m, Message: q[0], Time: r.now})
			if int64(len(q)) == r.capacity[m] {
				if sd := r.senders[m]; len(sd) > 0 {
					w := earliestBlocked(sd, r.blockTime)
					r.senders[m] = removeBlocked(sd, w)
					r.queue[m] = append(r.queue[m], r.tasks[w].acts[r.pc[w]].message)
					r.unblock(w)
				}
			}
			r.pc[i]++
		}
	}
	r.done[i] = true
	at := r.now
	r.finish[i] = &at
}

func (r *mbRefSim) block(i int, what, m string) {
	r.blockedWhat[i] = what
	r.blockedM[i] = m
	r.blockTime[i] = r.now
	if what == "send" {
		r.senders[m] = append(r.senders[m], i)
	} else {
		r.receivers[m] = append(r.receivers[m], i)
	}
}

func (r *mbRefSim) unblock(i int) {
	r.blockedWhat[i] = ""
	r.blockedM[i] = ""
	r.pc[i]++
}

func (r *mbRefSim) pick() int {
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
		if a.prio < b.prio ||
			(a.prio == b.prio && a.release < b.release) ||
			(a.prio == b.prio && a.release == b.release && i < best) {
			best = i
		}
	}
	return best
}

func (r *mbRefSim) build() mboxResponse {
	resp := mboxResponse{Timeline: []mboxTimelineInterval{}}
	for _, seg := range r.segs {
		last := len(resp.Timeline) - 1
		if last >= 0 &&
			resp.Timeline[last].TaskID == r.tasks[seg.index].id &&
			resp.Timeline[last].End == seg.start {
			resp.Timeline[last].End = seg.end
			continue
		}
		resp.Timeline = append(resp.Timeline, mboxTimelineInterval{
			TaskID: r.tasks[seg.index].id, Start: seg.start, End: seg.end,
		})
	}
	resp.Results = make([]mboxTaskResult, r.n)
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
		var ba, bo *string
		if state == "blocked" {
			w := r.blockedWhat[i]
			ba = &w
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
		received := r.received[i]
		if received == nil {
			received = []mboxReceived{}
		}
		resp.Results[i] = mboxTaskResult{
			Executed: r.executed[i], Completion: r.finish[i],
			State: state, BlockedAction: ba, BlockedOn: bo,
			Received: received, DeadlineStatus: ds,
		}
	}
	switch {
	case allDone:
		resp.Status = "completed"
	case r.stoppedAt < r.horizon:
		resp.Status = "stalled"
	default:
		resp.Status = "horizon"
	}
	resp.StoppedAt = r.stoppedAt
	return resp
}

func TestMailboxDifferential(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	for iter := 0; iter < 2000; iter++ {
		nmb := 1 + rng.Intn(3)
		capacity := map[string]int64{}
		mbNames := make([]string, 0, nmb)
		for k := 0; k < nmb; k++ {
			name := "mb" + strconv.Itoa(k)
			mbNames = append(mbNames, name)
			capacity[name] = int64(1 + rng.Intn(3))
		}
		n := 1 + rng.Intn(5)
		horizon := int64(2 + rng.Intn(24))
		got := make([]mboxTask, n)
		ref := make([]mbRefTask, n)
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
			acts := make([]mboxAction, 0, nacts)
			for k := 0; k < nacts; k++ {
				switch choice := rng.Intn(10); {
				case choice < 5:
					acts = append(acts, mboxAction{kind: mboxActRun, run: int64(1 + rng.Intn(5))})
				case choice < 8:
					m := mbNames[rng.Intn(len(mbNames))]
					msg := string(rune('a' + rng.Intn(4)))
					acts = append(acts, mboxAction{kind: mboxActSend, mailbox: m, message: msg})
				default:
					m := mbNames[rng.Intn(len(mbNames))]
					acts = append(acts, mboxAction{kind: mboxActReceive, mailbox: m})
				}
			}
			got[i] = mboxTask{id: id, priority: prio, release: rel, deadline: dead, actions: acts}
			ref[i] = mbRefTask{id: id, prio: prio, release: rel, deadline: dead, acts: acts}
		}

		want := mbRefRun(ref, capacity, horizon)
		resp := simulateMailbox(got, capacity, horizon)

		if resp.Status != want.Status || resp.StoppedAt != want.StoppedAt {
			t.Fatalf("iter %d seed %d: status %q@%d vs %q@%d\ngot tl %+v",
				iter, seed, resp.Status, resp.StoppedAt, want.Status, want.StoppedAt, resp.Timeline)
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
			if (g.BlockedAction == nil) != (w.BlockedAction == nil) ||
				(g.BlockedAction != nil && *g.BlockedAction != *w.BlockedAction) ||
				(g.BlockedOn == nil) != (w.BlockedOn == nil) ||
				(g.BlockedOn != nil && *g.BlockedOn != *w.BlockedOn) {
				t.Fatalf("iter %d seed %d: %s blocked %v/%v vs %v/%v",
					iter, seed, ref[i].id, g.BlockedAction, g.BlockedOn, w.BlockedAction, w.BlockedOn)
			}
			if len(g.Received) != len(w.Received) {
				t.Fatalf("iter %d seed %d: %s received %+v vs %+v", iter, seed, ref[i].id, g.Received, w.Received)
			}
			for k := range w.Received {
				if g.Received[k] != w.Received[k] {
					t.Fatalf("iter %d seed %d: %s received[%d] %+v vs %+v",
						iter, seed, ref[i].id, k, g.Received[k], w.Received[k])
				}
			}
		}
	}
}
