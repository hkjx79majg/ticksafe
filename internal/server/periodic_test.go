package server

import (
	"bytes"
	"encoding/json"
	"math/big"
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

func periodicOK(t *testing.T, body string) periodicResponse {
	t.Helper()
	rec := postPeriodic(t, body, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp periodicResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp
}

func periodicTaskObj(id string, prio, exec, period, deadline int64) string {
	return `{"id":"` + id + `","priority":` + strconv.FormatInt(prio, 10) +
		`,"execution":` + strconv.FormatInt(exec, 10) +
		`,"period":` + strconv.FormatInt(period, 10) +
		`,"deadline":` + strconv.FormatInt(deadline, 10) + `}`
}

func TestPeriodicHandComputedResponseTimes(t *testing.T) {
	// A (low): C=2,T=6; B (high): C=1,T=4. RTA for A: 2 -> 3 -> 3, so 3.
	resp := periodicOK(t, `{"tasks":[
		{"id":"A","priority":2,"execution":2,"period":6,"deadline":6},
		{"id":"B","priority":1,"execution":1,"period":4,"deadline":4}
	]}`)
	if !resp.Schedulable {
		t.Fatalf("schedulable = false, want true: %+v", resp.Results)
	}
	wantA, wantB := int64(3), int64(1)
	if len(resp.Results) != 2 {
		t.Fatalf("results = %+v", resp.Results)
	}
	if resp.Results[0].ID != "A" || resp.Results[0].ResponseTime == nil ||
		*resp.Results[0].ResponseTime != wantA || resp.Results[0].DeadlineStatus != "met" {
		t.Fatalf("A result = %+v, want responseTime %d met", resp.Results[0], wantA)
	}
	if resp.Results[1].ID != "B" || resp.Results[1].ResponseTime == nil ||
		*resp.Results[1].ResponseTime != wantB || resp.Results[1].DeadlineStatus != "met" {
		t.Fatalf("B result = %+v, want responseTime %d met", resp.Results[1], wantB)
	}
}

func TestPeriodicMissYieldsNullAndUnschedulable(t *testing.T) {
	// B: C=1,T=2; C: C=1,T=3; A: C=3,D=T=7. A's iteration walks 3 -> 6 -> 8
	// (>7), so responseTime is null. C itself suffers interference from B:
	// 1 -> 2 -> 2, so its response time is 2, not 1.
	resp := periodicOK(t, `{"tasks":[
		{"id":"A","priority":2,"execution":3,"period":7,"deadline":7},
		{"id":"B","priority":0,"execution":1,"period":2,"deadline":2},
		{"id":"C","priority":1,"execution":1,"period":3,"deadline":3}
	]}`)
	if resp.Schedulable {
		t.Fatalf("schedulable = true, want false: %+v", resp.Results)
	}
	if len(resp.Results) != 3 || resp.Results[0].ID != "A" || resp.Results[1].ID != "B" || resp.Results[2].ID != "C" {
		t.Fatalf("results must stay in input order: %+v", resp.Results)
	}
	a := resp.Results[0]
	if a.ResponseTime != nil || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v, want null/missed", a)
	}
	b, c := resp.Results[1], resp.Results[2]
	if b.ResponseTime == nil || *b.ResponseTime != 1 || b.DeadlineStatus != "met" {
		t.Fatalf("B result = %+v, want 1/met", b)
	}
	if c.ResponseTime == nil || *c.ResponseTime != 2 || c.DeadlineStatus != "met" {
		t.Fatalf("C result = %+v, want 2/met", c)
	}
}

func TestPeriodicResponseExactlyDeadlineIsMet(t *testing.T) {
	// A: C=1 with hp B C=1,T=2: 1 -> 2 -> 2 (fixed), meeting a deadline
	// equal to the response time.
	resp := periodicOK(t, `{"tasks":[
		{"id":"A","priority":1,"execution":1,"period":3,"deadline":2},
		{"id":"B","priority":0,"execution":1,"period":2,"deadline":2}
	]}`)
	if !resp.Schedulable {
		t.Fatalf("schedulable = false, want true: %+v", resp.Results)
	}
	a := resp.Results[0]
	if a.ResponseTime == nil || *a.ResponseTime != 2 || a.DeadlineStatus != "met" {
		t.Fatalf("A result = %+v, want 2/met", a)
	}
}

func TestPeriodicConstrainedDeadlineMiss(t *testing.T) {
	// D < T: C=2,T=10,D=2 with hp C=1,T=3 gives 2 -> 3 (>2), a miss even
	// though the task would meet its period.
	resp := periodicOK(t, `{"tasks":[
		{"id":"A","priority":1,"execution":2,"period":10,"deadline":2},
		{"id":"B","priority":0,"execution":1,"period":3,"deadline":3}
	]}`)
	if resp.Schedulable {
		t.Fatalf("schedulable = true, want false")
	}
	a := resp.Results[0]
	if a.ResponseTime != nil || a.DeadlineStatus != "missed" {
		t.Fatalf("A result = %+v, want null/missed", a)
	}
}

func TestPeriodicSingleTaskAndExecutionPastDeadline(t *testing.T) {
	resp := periodicOK(t, `{"tasks":[
		{"id":"solo","priority":0,"execution":5,"period":10,"deadline":10}
	]}`)
	r := resp.Results[0]
	if !resp.Schedulable || r.ResponseTime == nil || *r.ResponseTime != 5 || r.DeadlineStatus != "met" {
		t.Fatalf("solo result = %+v", r)
	}

	// Initial value C already past the deadline.
	resp = periodicOK(t, `{"tasks":[
		{"id":"big","priority":0,"execution":11,"period":100,"deadline":10}
	]}`)
	r = resp.Results[0]
	if resp.Schedulable || r.ResponseTime != nil || r.DeadlineStatus != "missed" {
		t.Fatalf("big result = %+v, want null/missed", r)
	}
}

func TestPeriodicOrderIndependenceAndDeterminism(t *testing.T) {
	body := `{"tasks":[
		{"id":"A","priority":2,"execution":3,"period":7,"deadline":7},
		{"id":"B","priority":0,"execution":1,"period":2,"deadline":2},
		{"id":"C","priority":1,"execution":1,"period":3,"deadline":3}
	]}`
	shuffled := `{"tasks":[
		{"id":"C","priority":1,"execution":1,"period":3,"deadline":3},
		{"id":"A","priority":2,"execution":3,"period":7,"deadline":7},
		{"id":"B","priority":0,"execution":1,"period":2,"deadline":2}
	]}`
	byID := func(resp periodicResponse) map[string]periodicTaskResult {
		m := make(map[string]periodicTaskResult, len(resp.Results))
		for _, r := range resp.Results {
			m[r.ID] = r
		}
		return m
	}
	first := periodicOK(t, body)
	second := periodicOK(t, body)
	reordered := periodicOK(t, shuffled)
	a, b := byID(first), byID(reordered)
	for id, want := range a {
		got := b[id]
		if (got.ResponseTime == nil) != (want.ResponseTime == nil) ||
			(got.ResponseTime != nil && *got.ResponseTime != *want.ResponseTime) ||
			got.DeadlineStatus != want.DeadlineStatus {
			t.Fatalf("task %s differs across orderings: %+v vs %+v", id, got, want)
		}
	}
	// Same request is field-for-field identical, including input order.
	for i := range first.Results {
		x, y := first.Results[i], second.Results[i]
		if x.ID != y.ID || x.DeadlineStatus != y.DeadlineStatus ||
			(x.ResponseTime == nil) != (y.ResponseTime == nil) ||
			(x.ResponseTime != nil && *x.ResponseTime != *y.ResponseTime) {
			t.Fatalf("non-deterministic result at %d: %+v vs %+v", i, x, y)
		}
	}
	if got := reordered.Results[0].ID; got != "C" {
		t.Fatalf("reordered results[0] = %q, want C", got)
	}
}

func TestPeriodicOverSubscribedSetMissesImmediately(t *testing.T) {
	// The high-priority task alone saturates the processor (C/T = 1), so the
	// low task has no finite fixed point: it must miss without iterating up to
	// the (1e12) deadline. Measured wall time guards that fast path.
	body := `{"tasks":[
		{"id":"low","priority":1,"execution":1,"period":1000000000000,"deadline":1000000000000},
		{"id":"hi","priority":0,"execution":1,"period":1,"deadline":1}
	]}`
	start := time.Now()
	resp := periodicOK(t, body)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("analysis of an oversubscribed set took %s", elapsed)
	}
	if resp.Schedulable {
		t.Fatalf("schedulable = true, want false")
	}
	low := resp.Results[0]
	if low.ResponseTime != nil || low.DeadlineStatus != "missed" {
		t.Fatalf("low result = %+v, want null/missed", low)
	}
	hi := resp.Results[1]
	if hi.ResponseTime == nil || *hi.ResponseTime != 1 || hi.DeadlineStatus != "met" {
		t.Fatalf("hi result = %+v, want 1/met", hi)
	}

	// Exactly 1/3+1/3+1/3 == 1 must be recognized as the boundary: exact
	// rational arithmetic, no floating-point misclassification.
	resp = periodicOK(t, `{"tasks":[
		{"id":"low","priority":3,"execution":1,"period":9,"deadline":9},
		{"id":"h1","priority":0,"execution":1,"period":3,"deadline":3},
		{"id":"h2","priority":1,"execution":1,"period":3,"deadline":3},
		{"id":"h3","priority":2,"execution":1,"period":3,"deadline":3}
	]}`)
	if resp.Schedulable || resp.Results[0].ResponseTime != nil || resp.Results[0].DeadlineStatus != "missed" {
		t.Fatalf("utilization == 1 set: %+v", resp.Results)
	}
	for i := 1; i <= 3; i++ {
		r := resp.Results[i]
		if r.ResponseTime == nil || r.DeadlineStatus != "met" {
			t.Fatalf("%s = %+v, want met", r.ID, r)
		}
	}
}

func TestPeriodicLargeBoundaryValues(t *testing.T) {
	// The largest legal execution meeting the largest legal deadline.
	resp := periodicOK(t, `{"tasks":[
		{"id":"max","priority":0,"execution":1000000000000,"period":1000000000000,"deadline":1000000000000}
	]}`)
	mr := resp.Results[0]
	if !resp.Schedulable || mr.ResponseTime == nil || *mr.ResponseTime != 1_000_000_000_000 {
		t.Fatalf("max result = %+v", mr)
	}

	// Miss by one unit: the first interference step lands just past D.
	resp = periodicOK(t, `{"tasks":[
		{"id":"low","priority":1,"execution":1,"period":1000000000000,"deadline":1000000000000},
		{"id":"hi","priority":0,"execution":1000000000000,"period":1000000000000,"deadline":1000000000000}
	]}`)
	if resp.Schedulable {
		t.Fatalf("schedulable = true, want false")
	}
	if low := resp.Results[0]; low.ResponseTime != nil || low.DeadlineStatus != "missed" {
		t.Fatalf("low result = %+v, want null/missed", low)
	}
	if hi := resp.Results[1]; hi.ResponseTime == nil || *hi.ResponseTime != 1_000_000_000_000 {
		t.Fatalf("hi result = %+v", hi)
	}
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
	rec := postPeriodic(t, `{"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2}]}`,
		"application/json; charset=utf-8")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestPeriodicInvalidJSON(t *testing.T) {
	cases := map[string]string{
		"empty":              "",
		"syntax error":       "{",
		"trailing token":     `{"tasks":[]} garbage`,
		"unknown field":      `{"tasks":[],"bogus":1}`,
		"unknown task field": `{"tasks":[{"id":"a","priority":0,"execution":1,"period":2,"deadline":2,"release":0}]}`,
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

	// A body larger than 1 MiB (whitespace included) is invalid_json, checked
	// before schema validation.
	objs := periodicTaskObj("a", 0, 1, 2, 2)
	padded := `{"tasks":[` + objs + strings.Repeat(" ", maxBodyBytes) + `]}`
	assertErrorCode(t, postPeriodic(t, padded, "application/json"),
		http.StatusBadRequest, "invalid_json")
}

func TestPeriodicValidationFailures(t *testing.T) {
	good := `{"id":"a","priority":0,"execution":1,"period":2,"deadline":2}`
	wrap := func(task string) string { return `{"tasks":[` + task + `]}` }
	cases := map[string]string{
		"json null body":        `null`,
		"json array body":       `[]`,
		"missing tasks":         `{}`,
		"tasks null":            `{"tasks":null}`,
		"empty tasks":           `{"tasks":[]}`,
		"tasks not an array":    `{"tasks":{}}`,
		"missing id":            `{"tasks":[{"priority":0,"execution":1,"period":2,"deadline":2}]}`,
		"missing priority":      `{"tasks":[{"id":"a","execution":1,"period":2,"deadline":2}]}`,
		"missing execution":     `{"tasks":[{"id":"a","priority":0,"period":2,"deadline":2}]}`,
		"missing period":        `{"tasks":[{"id":"a","priority":0,"execution":1,"deadline":2}]}`,
		"missing deadline":      `{"tasks":[{"id":"a","priority":0,"execution":1,"period":2}]}`,
		"null field":            `{"tasks":[{"id":"a","priority":null,"execution":1,"period":2,"deadline":2}]}`,
		"wrong field type":      wrap(strings.Replace(good, `"priority":0`, `"priority":"0"`, 1)),
		"execution wrong type":  wrap(strings.Replace(good, `"execution":1`, `"execution":true`, 1)),
		"empty id":              wrap(strings.Replace(good, `"id":"a"`, `"id":""`, 1)),
		"whitespace id":         wrap(strings.Replace(good, `"id":"a"`, `"id":"a b"`, 1)),
		"duplicate id":          `{"tasks":[` + periodicTaskObj("a", 0, 1, 2, 2) + "," + periodicTaskObj("a", 1, 1, 2, 2) + `]}`,
		"priority negative":     wrap(strings.Replace(good, `"priority":0`, `"priority":-1`, 1)),
		"priority too large":    wrap(strings.Replace(good, `"priority":0`, `"priority":256`, 1)),
		"duplicate priority":    `{"tasks":[` + periodicTaskObj("a", 0, 1, 2, 2) + "," + periodicTaskObj("b", 0, 1, 3, 3) + `]}`,
		"execution zero":        wrap(strings.Replace(good, `"execution":1`, `"execution":0`, 1)),
		"execution negative":    wrap(strings.Replace(good, `"execution":1`, `"execution":-1`, 1)),
		"execution too large":   wrap(strings.Replace(good, `"execution":1`, `"execution":1000000000001`, 1)),
		"period zero":           wrap(strings.Replace(good, `"period":2`, `"period":0`, 1)),
		"period too large":      wrap(strings.Replace(good, `"period":2`, `"period":1000000000001`, 1)),
		"deadline zero":         wrap(strings.Replace(good, `"deadline":2`, `"deadline":0`, 1)),
		"deadline too large":    wrap(strings.Replace(good, `"deadline":2`, `"deadline":1000000000001`, 1)),
		"deadline after period": wrap(strings.Replace(good, `"deadline":2`, `"deadline":3`, 1)),
		"number out of int64":   wrap(strings.Replace(good, `"period":2`, `"period":99999999999999999999999`, 1)),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, postPeriodic(t, body, "application/json"),
				http.StatusUnprocessableEntity, "validation_failed")
		})
	}

	// 256 tasks is the upper bound; 257 fails.
	var many bytes.Buffer
	many.WriteString(`{"tasks":[`)
	for i := 0; i < 257; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		many.WriteString(periodicTaskObj("t"+strconv.Itoa(i), int64(i), 1, 1000, 1000))
	}
	many.WriteString(`]}`)
	assertErrorCode(t, postPeriodic(t, many.String(), "application/json"),
		http.StatusUnprocessableEntity, "validation_failed")

	// Boundary: 256 tasks with priorities 0..255 succeeds.
	var ok bytes.Buffer
	ok.WriteString(`{"tasks":[`)
	for i := 0; i < 256; i++ {
		if i > 0 {
			ok.WriteByte(',')
		}
		ok.WriteString(periodicTaskObj("t"+strconv.Itoa(i), int64(i), 1, 1000, 1000))
	}
	ok.WriteString(`]}`)
	periodicOK(t, ok.String())
}

func TestPeriodicNoPartialResultsOnError(t *testing.T) {
	rec := postPeriodic(t, `{"tasks":[{"id":"a"`, "application/json")
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_json")
	body := rec.Body.String()
	if strings.Contains(body, "responseTime") || strings.Contains(body, "results") {
		t.Fatalf("error response leaked partial results: %s", body)
	}
}

// TestWorstResponseTimeDifferential compares the bounded fixed-point
// computation against a direct iteration of the RTA recurrence on random
// small task sets.
func TestWorstResponseTimeDifferential(t *testing.T) {
	seed := rand.New(rand.NewSource(0x5eed))
	for iter := 0; iter < 5000; iter++ {
		n := 1 + seed.Intn(6)
		tasks := make([]periodicTask, n)
		prios := seed.Perm(n)
		large := iter%3 == 0
		for i := 0; i < n; i++ {
			c := int64(1 + seed.Intn(5))
			if large {
				c = int64(1 + seed.Intn(50))
			}
			tp := c + int64(seed.Intn(map[bool]int{true: 4000, false: 20}[large]))
			d := c + int64(seed.Intn(int(tp-c+1)))
			if large {
				// occasionally a wide gap between deadline and period
				if seed.Intn(4) == 0 {
					d = c + int64(seed.Intn(int(tp-c+1)))
				}
			}
			tasks[i] = periodicTask{
				id:        "t" + strconv.Itoa(i),
				priority:  int64(prios[i]),
				execution: c,
				period:    tp,
				deadline:  d,
			}
		}
		for i, tk := range tasks {
			var hp []periodicTask
			u := new(big.Rat)
			for _, h := range tasks {
				if h.priority < tk.priority {
					hp = append(hp, h)
					u.Add(u, new(big.Rat).SetFrac(big.NewInt(h.execution), big.NewInt(h.period)))
				}
			}
			got := worstResponseTime(tk, hp)

			// Reference: iterate w0=C, w_{k+1}=C+sum ceil(w/Tj)*Cj until a
			// fixed point or a value past the deadline. When hp utilization is
			// at least 1 there is no fixed point; small random deadlines keep
			// the reference walk bounded.
			var want *int64
			w := tk.execution
			if u.Cmp(big.NewRat(1, 1)) < 0 && w <= tk.deadline {
				for steps := 0; steps <= int(tk.deadline)+1; steps++ {
					next := tk.execution
					for _, h := range hp {
						next += ((w + h.period - 1) / h.period) * h.execution
					}
					if next == w {
						v := w
						want = &v
						break
					}
					if next > tk.deadline {
						break
					}
					w = next
				}
			}

			if (got == nil) != (want == nil) {
				t.Fatalf("iter %d task %d %+v hp=%+v: got %v, want %v",
					iter, i, tk, hp, got, want)
			}
			if got != nil && *got != *want {
				t.Fatalf("iter %d task %d %+v: got %d, want %d", iter, i, tk, *got, *want)
			}
		}
	}
}

func TestPeriodicGeometricFixedPointBoundary(t *testing.T) {
	// hp tasks C=1 at periods 2,4,...,2^m: own R = 2^m. With m=39 every
	// period is legal (<= 1e12) and R = 549755813888.
	const m = 39
	r := int64(1) << m
	build := func(deadline int64) string {
		var b strings.Builder
		b.WriteString(`{"tasks":[{"id":"low","priority":255,"execution":1,"period":1000000000000,"deadline":` +
			strconv.FormatInt(deadline, 10) + `}`)
		for j := 0; j < m; j++ {
			tp := int64(1) << uint(j+1)
			b.WriteString("," + periodicTaskObj("h"+strconv.Itoa(j), int64(j), 1, tp, tp))
		}
		b.WriteString("]}")
		return b.String()
	}
	// Deadline equal to the fixed point meets exactly.
	resp := periodicOK(t, build(r))
	low := resp.Results[0]
	if !resp.Schedulable || low.ResponseTime == nil || *low.ResponseTime != r {
		t.Fatalf("D=R: %+v schedulable=%v", low, resp.Schedulable)
	}
	// One time unit tighter: the same fixed point now misses.
	resp = periodicOK(t, build(r-1))
	low = resp.Results[0]
	if resp.Schedulable || low.ResponseTime != nil || low.DeadlineStatus != "missed" {
		t.Fatalf("D=R-1: %+v schedulable=%v", low, resp.Schedulable)
	}
}

func TestPeriodicNearSaturationExact(t *testing.T) {
	// d = 1-U = 1e-12 with own C=1, D=1e12: g(1e12)=1e12 is a fixed point
	// meeting the deadline exactly; high-precision path must not round it off.
	resp := periodicOK(t, `{"tasks":[
		{"id":"low","priority":1,"execution":1,"period":1000000000000,"deadline":1000000000000},
		{"id":"hi","priority":0,"execution":999999999999,"period":1000000000000,"deadline":1000000000000}
	]}`)
	low := resp.Results[0]
	if !resp.Schedulable || low.ResponseTime == nil || *low.ResponseTime != 1_000_000_000_000 {
		t.Fatalf("near-saturation result: %+v schedulable=%v", low, resp.Schedulable)
	}
}

func TestPeriodicUtilizationOneBoundaryVariants(t *testing.T) {
	// 1/2 + 1/3 + 1/6 = 1 exactly: lowest-priority task must miss, and the
	// exact-rational gray-zone path must classify it (no float shortcut).
	resp := periodicOK(t, `{"tasks":[
		{"id":"low","priority":3,"execution":1,"period":100,"deadline":100},
		{"id":"a","priority":0,"execution":1,"period":2,"deadline":2},
		{"id":"b","priority":1,"execution":1,"period":3,"deadline":3},
		{"id":"c","priority":2,"execution":1,"period":6,"deadline":6}
	]}`)
	if resp.Schedulable {
		t.Fatalf("schedulable = true, want false: %+v", resp.Results)
	}
	if low := resp.Results[0]; low.ResponseTime != nil || low.DeadlineStatus != "missed" {
		t.Fatalf("low = %+v", low)
	}
	for i := 1; i <= 3; i++ {
		if r := resp.Results[i]; r.ResponseTime == nil || r.DeadlineStatus != "met" {
			t.Fatalf("%s = %+v, want met", r.ID, r)
		}
	}
}
