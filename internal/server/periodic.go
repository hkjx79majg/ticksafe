package server

import (
	"encoding/json"
	"math/big"
	"math/bits"
	"net/http"
	"sort"
)

const (
	periodicAnalyzePath = "/v1/schedules/periodic/analyze"
	maxPeriodicField    = 1_000_000_000_000
	maxInt64Uint        = uint64(1<<63 - 1)
)

type periodicTaskIn struct {
	ID        string `json:"id"`
	Priority  *int64 `json:"priority"`
	Execution *int64 `json:"execution"`
	Period    *int64 `json:"period"`
	Deadline  *int64 `json:"deadline"`
}

type periodicAnalyzeRequest struct {
	Tasks *[]periodicTaskIn `json:"tasks"`
}

type periodicTaskResult struct {
	ID             string `json:"id"`
	ResponseTime   *int64 `json:"responseTime"`
	DeadlineStatus string `json:"deadlineStatus"`
}

type periodicAnalyzeResponse struct {
	Schedulable bool                 `json:"schedulable"`
	Results     []periodicTaskResult `json:"results"`
}

type periodicTask struct {
	index     int
	id        string
	priority  int64
	execution int64
	period    int64
	deadline  int64
}

func handlePeriodicAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req periodicAnalyzeRequest
	if !decodeStrictRequest(w, r, &req) {
		return
	}
	tasks, ok := validatePeriodicRequest(&req)
	if !ok {
		writeAPIError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	resp := analyzePeriodic(tasks)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func validatePeriodicRequest(req *periodicAnalyzeRequest) ([]periodicTask, bool) {
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, false
	}
	tasks := make([]periodicTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	priorities := make(map[int64]struct{}, len(*req.Tasks))
	for i := range *req.Tasks {
		in := &(*req.Tasks)[i]
		if !validTaskID(in.ID) {
			return nil, false
		}
		if _, dup := ids[in.ID]; dup {
			return nil, false
		}
		ids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, false
		}
		if _, dup := priorities[*in.Priority]; dup {
			return nil, false
		}
		priorities[*in.Priority] = struct{}{}
		if in.Execution == nil || !withinPeriodicField(*in.Execution) {
			return nil, false
		}
		if in.Period == nil || !withinPeriodicField(*in.Period) {
			return nil, false
		}
		if in.Deadline == nil || !withinPeriodicField(*in.Deadline) ||
			*in.Deadline > *in.Period {
			return nil, false
		}
		tasks = append(tasks, periodicTask{
			index:     i,
			id:        in.ID,
			priority:  *in.Priority,
			execution: *in.Execution,
			period:    *in.Period,
			deadline:  *in.Deadline,
		})
	}
	return tasks, true
}

func withinPeriodicField(v int64) bool {
	return v > 0 && v <= maxPeriodicField
}

// analyzePeriodic applies fixed-priority worst-case response-time analysis
// under synchronous release, full preemption, no blocking and no release
// jitter. Tasks are processed from highest to lowest priority; the response
// of task k is the least fixed point of
//
//	w = C_k + Σ_{j∈hp(k)} ceil(w/T_j) C_j,
//
// provided it does not exceed D_k. Results keep the input order.
func analyzePeriodic(tasks []periodicTask) periodicAnalyzeResponse {
	n := len(tasks)
	byPriority := make([]periodicTask, n)
	copy(byPriority, tasks)
	sort.SliceStable(byPriority, func(a, b int) bool {
		return byPriority[a].priority < byPriority[b].priority
	})

	results := make([]periodicTaskResult, n)
	schedulable := true
	// Exact cumulative higher-priority utilization Σ C_j/T_j. At Σ ≥ 1 the
	// iteration f(w) = C_k + Σ C_j ceil(w/T_j) satisfies f(w) > w for every
	// w, so no fixed point exists; the early exit also bounds work on
	// pseudo-polynomial worst cases such as a T_j = C_j interferer.
	hpUtilization := new(big.Rat)
	one := big.NewRat(1, 1)
	for rank := range byPriority {
		t := byPriority[rank]
		var response *int64
		if hpUtilization.Cmp(one) < 0 {
			response = worstResponseTime(t, byPriority[:rank])
		}
		status := "met"
		if response == nil {
			status = "missed"
			schedulable = false
		}
		results[t.index] = periodicTaskResult{
			ID:             t.id,
			ResponseTime:   response,
			DeadlineStatus: status,
		}
		hpUtilization.Add(hpUtilization,
			new(big.Rat).SetFrac(big.NewInt(t.execution), big.NewInt(t.period)))
	}
	return periodicAnalyzeResponse{Schedulable: schedulable, Results: results}
}

// worstResponseTime iterates the fixed-point equation starting from w0 = C_k
// and returns the converged value when it never exceeds the deadline. The
// initial value or any iteration result above the deadline, or an int64
// overflow, yields nil. f is non-decreasing and the sequence starts at a
// lower bound, so every accepted value strictly increases until it fixes.
func worstResponseTime(t periodicTask, higher []periodicTask) *int64 {
	w := t.execution
	if w > t.deadline {
		return nil
	}
	for {
		next, overflow := rtaIteration(w, t.execution, higher)
		if overflow || next > t.deadline {
			return nil
		}
		if next == w {
			value := w
			return &value
		}
		w = next
	}
}

// rtaIteration computes C_k + Σ ceil(w/T_j) C_j with checked 64-bit
// arithmetic. Individual products can reach ~1e24, so multiplication and
// accumulation are widened to 128 bits; a total outside int64 is reported as
// overflow instead of wrapping.
func rtaIteration(w, execution int64, higher []periodicTask) (int64, bool) {
	sum := uint64(execution)
	var hi uint64
	for _, j := range higher {
		// w and T_j are positive and at most 1e12, so w + T_j - 1 cannot wrap.
		q := uint64((w + j.period - 1) / j.period)
		prodHi, prodLo := bits.Mul64(q, uint64(j.execution))
		var carry uint64
		sum, carry = bits.Add64(sum, prodLo, 0)
		hi, _ = bits.Add64(hi, prodHi, carry)
	}
	if hi != 0 || sum > maxInt64Uint {
		return 0, true
	}
	return int64(sum), false
}
