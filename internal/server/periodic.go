package server

import (
	"encoding/json"
	"math/big"
	"net/http"
)

const (
	periodicAnalyzePath = "/v1/schedules/periodic/analyze"
	maxPeriodicValue    = 1_000_000_000_000
	maxInt64            = 1<<63 - 1
	// rtaPrecision is the working precision (bits) for the high-precision
	// utilization sums; rtaGrayZone is the band around 1 resolved exactly.
	// 256 bits keeps the jump error below C*n*2^-256/d^2 << 1 even at the
	// edge of the gray zone, without the cost of wider arithmetic.
	rtaPrecision = 256
	rtaGrayZone  = "1e-30"
)

type periodicTaskIn struct {
	ID        string `json:"id"`
	Priority  *int64 `json:"priority"`
	Execution *int64 `json:"execution"`
	Period    *int64 `json:"period"`
	Deadline  *int64 `json:"deadline"`
}

type periodicRequest struct {
	Tasks *[]periodicTaskIn `json:"tasks"`
}

type periodicTask struct {
	id        string
	priority  int64
	execution int64
	period    int64
	deadline  int64
}

type periodicTaskResult struct {
	ID             string `json:"id"`
	ResponseTime   *int64 `json:"responseTime"`
	DeadlineStatus string `json:"deadlineStatus"`
}

type periodicResponse struct {
	Schedulable bool                 `json:"schedulable"`
	Results     []periodicTaskResult `json:"results"`
}

func handlePeriodicAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req periodicRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	tasks, ok := validatePeriodicRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(analyzePeriodic(tasks))
}

func validatePeriodicRequest(req *periodicRequest) ([]periodicTask, bool) {
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, false
	}
	tasks := make([]periodicTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	priorities := make(map[int64]struct{}, len(*req.Tasks))
	for i := range *req.Tasks {
		in := (*req.Tasks)[i]
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
		if in.Execution == nil || *in.Execution <= 0 || *in.Execution > maxPeriodicValue {
			return nil, false
		}
		if in.Period == nil || *in.Period <= 0 || *in.Period > maxPeriodicValue {
			return nil, false
		}
		if in.Deadline == nil || *in.Deadline <= 0 || *in.Deadline > maxPeriodicValue ||
			*in.Deadline > *in.Period {
			return nil, false
		}
		tasks = append(tasks, periodicTask{
			id:        in.ID,
			priority:  *in.Priority,
			execution: *in.Execution,
			period:    *in.Period,
			deadline:  *in.Deadline,
		})
	}
	return tasks, true
}

// analyzePeriodic computes worst-case response times under synchronous
// release, fixed priorities, preemptive scheduling, no blocking and no
// release jitter (constrained deadlines: deadline <= period).
func analyzePeriodic(tasks []periodicTask) periodicResponse {
	n := len(tasks)
	// hp[i] lists the strictly higher-priority tasks of task i in input order.
	hp := make([][]periodicTask, n)
	for i, t := range tasks {
		for j := range tasks {
			if tasks[j].priority < t.priority {
				hp[i] = append(hp[i], tasks[j])
			}
		}
	}

	resp := periodicResponse{
		Schedulable: true,
		Results:     make([]periodicTaskResult, n),
	}
	for i, t := range tasks {
		rt := worstResponseTime(t, hp[i])
		result := periodicTaskResult{ID: t.id, ResponseTime: rt}
		if rt != nil && *rt <= t.deadline {
			result.DeadlineStatus = "met"
		} else {
			result.DeadlineStatus = "missed"
			resp.Schedulable = false
		}
		resp.Results[i] = result
	}
	return resp
}

// interference sums ceil(w/period)*execution over every higher-priority
// task. A result of false means the sum exceeds int64.
func interference(hp []periodicTask, w int64) (int64, bool) {
	var sum int64
	for _, t := range hp {
		jobs := (w + t.period - 1) / t.period
		term, ok := saturatingMul(jobs, t.execution)
		if !ok {
			return 0, false
		}
		sum, ok = saturatingAdd(sum, term)
		if !ok {
			return 0, false
		}
	}
	return sum, true
}

// worstResponseTime returns the least fixed point of
//
//	w = C + sum_j ceil(w/T_j) * C_j   (j ranging over hp)
//
// or nil if it does not settle at or before the deadline.
//
// Semantics:
//
//   - If the higher-priority utilization U = sum C_j/T_j is at least 1 there
//     is no finite fixed point (those tasks alone saturate the processor), so
//     the task misses.
//   - Otherwise the iteration from w0 = C is a strictly increasing integer
//     chain bounded by the least fixed point R; it reaches R or crosses the
//     deadline. Starting above R would be wrong: g can admit further, larger
//     fixed points even with U < 1, so every accelerated start must be a
//     proven lower bound of R.
//
// Acceleration: since ceil(x/T_j)*C_j >= (x/T_j)*C_j for every x >= 0,
// g(x) >= C + U*x, hence R >= C/(1-U) =: j0. Beginning the iteration at j0
// skips the well-known pseudo-polynomial blow-up in which tiny per-round
// increments walk all the way to a very distant fixed point. The common
// path evaluates j0 with high-precision (256-bit) floating point, avoiding
// the denominator LCM blow-up an exact rational sum would suffer;
// with |1-U| above a 1e-30 gray zone its absolute error is provably below 1,
// so floor(j0)-1 is strictly below the exact j0. In the gray zone (which
// contains every exact equality such as 1/3+1/3+1/3 == 1) and for obvious
// saturation, exact rational arithmetic decides. A start above the deadline
// proves a miss because R >= j0 >= start. The residual iteration after the
// jump remains, in the worst case, pseudo-polynomial.
func worstResponseTime(t periodicTask, hp []periodicTask) *int64 {
	if t.execution > t.deadline {
		return nil
	}

	start, miss := initialBound(t, hp)
	if miss {
		return nil
	}

	w := start
	for {
		f, ok := interference(hp, w)
		if !ok {
			return nil
		}
		next, ok := saturatingAdd(t.execution, f)
		if !ok || next > t.deadline {
			return nil
		}
		if next == w {
			rt := w
			return &rt
		}
		w = next
	}
}

// initialBound returns the iteration start and whether the bound proves the
// deadline cannot be met.
func initialBound(t periodicTask, hp []periodicTask) (start int64, miss bool) {
	if len(hp) == 0 {
		return t.execution, false
	}

	// High-precision utilization: error per quotient is < 2^-256, so the
	// accumulated absolute error is below len(hp)*2^-256.
	u := new(big.Float).SetPrec(rtaPrecision)
	for _, h := range hp {
		q := new(big.Float).SetPrec(rtaPrecision).Quo(
			new(big.Float).SetPrec(rtaPrecision).SetInt64(h.execution),
			new(big.Float).SetPrec(rtaPrecision).SetInt64(h.period))
		u.Add(u, q)
	}
	one := new(big.Float).SetPrec(rtaPrecision).SetInt64(1)
	d := new(big.Float).SetPrec(rtaPrecision).Sub(one, u) // d = 1 - U
	gray, _ := new(big.Float).SetPrec(rtaPrecision).SetString(rtaGrayZone)

	if d.Cmp(gray) <= 0 {
		// Gray zone (d ~ 0) or U > 1: settle utilization exactly.
		ru := new(big.Rat)
		for _, h := range hp {
			ru.Add(ru, new(big.Rat).SetFrac(big.NewInt(h.execution), big.NewInt(h.period)))
		}
		if ru.Cmp(big.NewRat(1, 1)) >= 0 {
			return 0, true
		}
		rd := new(big.Rat).Sub(big.NewRat(1, 1), ru)
		j := new(big.Int).Quo(
			new(big.Int).Mul(big.NewInt(t.execution), rd.Denom()), rd.Num())
		if !j.IsInt64() || j.Int64() > t.deadline {
			return 0, true
		}
		if jv := j.Int64(); jv > t.execution {
			return jv, false
		}
		return t.execution, false
	}

	// d > gray > 0, so U is certainly below 1. j0 = C/d; the bound
	// C*n*2^-256/d^2 is below 1 throughout this region, hence floor(j0)-1 is
	// strictly smaller than the exact j0 for all representable inputs.
	j0 := new(big.Float).SetPrec(rtaPrecision).Quo(
		new(big.Float).SetPrec(rtaPrecision).SetInt64(t.execution), d)
	j, _ := j0.Int(nil) // truncation towards zero; j0 is positive
	j.Sub(j, big.NewInt(1))
	if !j.IsInt64() || j.Int64() > t.deadline {
		return 0, true
	}
	if jv := j.Int64(); jv > t.execution {
		return jv, false
	}
	return t.execution, false
}

func saturatingAdd(a, b int64) (int64, bool) {
	if b > maxInt64-a {
		return 0, false
	}
	return a + b, true
}

func saturatingMul(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > maxInt64/b {
		return 0, false
	}
	return a * b, true
}
