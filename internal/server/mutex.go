package server

import (
	"encoding/json"
	"net/http"
	"sort"
)

const (
	mutexAnalyzePath = "/v1/schedules/mutex/analyze"
	maxActions       = 1024
)

type mutexActionIn struct {
	Run    *int64  `json:"run"`
	Lock   *string `json:"lock"`
	Unlock *string `json:"unlock"`
}

type mutexTaskIn struct {
	ID       string           `json:"id"`
	Priority *int64           `json:"priority"`
	Release  *int64           `json:"release"`
	Deadline *int64           `json:"deadline"`
	Actions  *[]mutexActionIn `json:"actions"`
}

type mutexRequest struct {
	Horizon *int64         `json:"horizon"`
	Tasks   *[]mutexTaskIn `json:"tasks"`
}

type mutexTimelineInterval struct {
	TaskID            string `json:"taskId"`
	Start             int64  `json:"start"`
	End               int64  `json:"end"`
	EffectivePriority int64  `json:"effectivePriority"`
}

type mutexTaskResult struct {
	Executed       int64   `json:"executed"`
	Completion     *int64  `json:"completion"`
	State          string  `json:"state"`
	BlockedOn      *string `json:"blockedOn"`
	DeadlineStatus string  `json:"deadlineStatus"`
}

type mutexResponse struct {
	Status     string                  `json:"status"`
	Timeline   []mutexTimelineInterval `json:"timeline"`
	Results    []mutexTaskResult       `json:"results"`
	DeadlockAt *int64                  `json:"deadlockAt"`
	Cycle      []string                `json:"cycle"`
}

// Action kinds for a parsed mutexTask.
const (
	actionRun = iota
	actionLock
	actionUnlock
)

type mutexAction struct {
	kind  int
	run   int64 // actionRun: duration in microseconds
	mutex int   // actionLock/actionUnlock: index into mutexNames
}

type mutexTask struct {
	id       string
	priority int64
	release  int64
	deadline int64
	actions  []mutexAction
}

func handleMutexAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req mutexRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	tasks, mutexNames, ok := validateMutexRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	resp := simulateMutex(tasks, mutexNames, *req.Horizon)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// validateMutexRequest checks the one-shot task fields (except execution, which
// the action list replaces) plus the action program: every action is exactly
// one of run/lock/unlock, run durations are positive and their per-task total
// stays within int64, and each task's lock discipline is sound (no re-lock of a
// held mutex, no unlock of an unheld mutex, nothing held at the end).
func validateMutexRequest(req *mutexRequest) ([]mutexTask, []string, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, nil, false
	}
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, nil, false
	}
	horizon := *req.Horizon
	tasks := make([]mutexTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	mutexIndex := make(map[string]int)
	var mutexNames []string
	for i := range *req.Tasks {
		in := (*req.Tasks)[i]
		if !validTaskID(in.ID) {
			return nil, nil, false
		}
		if _, dup := ids[in.ID]; dup {
			return nil, nil, false
		}
		ids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, nil, false
		}
		if in.Release == nil || *in.Release < 0 || *in.Release >= horizon {
			return nil, nil, false
		}
		if in.Deadline == nil || *in.Deadline <= *in.Release {
			return nil, nil, false
		}
		if in.Actions == nil || len(*in.Actions) == 0 || len(*in.Actions) > maxActions {
			return nil, nil, false
		}
		actions := make([]mutexAction, 0, len(*in.Actions))
		held := make(map[string]struct{})
		var runSum int64
		for _, a := range *in.Actions {
			kinds := 0
			if a.Run != nil {
				kinds++
			}
			if a.Lock != nil {
				kinds++
			}
			if a.Unlock != nil {
				kinds++
			}
			if kinds != 1 {
				return nil, nil, false
			}
			switch {
			case a.Run != nil:
				if *a.Run <= 0 {
					return nil, nil, false
				}
				if runSum > maxInt64-*a.Run {
					return nil, nil, false
				}
				runSum += *a.Run
				actions = append(actions, mutexAction{kind: actionRun, run: *a.Run})
			case a.Lock != nil:
				name := *a.Lock
				if !validTaskID(name) {
					return nil, nil, false
				}
				if _, dup := held[name]; dup {
					return nil, nil, false
				}
				held[name] = struct{}{}
				idx, ok := mutexIndex[name]
				if !ok {
					idx = len(mutexNames)
					mutexIndex[name] = idx
					mutexNames = append(mutexNames, name)
				}
				actions = append(actions, mutexAction{kind: actionLock, mutex: idx})
			default: // a.Unlock != nil
				name := *a.Unlock
				if !validTaskID(name) {
					return nil, nil, false
				}
				if _, ok := held[name]; !ok {
					return nil, nil, false
				}
				delete(held, name)
				actions = append(actions, mutexAction{kind: actionUnlock, mutex: mutexIndex[name]})
			}
		}
		if len(held) != 0 {
			return nil, nil, false
		}
		tasks = append(tasks, mutexTask{
			id:       in.ID,
			priority: *in.Priority,
			release:  *in.Release,
			deadline: *in.Deadline,
			actions:  actions,
		})
	}
	return tasks, mutexNames, true
}

// simulateMutex runs an event-driven single-core simulation over [0, horizon)
// with mutexes and transitive priority inheritance. lock, unlock and
// completion take zero time; after any state change the current task's
// consecutive zero-duration actions are processed before the next scheduling
// decision. The first wait cycle to form stops the simulation as a deadlock.
func simulateMutex(tasks []mutexTask, mutexNames []string, horizon int64) mutexResponse {
	n := len(tasks)
	m := len(mutexNames)

	pc := make([]int, n) // next action to execute
	runLeft := make([]int64, n)
	executed := make([]int64, n)
	completed := make([]bool, n)
	completion := make([]*int64, n)
	released := make([]bool, n)
	blockedOn := make([]int, n) // mutex index, -1 when not blocked
	blockedSince := make([]int64, n)
	for i := range blockedOn {
		blockedOn[i] = -1
	}
	owner := make([]int, m) // task index, -1 when free
	for j := range owner {
		owner[j] = -1
	}
	waiters := make([][]int, m)

	// Indices sorted by (release, input order) for release bookkeeping and
	// same-effective-priority tie breaking.
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		ia, ib := order[a], order[b]
		if tasks[ia].release != tasks[ib].release {
			return tasks[ia].release < tasks[ib].release
		}
		return ia < ib
	})
	nextRel := 0
	releaseUpTo := func(now int64) {
		for nextRel < n && tasks[order[nextRel]].release <= now {
			released[order[nextRel]] = true
			nextRel++
		}
	}

	// Effective priorities: a holder inherits the highest priority (smallest
	// value) of its direct and indirect waiters. The wait-for graph is
	// acyclic here (a cycle stops the simulation the instant it forms), so
	// walking each blocked task's owner chain reaches every ancestor.
	eff := make([]int64, n)
	computeEff := func() {
		for i, t := range tasks {
			eff[i] = t.priority
		}
		for w := 0; w < n; w++ {
			if blockedOn[w] == -1 {
				continue
			}
			cur := w
			for blockedOn[cur] != -1 {
				o := owner[blockedOn[cur]]
				if tasks[w].priority < eff[o] {
					eff[o] = tasks[w].priority
				}
				cur = o
			}
		}
	}

	var now int64
	current := -1
	var deadlockAt *int64
	var cycle []string

	const (
		zeroRunning = iota
		zeroBlocked
		zeroCompleted
		zeroDeadlocked
	)

	// zeroTime executes the consecutive zero-duration actions of task t until
	// it reaches a run action, blocks, completes, or closes a wait cycle.
	zeroTime := func(t int) int {
		for {
			if pc[t] == len(tasks[t].actions) {
				completed[t] = true
				c := now
				completion[t] = &c
				return zeroCompleted
			}
			a := tasks[t].actions[pc[t]]
			switch a.kind {
			case actionRun:
				runLeft[t] = a.run
				return zeroRunning
			case actionLock:
				mu := a.mutex
				if owner[mu] == -1 {
					owner[mu] = t
					break // switch; advance to the next action
				}
				blockedOn[t] = mu
				blockedSince[t] = now
				waiters[mu] = append(waiters[mu], t)
				// The wait-for graph was acyclic before this edge, so a
				// cycle exists iff the owner chain leads back to t.
				path := []int{t}
				cur := owner[mu]
				for cur != t {
					path = append(path, cur)
					if blockedOn[cur] == -1 {
						path = nil
						break
					}
					cur = owner[blockedOn[cur]]
				}
				if path != nil {
					start := 0
					for i := range path {
						if path[i] < path[start] {
							start = i
						}
					}
					cycle = make([]string, len(path))
					for i := range path {
						cycle[i] = tasks[path[(start+i)%len(path)]].id
					}
					d := now
					deadlockAt = &d
					return zeroDeadlocked
				}
				return zeroBlocked
			case actionUnlock:
				mu := a.mutex
				if len(waiters[mu]) == 0 {
					owner[mu] = -1
					break // switch
				}
				// Ownership passes to the waiter with the highest effective
				// priority; ties by blocking time, then input order.
				computeEff()
				best, bestPos := -1, -1
				for pos, w := range waiters[mu] {
					if best == -1 ||
						eff[w] < eff[best] ||
						(eff[w] == eff[best] && blockedSince[w] < blockedSince[best]) ||
						(eff[w] == eff[best] && blockedSince[w] == blockedSince[best] && w < best) {
						best, bestPos = w, pos
					}
				}
				waiters[mu] = append(waiters[mu][:bestPos], waiters[mu][bestPos+1:]...)
				owner[mu] = best
				blockedOn[best] = -1
				pc[best]++ // the handoff completes the waiter's lock action
			}
			pc[t]++
		}
	}

	type segment struct {
		index      int
		start, end int64
		eff        int64
	}
	var segments []segment

	allCompleted := func() bool {
		for i := range completed {
			if !completed[i] {
				return false
			}
		}
		return true
	}

	status := ""
	for status == "" {
		if allCompleted() {
			status = "completed"
			break
		}
		// After a state change, the current task's consecutive zero-duration
		// actions run before any scheduling decision.
		if current != -1 && runLeft[current] == 0 {
			switch zeroTime(current) {
			case zeroDeadlocked:
				status = "deadlocked"
			case zeroBlocked, zeroCompleted:
				current = -1
			}
			if status != "" {
				break
			}
			if allCompleted() {
				status = "completed"
				break
			}
		}
		if now >= horizon {
			status = "horizon"
			break
		}
		releaseUpTo(now)
		computeEff()
		// Best runnable task: smallest effective priority, then earliest
		// release, then earliest input index.
		best := -1
		for i := 0; i < n; i++ {
			if !released[i] || completed[i] || blockedOn[i] != -1 {
				continue
			}
			if best == -1 ||
				eff[i] < eff[best] ||
				(eff[i] == eff[best] && tasks[i].release < tasks[best].release) ||
				(eff[i] == eff[best] && tasks[i].release == tasks[best].release && i < best) {
				best = i
			}
		}
		if current == -1 {
			if best == -1 {
				// Idle: jump to the next release. With no releases left and
				// work unfinished, every blocked task's lock holder would be
				// runnable or part of a cycle, so this is unreachable.
				if nextRel >= n {
					status = "horizon"
					break
				}
				now = tasks[order[nextRel]].release
				continue
			}
			current = best
			continue
		}
		if best != current && eff[best] < eff[current] {
			// Strictly higher effective priority preempts; equal never does.
			current = best
			continue
		}

		// Run until the run action completes, the horizon, or the next
		// release, whichever comes first.
		runEnd := horizon
		if runLeft[current] < horizon-now {
			runEnd = now + runLeft[current]
		}
		if nextRel < n && tasks[order[nextRel]].release < runEnd {
			runEnd = tasks[order[nextRel]].release
		}
		segments = append(segments, segment{index: current, start: now, end: runEnd, eff: eff[current]})
		delta := runEnd - now
		executed[current] += delta
		runLeft[current] -= delta
		now = runEnd
		if runLeft[current] == 0 {
			pc[current]++
		}
	}

	timeline := make([]mutexTimelineInterval, 0, len(segments))
	for _, seg := range segments {
		last := len(timeline) - 1
		if last >= 0 &&
			timeline[last].TaskID == tasks[seg.index].id &&
			timeline[last].End == seg.start &&
			timeline[last].EffectivePriority == seg.eff {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, mutexTimelineInterval{
			TaskID:            tasks[seg.index].id,
			Start:             seg.start,
			End:               seg.end,
			EffectivePriority: seg.eff,
		})
	}

	results := make([]mutexTaskResult, n)
	for i, t := range tasks {
		var state string
		var blockedBy *string
		switch {
		case completed[i]:
			state = "completed"
		case !released[i]:
			state = "unreleased"
		case blockedOn[i] != -1:
			state = "blocked"
			name := mutexNames[blockedOn[i]]
			blockedBy = &name
		default:
			state = "ready"
		}
		var deadlineStatus string
		switch c := completion[i]; {
		case c != nil:
			if *c <= t.deadline {
				deadlineStatus = "met"
			} else {
				deadlineStatus = "missed"
			}
		case t.deadline <= horizon:
			deadlineStatus = "missed"
		default:
			deadlineStatus = "pending"
		}
		results[i] = mutexTaskResult{
			Executed:       executed[i],
			Completion:     completion[i],
			State:          state,
			BlockedOn:      blockedBy,
			DeadlineStatus: deadlineStatus,
		}
	}

	return mutexResponse{
		Status:     status,
		Timeline:   timeline,
		Results:    results,
		DeadlockAt: deadlockAt,
		Cycle:      cycle,
	}
}
