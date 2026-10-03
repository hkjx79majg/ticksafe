package server

import (
	"encoding/json"
	"net/http"
	"sort"
)

const (
	mutexAnalyzePath = "/v1/schedules/mutex/analyze"
	maxMutexActions  = 1024
)

// Mutex action discriminators.
const (
	mutexActRun int = iota
	mutexActLock
	mutexActUnlock
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

type mutexAction struct {
	kind int
	run  int64
	name string
}

type mutexTask struct {
	id       string
	priority int64
	release  int64
	deadline int64
	actions  []mutexAction
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
	tasks, ok := validateMutexRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(simulateMutex(tasks, *req.Horizon))
}

// validateMutexRequest enforces the one-shot task constraints (without
// execution) plus the action-program rules: exactly one verb per action,
// positive run microseconds, id-shaped mutex names, balanced nesting (no
// duplicate acquire, no unmatched release, nothing held at the end) and an
// int64-wide run total.
func validateMutexRequest(req *mutexRequest) ([]mutexTask, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, false
	}
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, false
	}
	horizon := *req.Horizon
	tasks := make([]mutexTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	var totalRun int64
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
		if in.Release == nil || *in.Release < 0 || *in.Release >= horizon {
			return nil, false
		}
		if in.Deadline == nil || *in.Deadline <= *in.Release {
			return nil, false
		}
		if in.Actions == nil || len(*in.Actions) == 0 || len(*in.Actions) > maxMutexActions {
			return nil, false
		}

		actions := make([]mutexAction, 0, len(*in.Actions))
		held := make(map[string]struct{})
		var taskRun int64
		for j := range *in.Actions {
			ai := (*in.Actions)[j]
			verbs := 0
			if ai.Run != nil {
				verbs++
			}
			if ai.Lock != nil {
				verbs++
			}
			if ai.Unlock != nil {
				verbs++
			}
			if verbs != 1 {
				return nil, false
			}
			switch {
			case ai.Run != nil:
				if *ai.Run <= 0 {
					return nil, false
				}
				var ok bool
				taskRun, ok = saturatingAdd(taskRun, *ai.Run)
				if !ok {
					return nil, false
				}
				actions = append(actions, mutexAction{kind: mutexActRun, run: *ai.Run})
			case ai.Lock != nil:
				name := *ai.Lock
				if !validTaskID(name) {
					return nil, false
				}
				if _, dup := held[name]; dup {
					return nil, false
				}
				held[name] = struct{}{}
				actions = append(actions, mutexAction{kind: mutexActLock, name: name})
			default:
				name := *ai.Unlock
				if !validTaskID(name) {
					return nil, false
				}
				if _, ok := held[name]; !ok {
					return nil, false
				}
				delete(held, name)
				actions = append(actions, mutexAction{kind: mutexActUnlock, name: name})
			}
		}
		if len(held) != 0 {
			return nil, false
		}
		var ok bool
		totalRun, ok = saturatingAdd(totalRun, taskRun)
		if !ok {
			return nil, false
		}
		tasks = append(tasks, mutexTask{
			id:       in.ID,
			priority: *in.Priority,
			release:  *in.Release,
			deadline: *in.Deadline,
			actions:  actions,
		})
	}
	return tasks, true
}

// mutexSegment is an unmerged run interval recorded by the simulation.
type mutexSegment struct {
	index      int
	start, end int64
	eff        int64
}

// mutexSim is an event-driven single-core simulation with priority
// inheritance. Time advances only through run actions; lock, unlock and
// completion happen at a single instant. The wait-for graph maps a blocked
// task (waiter) to the owner of the mutex it is blocked on.
type mutexSim struct {
	tasks   []mutexTask
	n       int
	horizon int64

	owner   map[string]int    // mutex -> owning task
	waiters map[string][]int  // mutex -> blocked tasks
	locksOf []map[string]bool // task -> owned mutexes

	blockedM  []string // "" when not blocked
	blockTime []int64
	pc        []int   // next action to interpret
	runRem    []int64 // remainder of the run action at pc

	released   []bool
	completed  []bool
	executed   []int64
	completion []*int64
	eff        []int64

	order     []int // tasks sorted by (release, input index)
	nextRel   int
	segs      []mutexSegment
	current   int
	now       int64
	deadCycle []int
}

func simulateMutex(tasks []mutexTask, horizon int64) mutexResponse {
	n := len(tasks)
	s := &mutexSim{
		tasks:      tasks,
		n:          n,
		horizon:    horizon,
		owner:      make(map[string]int),
		waiters:    make(map[string][]int),
		locksOf:    make([]map[string]bool, n),
		blockedM:   make([]string, n),
		blockTime:  make([]int64, n),
		pc:         make([]int, n),
		runRem:     make([]int64, n),
		released:   make([]bool, n),
		completed:  make([]bool, n),
		executed:   make([]int64, n),
		completion: make([]*int64, n),
		eff:        make([]int64, n),
		current:    -1,
	}
	for i := range s.locksOf {
		s.locksOf[i] = make(map[string]bool)
		s.eff[i] = tasks[i].priority
	}
	s.order = make([]int, n)
	for i := range s.order {
		s.order[i] = i
	}
	sort.Slice(s.order, func(a, b int) bool {
		ia, ib := s.order[a], s.order[b]
		if tasks[ia].release != tasks[ib].release {
			return tasks[ia].release < tasks[ib].release
		}
		return ia < ib
	})

	deadlocked := false
loop:
	for {
		s.releaseUpTo()

		// After any state change, first drain the current task's consecutive
		// zero-duration actions, then schedule.
		if s.current != -1 {
			if s.settle(s.current) {
				deadlocked = true
				break
			}
			if !s.runnable(s.current) {
				s.current = -1
			}
		}

		if s.current == -1 {
			best := s.pickBest()
			if best == -1 {
				// Nothing runnable: jump to the next release or the horizon.
				if s.nextRel >= n {
					s.now = horizon
					break loop
				}
				s.now = tasks[s.order[s.nextRel]].release
				continue
			}
			s.current = best
			if s.settle(best) {
				deadlocked = true
				break
			}
			if !s.runnable(best) {
				s.current = -1
				continue
			}
		}

		// A freshly selected task's zero-duration chain (or the previous
		// current task's chain) can hand a mutex to a strictly higher
		// effective-priority task at the same instant. Re-resolve until the
		// selection is stable; equal priority never preempts.
		for {
			best := s.pickBest()
			if best != -1 && best != s.current && s.eff[best] < s.eff[s.current] {
				s.current = best
				if s.settle(best) {
					deadlocked = true
					break loop
				}
				if !s.runnable(best) {
					s.current = -1
					continue loop
				}
				continue
			}
			break
		}

		if s.now >= horizon {
			break
		}
		cur := s.current
		// Advance until the run completes, the horizon cuts it, or a strictly
		// higher base-priority task releases. Effective priorities are stable
		// across a run: wait edges change only via zero-duration actions.
		rem := s.runRem[cur]
		length := rem
		if avail := horizon - s.now; length > avail {
			length = avail
		}
		end := s.now + length
		for k := s.nextRel; k < n; k++ {
			j := s.order[k]
			if tasks[j].priority < s.eff[cur] &&
				tasks[j].release > s.now && tasks[j].release < end {
				end = tasks[j].release
			}
		}
		s.segs = append(s.segs, mutexSegment{index: cur, start: s.now, end: end, eff: s.eff[cur]})
		delta := end - s.now
		s.executed[cur] += delta
		s.now = end
		if rem -= delta; rem > 0 {
			s.runRem[cur] = rem
			continue
		}
		s.runRem[cur] = 0
		s.pc[cur]++
		if s.now >= horizon {
			// The run ends exactly at the horizon: matching the one-shot
			// baseline, completion at the boundary is observable. Releases
			// strictly before the horizon are already in effect, so account for
			// them before draining this task's trailing zero-duration actions
			// once. Nothing else gets to run at or past the horizon.
			s.releaseUpTo()
			if s.settle(cur) {
				deadlocked = true
			}
			break
		}
	}

	return s.buildResponse(deadlocked)
}

// settle drains the task's consecutive zero-duration actions. It stops when
// the task faces a run action (arming its remainder), blocks, or completes.
// It reports whether the blocking action closed a wait-for cycle.
func (s *mutexSim) settle(i int) bool {
	for {
		t := s.tasks[i]
		if s.pc[i] >= len(t.actions) {
			s.completed[i] = true
			now := s.now
			s.completion[i] = &now
			return false
		}
		a := t.actions[s.pc[i]]
		switch a.kind {
		case mutexActRun:
			if s.runRem[i] == 0 {
				s.runRem[i] = a.run
			}
			return false
		case mutexActLock:
			m := a.name
			if owner, ok := s.owner[m]; ok {
				if owner == i {
					// Granted via handoff while blocked at this action.
					s.pc[i]++
					continue
				}
				cycle := s.cycleOnBlock(i, owner)
				s.blockedM[i] = m
				s.blockTime[i] = s.now
				s.waiters[m] = append(s.waiters[m], i)
				if cycle != nil {
					s.deadCycle = cycle
					return true
				}
				s.recompute()
				return false
			}
			s.owner[m] = i
			s.locksOf[i][m] = true
			s.pc[i]++
		case mutexActUnlock:
			s.handoff(a.name, i)
			s.pc[i]++
		}
	}
}

// waitersOn lists the blocked tasks directly waiting on mutexes owned by x
// (the predecessor direction of the wait-for graph). A task blocks on at most
// one mutex, so no member repeats. Order is irrelevant: inheritance takes the
// minimum and the graph is acyclic between recomputations.
func (s *mutexSim) waitersOn(x int) []int {
	var out []int
	for m := range s.locksOf[x] {
		out = append(out, s.waiters[m]...)
	}
	return out
}

// blockTarget is the outgoing wait-for edge of x: the owner of the mutex x is
// blocked on, or -1 when x is not blocked.
func (s *mutexSim) blockTarget(x int) int {
	if s.blockedM[x] == "" {
		return -1
	}
	return s.owner[s.blockedM[x]]
}

// cycleOnBlock checks whether adding the edge waiter -> holder closes a cycle
// in the wait-for graph. Every task has at most one outgoing edge, so the
// existing graph is a forest of chains (any earlier cycle would already have
// stopped the simulation): holder reaches waiter at most once. The returned
// cycle follows the wait direction and starts at the earliest input-order task.
func (s *mutexSim) cycleOnBlock(waiter, holder int) []int {
	chain := []int{holder}
	cur := holder
	for {
		next := s.blockTarget(cur)
		if next == -1 {
			return nil
		}
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

// handoff releases m from its owner and transfers it to the waiter with the
// highest effective priority, breaking ties by block time then input index.
// Waiter effective priorities cannot depend on the outgoing owner's edge
// (waiters have no edge toward a holder of m), so the pre-unlock values stay
// valid for the selection.
func (s *mutexSim) handoff(m string, from int) {
	ws := s.waiters[m]
	delete(s.locksOf[from], m)
	if len(ws) == 0 {
		delete(s.owner, m)
		s.recompute()
		return
	}
	w := ws[0]
	for _, x := range ws[1:] {
		if s.eff[x] < s.eff[w] ||
			(s.eff[x] == s.eff[w] && s.blockTime[x] < s.blockTime[w]) ||
			(s.eff[x] == s.eff[w] && s.blockTime[x] == s.blockTime[w] && x < w) {
			w = x
		}
	}
	s.owner[m] = w
	s.locksOf[w][m] = true
	remaining := make([]int, 0, len(ws)-1)
	for _, x := range ws {
		if x != w {
			remaining = append(remaining, x)
		}
	}
	s.waiters[m] = remaining
	s.blockedM[w] = ""
	s.recompute()
}

// recompute derives every effective priority as the minimum of the task's own
// priority and the effective priorities of the direct waiters on its mutexes;
// the DFS therefore covers indirect inheritance transitively.
func (s *mutexSim) recompute() {
	mark := make([]int8, s.n)
	var dfs func(int) int64
	dfs = func(i int) int64 {
		if mark[i] == 2 {
			return s.eff[i]
		}
		mark[i] = 1
		best := s.tasks[i].priority
		for _, w := range s.waitersOn(i) {
			if v := dfs(w); v < best {
				best = v
			}
		}
		mark[i] = 2
		s.eff[i] = best
		return best
	}
	for i := 0; i < s.n; i++ {
		dfs(i)
	}
}

// pickBest selects the runnable task with the highest effective priority,
// breaking ties by release time then input index.
func (s *mutexSim) pickBest() int {
	best := -1
	for i := 0; i < s.n; i++ {
		if !s.released[i] || s.completed[i] || s.blockedM[i] != "" {
			continue
		}
		if best == -1 {
			best = i
			continue
		}
		a, b := s.tasks[i], s.tasks[best]
		if s.eff[i] < s.eff[best] ||
			(s.eff[i] == s.eff[best] && a.release < b.release) ||
			(s.eff[i] == s.eff[best] && a.release == b.release && i < best) {
			best = i
		}
	}
	return best
}

func (s *mutexSim) runnable(i int) bool {
	return s.released[i] && !s.completed[i] && s.blockedM[i] == "" && s.runRem[i] > 0
}

func (s *mutexSim) releaseUpTo() {
	for s.nextRel < s.n && s.tasks[s.order[s.nextRel]].release <= s.now {
		s.released[s.order[s.nextRel]] = true
		s.nextRel++
	}
}

func (s *mutexSim) buildResponse(deadlocked bool) mutexResponse {
	timeline := make([]mutexTimelineInterval, 0, len(s.segs))
	for _, seg := range s.segs {
		last := len(timeline) - 1
		if last >= 0 &&
			timeline[last].TaskID == s.tasks[seg.index].id &&
			timeline[last].EffectivePriority == seg.eff &&
			timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, mutexTimelineInterval{
			TaskID:            s.tasks[seg.index].id,
			Start:             seg.start,
			End:               seg.end,
			EffectivePriority: seg.eff,
		})
	}

	results := make([]mutexTaskResult, s.n)
	allCompleted := true
	for i, t := range s.tasks {
		state := "ready"
		switch {
		case s.completed[i]:
			state = "completed"
		case s.blockedM[i] != "":
			state = "blocked"
		case !s.released[i]:
			state = "unreleased"
		}
		if state != "completed" {
			allCompleted = false
		}
		var blockedOn *string
		if state == "blocked" {
			m := s.blockedM[i]
			blockedOn = &m
		}
		var deadlineStatus string
		switch c := s.completion[i]; {
		case c != nil && *c <= t.deadline:
			deadlineStatus = "met"
		case c != nil:
			deadlineStatus = "missed"
		case t.deadline <= s.horizon:
			deadlineStatus = "missed"
		default:
			deadlineStatus = "pending"
		}
		results[i] = mutexTaskResult{
			Executed:       s.executed[i],
			Completion:     s.completion[i],
			State:          state,
			BlockedOn:      blockedOn,
			DeadlineStatus: deadlineStatus,
		}
	}

	status := "horizon"
	if deadlocked {
		status = "deadlocked"
	} else if allCompleted {
		status = "completed"
	}

	resp := mutexResponse{
		Status:   status,
		Timeline: timeline,
		Results:  results,
	}
	if deadlocked {
		at := s.now
		resp.DeadlockAt = &at
		cycle := make([]string, len(s.deadCycle))
		for k, idx := range s.deadCycle {
			cycle[k] = s.tasks[idx].id
		}
		resp.Cycle = cycle
	}
	return resp
}
