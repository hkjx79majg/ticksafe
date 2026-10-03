package server

import (
	"encoding/json"
	"net/http"
	"sort"
)

const (
	semaphoreAnalyzePath = "/v1/schedules/semaphore/analyze"
	maxSemaphores        = 256
	maxSemaphoreMaximum  = 65535
	maxSemaphoreActions  = 1024
)

// Semaphore action discriminators.
const (
	semActRun int = iota
	semActWait
	semActPost
)

type semActionIn struct {
	Run  *int64  `json:"run"`
	Wait *string `json:"wait"`
	Post *string `json:"post"`
}

type semTaskIn struct {
	ID       string         `json:"id"`
	Priority *int64         `json:"priority"`
	Release  *int64         `json:"release"`
	Deadline *int64         `json:"deadline"`
	Actions  *[]semActionIn `json:"actions"`
}

type semDeclIn struct {
	Name    string `json:"name"`
	Initial *int64 `json:"initial"`
	Maximum *int64 `json:"maximum"`
}

type semRequest struct {
	Horizon    *int64       `json:"horizon"`
	Semaphores *[]semDeclIn `json:"semaphores"`
	Tasks      *[]semTaskIn `json:"tasks"`
}

type semAction struct {
	kind int
	run  int64
	name string // wait/post target
}

type semTask struct {
	id       string
	priority int64
	release  int64
	deadline int64
	actions  []semAction
}

type semDecl struct {
	initial int64
	maximum int64
}

type semTimelineInterval struct {
	TaskID string `json:"taskId"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

type semTaskResult struct {
	Executed       int64   `json:"executed"`
	Completion     *int64  `json:"completion"`
	State          string  `json:"state"`
	BlockedOn      *string `json:"blockedOn"`
	DeadlineStatus string  `json:"deadlineStatus"`
}

type semResponse struct {
	Status         string                `json:"status"`
	StoppedAt      int64                 `json:"stoppedAt"`
	Timeline       []semTimelineInterval `json:"timeline"`
	Results        []semTaskResult       `json:"results"`
	FaultAt        *int64                `json:"faultAt,omitempty"`
	FaultTask      *string               `json:"faultTask,omitempty"`
	FaultSemaphore *string               `json:"faultSemaphore,omitempty"`
}

func handleSemaphoreAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req semRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	tasks, decls, ok := validateSemaphoreRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(simulateSemaphore(tasks, decls, *req.Horizon))
}

// validateSemaphoreRequest enforces the one-shot task constraints (without
// execution), the semaphore declarations (1..256 unique id-shaped names with
// maximum 1..65535 and initial 0..maximum) and the action-program rules:
// exactly one verb per action, positive run microseconds, wait/post references
// pointing at declared semaphores, and per-task and global run totals that fit
// int64.
func validateSemaphoreRequest(req *semRequest) ([]semTask, map[string]semDecl, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, nil, false
	}
	if req.Semaphores == nil || len(*req.Semaphores) == 0 || len(*req.Semaphores) > maxSemaphores {
		return nil, nil, false
	}
	decls := make(map[string]semDecl, len(*req.Semaphores))
	for i := range *req.Semaphores {
		sd := (*req.Semaphores)[i]
		if !validTaskID(sd.Name) {
			return nil, nil, false
		}
		if _, dup := decls[sd.Name]; dup {
			return nil, nil, false
		}
		if sd.Maximum == nil || *sd.Maximum < 1 || *sd.Maximum > maxSemaphoreMaximum {
			return nil, nil, false
		}
		if sd.Initial == nil || *sd.Initial < 0 || *sd.Initial > *sd.Maximum {
			return nil, nil, false
		}
		decls[sd.Name] = semDecl{initial: *sd.Initial, maximum: *sd.Maximum}
	}
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, nil, false
	}
	horizon := *req.Horizon
	tasks := make([]semTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	var totalRun int64
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
		if in.Actions == nil || len(*in.Actions) == 0 || len(*in.Actions) > maxSemaphoreActions {
			return nil, nil, false
		}

		actions := make([]semAction, 0, len(*in.Actions))
		var taskRun int64
		for j := range *in.Actions {
			ai := (*in.Actions)[j]
			verbs := 0
			if ai.Run != nil {
				verbs++
			}
			if ai.Wait != nil {
				verbs++
			}
			if ai.Post != nil {
				verbs++
			}
			if verbs != 1 {
				return nil, nil, false
			}
			switch {
			case ai.Run != nil:
				if *ai.Run <= 0 {
					return nil, nil, false
				}
				var ok bool
				taskRun, ok = saturatingAdd(taskRun, *ai.Run)
				if !ok {
					return nil, nil, false
				}
				actions = append(actions, semAction{kind: semActRun, run: *ai.Run})
			case ai.Wait != nil:
				name := *ai.Wait
				if _, ok := decls[name]; !ok {
					return nil, nil, false
				}
				actions = append(actions, semAction{kind: semActWait, name: name})
			default:
				name := *ai.Post
				if _, ok := decls[name]; !ok {
					return nil, nil, false
				}
				actions = append(actions, semAction{kind: semActPost, name: name})
			}
		}
		var ok bool
		totalRun, ok = saturatingAdd(totalRun, taskRun)
		if !ok {
			return nil, nil, false
		}
		tasks = append(tasks, semTask{
			id:       in.ID,
			priority: *in.Priority,
			release:  *in.Release,
			deadline: *in.Deadline,
			actions:  actions,
		})
	}
	return tasks, decls, true
}

// semSegment is an unmerged run interval recorded by the simulation.
type semSegment struct {
	index      int
	start, end int64
}

// semSim is an event-driven single-core simulation of counting semaphores.
// Time advances only through run actions; wait, post and completion happen at
// a single instant. A post always prefers direct delivery to the earliest
// blocked waiter (the count is unchanged); with no waiters it increments the
// count, and a count already at maximum is an overflow fault that stops the
// analysis on the spot.
type semSim struct {
	tasks   []semTask
	n       int
	horizon int64
	maximum map[string]int64

	count   map[string]int64
	waiters map[string][]int // semaphore -> tasks blocked on wait

	blockedS  []string // semaphore name when blocked, "" otherwise
	blockTime []int64
	pc        []int   // next action to interpret
	runRem    []int64 // remainder of the run action at pc

	released   []bool
	completed  []bool
	executed   []int64
	completion []*int64

	order   []int // tasks sorted by (release, input index)
	nextRel int
	segs    []semSegment
	current int
	now     int64

	faultTask int
	faultSem  string
}

func simulateSemaphore(tasks []semTask, decls map[string]semDecl, horizon int64) semResponse {
	n := len(tasks)
	s := &semSim{
		tasks:      tasks,
		n:          n,
		horizon:    horizon,
		maximum:    make(map[string]int64, len(decls)),
		count:      make(map[string]int64, len(decls)),
		waiters:    make(map[string][]int),
		blockedS:   make([]string, n),
		blockTime:  make([]int64, n),
		pc:         make([]int, n),
		runRem:     make([]int64, n),
		released:   make([]bool, n),
		completed:  make([]bool, n),
		executed:   make([]int64, n),
		completion: make([]*int64, n),
		current:    -1,
	}
	for name, d := range decls {
		s.maximum[name] = d.maximum
		s.count[name] = d.initial
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

	overflow := false
	stalled := false
loop:
	for {
		s.releaseUpTo()

		// After any state change, first drain the current task's consecutive
		// zero-duration actions, then schedule.
		if s.current != -1 {
			if s.settle(s.current) {
				overflow = true
				break
			}
			if !s.runnable(s.current) {
				s.current = -1
			}
		}

		if s.current == -1 {
			best := s.pickBest()
			if best == -1 {
				// Nothing runnable: with no pending release every unfinished
				// task is blocked, so the simulation stalls here; otherwise
				// jump to the next release.
				if s.nextRel >= n {
					stalled = !s.allCompleted()
					break loop
				}
				s.now = tasks[s.order[s.nextRel]].release
				continue
			}
			s.current = best
			if s.settle(best) {
				overflow = true
				break
			}
			if !s.runnable(best) {
				s.current = -1
				continue
			}
		}

		// A freshly selected task's zero-duration chain can unblock a strictly
		// higher-priority task at the same instant. Re-resolve until the
		// selection is stable; equal priority never preempts.
		for {
			best := s.pickBest()
			if best != -1 && best != s.current && tasks[best].priority < tasks[s.current].priority {
				s.current = best
				if s.settle(best) {
					overflow = true
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
		// higher-priority task releases.
		rem := s.runRem[cur]
		length := rem
		if avail := horizon - s.now; length > avail {
			length = avail
		}
		end := s.now + length
		for k := s.nextRel; k < n; k++ {
			j := s.order[k]
			if tasks[j].priority < tasks[cur].priority &&
				tasks[j].release > s.now && tasks[j].release < end {
				end = tasks[j].release
			}
		}
		s.segs = append(s.segs, semSegment{index: cur, start: s.now, end: end})
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
			// baseline, releases strictly before the horizon are already in
			// effect, so account for them before draining this task's trailing
			// zero-duration actions once. Nothing else gets to run.
			s.releaseUpTo()
			if s.settle(cur) {
				overflow = true
			}
			break
		}
	}

	return s.buildResponse(overflow, stalled)
}

// settle drains the task's consecutive zero-duration actions. It stops when
// the task faces a run action (arming its remainder), blocks, or completes.
// It reports whether a post hit a full semaphore with no waiters (overflow).
func (s *semSim) settle(i int) bool {
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
		case semActRun:
			if s.runRem[i] == 0 {
				s.runRem[i] = a.run
			}
			return false
		case semActWait:
			m := a.name
			if s.count[m] > 0 {
				s.count[m]--
				s.pc[i]++
				continue
			}
			// Empty semaphore: the task blocks on it.
			s.blockedS[i] = m
			s.blockTime[i] = s.now
			s.waiters[m] = append(s.waiters[m], i)
			return false
		case semActPost:
			m := a.name
			// Direct delivery to the earliest blocked waiter wins over
			// incrementing; the count is unchanged.
			if ws := s.waiters[m]; len(ws) > 0 {
				w := earliestBlocked(ws, s.blockTime)
				s.waiters[m] = removeBlocked(ws, w)
				s.blockedS[w] = ""
				s.pc[w]++
				s.pc[i]++
				continue
			}
			if s.count[m] >= s.maximum[m] {
				s.faultTask = i
				s.faultSem = m
				return true
			}
			s.count[m]++
			s.pc[i]++
		}
	}
}

// pickBest selects the runnable task with the highest priority, breaking ties
// by release time then input index.
func (s *semSim) pickBest() int {
	best := -1
	for i := 0; i < s.n; i++ {
		if !s.released[i] || s.completed[i] || s.blockedS[i] != "" {
			continue
		}
		if best == -1 {
			best = i
			continue
		}
		a, b := s.tasks[i], s.tasks[best]
		if a.priority < b.priority ||
			(a.priority == b.priority && a.release < b.release) ||
			(a.priority == b.priority && a.release == b.release && i < best) {
			best = i
		}
	}
	return best
}

func (s *semSim) runnable(i int) bool {
	return s.released[i] && !s.completed[i] && s.blockedS[i] == "" && s.runRem[i] > 0
}

func (s *semSim) releaseUpTo() {
	for s.nextRel < s.n && s.tasks[s.order[s.nextRel]].release <= s.now {
		s.released[s.order[s.nextRel]] = true
		s.nextRel++
	}
}

func (s *semSim) allCompleted() bool {
	for i := 0; i < s.n; i++ {
		if !s.completed[i] {
			return false
		}
	}
	return true
}

func (s *semSim) buildResponse(overflow, stalled bool) semResponse {
	timeline := make([]semTimelineInterval, 0, len(s.segs))
	for _, seg := range s.segs {
		last := len(timeline) - 1
		if last >= 0 &&
			timeline[last].TaskID == s.tasks[seg.index].id &&
			timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, semTimelineInterval{
			TaskID: s.tasks[seg.index].id,
			Start:  seg.start,
			End:    seg.end,
		})
	}

	results := make([]semTaskResult, s.n)
	for i, t := range s.tasks {
		state := "ready"
		switch {
		case s.completed[i]:
			state = "completed"
		case s.blockedS[i] != "":
			state = "blocked"
		case !s.released[i]:
			state = "unreleased"
		}
		var blockedOn *string
		if state == "blocked" {
			m := s.blockedS[i]
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
		results[i] = semTaskResult{
			Executed:       s.executed[i],
			Completion:     s.completion[i],
			State:          state,
			BlockedOn:      blockedOn,
			DeadlineStatus: deadlineStatus,
		}
	}

	status := "horizon"
	switch {
	case overflow:
		status = "overflow"
	case s.allCompleted():
		status = "completed"
	case stalled:
		status = "stalled"
	}

	resp := semResponse{
		Status:    status,
		StoppedAt: s.now,
		Timeline:  timeline,
		Results:   results,
	}
	if overflow {
		at := s.now
		resp.FaultAt = &at
		id := s.tasks[s.faultTask].id
		resp.FaultTask = &id
		sem := s.faultSem
		resp.FaultSemaphore = &sem
	}
	return resp
}
