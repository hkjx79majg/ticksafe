package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"unicode/utf8"
)

const (
	mailboxAnalyzePath = "/v1/schedules/mailbox/analyze"
	maxMailboxes       = 256
	maxMailboxCapacity = 65535
	maxMailboxActions  = 1024
	maxMessageRunes    = 256
)

// Mailbox action discriminators.
const (
	mboxActRun int = iota
	mboxActSend
	mboxActReceive
)

type mboxSendIn struct {
	Mailbox string `json:"mailbox"`
	Message string `json:"message"`
}

type mboxActionIn struct {
	Run     *int64      `json:"run"`
	Send    *mboxSendIn `json:"send"`
	Receive *string     `json:"receive"`
}

type mboxTaskIn struct {
	ID       string          `json:"id"`
	Priority *int64          `json:"priority"`
	Release  *int64          `json:"release"`
	Deadline *int64          `json:"deadline"`
	Actions  *[]mboxActionIn `json:"actions"`
}

type mboxMailboxIn struct {
	Name     string `json:"name"`
	Capacity *int64 `json:"capacity"`
}

type mboxRequest struct {
	Horizon   *int64           `json:"horizon"`
	Mailboxes *[]mboxMailboxIn `json:"mailboxes"`
	Tasks     *[]mboxTaskIn    `json:"tasks"`
}

type mboxAction struct {
	kind    int
	run     int64
	mailbox string // send/receive target
	message string // send payload
}

type mboxTask struct {
	id       string
	priority int64
	release  int64
	deadline int64
	actions  []mboxAction
}

type mboxTimelineInterval struct {
	TaskID string `json:"taskId"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

type mboxReceived struct {
	Mailbox string `json:"mailbox"`
	Message string `json:"message"`
	Time    int64  `json:"time"`
}

type mboxTaskResult struct {
	Executed       int64          `json:"executed"`
	Completion     *int64         `json:"completion"`
	State          string         `json:"state"`
	BlockedAction  *string        `json:"blockedAction"`
	BlockedOn      *string        `json:"blockedOn"`
	Received       []mboxReceived `json:"received"`
	DeadlineStatus string         `json:"deadlineStatus"`
}

type mboxResponse struct {
	Status    string                 `json:"status"`
	StoppedAt int64                  `json:"stoppedAt"`
	Timeline  []mboxTimelineInterval `json:"timeline"`
	Results   []mboxTaskResult       `json:"results"`
}

func handleMailboxAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req mboxRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	tasks, capacity, ok := validateMailboxRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(simulateMailbox(tasks, capacity, *req.Horizon))
}

// validateMailboxRequest enforces the one-shot task constraints (without
// execution), the mailbox declarations (1..256 unique id-shaped names with
// capacity 1..65535) and the action-program rules: exactly one verb per
// action, positive run microseconds, send/receive references pointing at
// declared mailboxes, messages of 1..256 Unicode code points, and per-task
// and global run totals that fit int64.
func validateMailboxRequest(req *mboxRequest) ([]mboxTask, map[string]int64, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, nil, false
	}
	if req.Mailboxes == nil || len(*req.Mailboxes) == 0 || len(*req.Mailboxes) > maxMailboxes {
		return nil, nil, false
	}
	capacity := make(map[string]int64, len(*req.Mailboxes))
	for i := range *req.Mailboxes {
		mb := (*req.Mailboxes)[i]
		if !validTaskID(mb.Name) {
			return nil, nil, false
		}
		if _, dup := capacity[mb.Name]; dup {
			return nil, nil, false
		}
		if mb.Capacity == nil || *mb.Capacity < 1 || *mb.Capacity > maxMailboxCapacity {
			return nil, nil, false
		}
		capacity[mb.Name] = *mb.Capacity
	}
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, nil, false
	}
	horizon := *req.Horizon
	tasks := make([]mboxTask, 0, len(*req.Tasks))
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
		if in.Actions == nil || len(*in.Actions) == 0 || len(*in.Actions) > maxMailboxActions {
			return nil, nil, false
		}

		actions := make([]mboxAction, 0, len(*in.Actions))
		var taskRun int64
		for j := range *in.Actions {
			ai := (*in.Actions)[j]
			verbs := 0
			if ai.Run != nil {
				verbs++
			}
			if ai.Send != nil {
				verbs++
			}
			if ai.Receive != nil {
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
				actions = append(actions, mboxAction{kind: mboxActRun, run: *ai.Run})
			case ai.Send != nil:
				name := ai.Send.Mailbox
				if _, ok := capacity[name]; !ok {
					return nil, nil, false
				}
				if n := utf8.RuneCountInString(ai.Send.Message); n < 1 || n > maxMessageRunes {
					return nil, nil, false
				}
				actions = append(actions, mboxAction{kind: mboxActSend, mailbox: name, message: ai.Send.Message})
			default:
				name := *ai.Receive
				if _, ok := capacity[name]; !ok {
					return nil, nil, false
				}
				actions = append(actions, mboxAction{kind: mboxActReceive, mailbox: name})
			}
		}
		var ok bool
		totalRun, ok = saturatingAdd(totalRun, taskRun)
		if !ok {
			return nil, nil, false
		}
		tasks = append(tasks, mboxTask{
			id:       in.ID,
			priority: *in.Priority,
			release:  *in.Release,
			deadline: *in.Deadline,
			actions:  actions,
		})
	}
	return tasks, capacity, true
}

// mboxSegment is an unmerged run interval recorded by the simulation.
type mboxSegment struct {
	index      int
	start, end int64
}

// mboxSim is an event-driven single-core simulation of bounded FIFO
// mailboxes. Time advances only through run actions; send, receive and
// completion happen at a single instant. Communication never alters
// priorities. Two invariants hold per mailbox: blocked receivers imply an
// empty queue (a send always prefers direct delivery), and blocked senders
// imply a full queue (a receive from a full mailbox immediately refills it
// from the earliest blocked sender).
type mboxSim struct {
	tasks    []mboxTask
	n        int
	horizon  int64
	capacity map[string]int64

	queue     map[string][]string // mailbox -> queued messages, head first
	senders   map[string][]int    // mailbox -> tasks blocked sending
	receivers map[string][]int    // mailbox -> tasks blocked receiving

	blockedWhat []string // "send"/"receive" when blocked, "" otherwise
	blockedM    []string // mailbox name when blocked, "" otherwise
	blockTime   []int64
	pc          []int   // next action to interpret
	runRem      []int64 // remainder of the run action at pc

	released   []bool
	completed  []bool
	executed   []int64
	completion []*int64
	received   [][]mboxReceived

	order   []int // tasks sorted by (release, input index)
	nextRel int
	segs    []mboxSegment
	current int
	now     int64
}

func simulateMailbox(tasks []mboxTask, capacity map[string]int64, horizon int64) mboxResponse {
	n := len(tasks)
	s := &mboxSim{
		tasks:       tasks,
		n:           n,
		horizon:     horizon,
		capacity:    capacity,
		queue:       make(map[string][]string),
		senders:     make(map[string][]int),
		receivers:   make(map[string][]int),
		blockedWhat: make([]string, n),
		blockedM:    make([]string, n),
		blockTime:   make([]int64, n),
		pc:          make([]int, n),
		runRem:      make([]int64, n),
		released:    make([]bool, n),
		completed:   make([]bool, n),
		executed:    make([]int64, n),
		completion:  make([]*int64, n),
		received:    make([][]mboxReceived, n),
		current:     -1,
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

	stalled := false
loop:
	for {
		s.releaseUpTo()

		// After any state change, first drain the current task's consecutive
		// zero-duration actions, then schedule.
		if s.current != -1 {
			s.settle(s.current)
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
			s.settle(best)
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
				s.settle(best)
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
		s.segs = append(s.segs, mboxSegment{index: cur, start: s.now, end: end})
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
			s.settle(cur)
			break
		}
	}

	return s.buildResponse(stalled)
}

// settle drains the task's consecutive zero-duration actions. It stops when
// the task faces a run action (arming its remainder), blocks, or completes.
func (s *mboxSim) settle(i int) {
	for {
		t := s.tasks[i]
		if s.pc[i] >= len(t.actions) {
			s.completed[i] = true
			now := s.now
			s.completion[i] = &now
			return
		}
		a := t.actions[s.pc[i]]
		switch a.kind {
		case mboxActRun:
			if s.runRem[i] == 0 {
				s.runRem[i] = a.run
			}
			return
		case mboxActSend:
			m := a.mailbox
			// Direct delivery to the earliest blocked receiver wins over
			// queuing; the message never enters the queue.
			if rs := s.receivers[m]; len(rs) > 0 {
				w := earliestBlocked(rs, s.blockTime)
				s.receivers[m] = removeBlocked(rs, w)
				s.unblock(w)
				s.received[w] = append(s.received[w], mboxReceived{Mailbox: m, Message: a.message, Time: s.now})
				s.pc[i]++
				continue
			}
			if int64(len(s.queue[m])) < s.capacity[m] {
				s.queue[m] = append(s.queue[m], a.message)
				s.pc[i]++
				continue
			}
			// Full mailbox: the sender blocks, carrying its message.
			s.block(i, "send", m)
			return
		case mboxActReceive:
			m := a.mailbox
			q := s.queue[m]
			if len(q) == 0 {
				s.block(i, "receive", m)
				return
			}
			s.queue[m] = q[1:]
			s.received[i] = append(s.received[i], mboxReceived{Mailbox: m, Message: q[0], Time: s.now})
			// If the mailbox was full, the earliest blocked sender's message
			// immediately moves to the tail and that sender unblocks.
			if int64(len(q)) == s.capacity[m] {
				if sd := s.senders[m]; len(sd) > 0 {
					w := earliestBlocked(sd, s.blockTime)
					s.senders[m] = removeBlocked(sd, w)
					s.queue[m] = append(s.queue[m], s.tasks[w].actions[s.pc[w]].message)
					s.unblock(w)
				}
			}
			s.pc[i]++
		}
	}
}

// block parks the task on a mailbox; its pc stays on the blocking action
// until unblock completes it.
func (s *mboxSim) block(i int, what, m string) {
	s.blockedWhat[i] = what
	s.blockedM[i] = m
	s.blockTime[i] = s.now
	if what == "send" {
		s.senders[m] = append(s.senders[m], i)
	} else {
		s.receivers[m] = append(s.receivers[m], i)
	}
}

// unblock releases a blocked task whose send or receive has been satisfied by
// another task's action, advancing it past the completed action.
func (s *mboxSim) unblock(i int) {
	s.blockedWhat[i] = ""
	s.blockedM[i] = ""
	s.pc[i]++
}

// earliestBlocked picks the task with the earliest block time, breaking ties
// at the same instant by input order.
func earliestBlocked(list []int, blockTime []int64) int {
	w := list[0]
	for _, x := range list[1:] {
		if blockTime[x] < blockTime[w] || (blockTime[x] == blockTime[w] && x < w) {
			w = x
		}
	}
	return w
}

func removeBlocked(list []int, x int) []int {
	for k, v := range list {
		if v == x {
			return append(list[:k], list[k+1:]...)
		}
	}
	return list
}

// pickBest selects the runnable task with the highest priority, breaking ties
// by release time then input index.
func (s *mboxSim) pickBest() int {
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
		if a.priority < b.priority ||
			(a.priority == b.priority && a.release < b.release) ||
			(a.priority == b.priority && a.release == b.release && i < best) {
			best = i
		}
	}
	return best
}

func (s *mboxSim) runnable(i int) bool {
	return s.released[i] && !s.completed[i] && s.blockedM[i] == "" && s.runRem[i] > 0
}

func (s *mboxSim) releaseUpTo() {
	for s.nextRel < s.n && s.tasks[s.order[s.nextRel]].release <= s.now {
		s.released[s.order[s.nextRel]] = true
		s.nextRel++
	}
}

func (s *mboxSim) allCompleted() bool {
	for i := 0; i < s.n; i++ {
		if !s.completed[i] {
			return false
		}
	}
	return true
}

func (s *mboxSim) buildResponse(stalled bool) mboxResponse {
	timeline := make([]mboxTimelineInterval, 0, len(s.segs))
	for _, seg := range s.segs {
		last := len(timeline) - 1
		if last >= 0 &&
			timeline[last].TaskID == s.tasks[seg.index].id &&
			timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, mboxTimelineInterval{
			TaskID: s.tasks[seg.index].id,
			Start:  seg.start,
			End:    seg.end,
		})
	}

	results := make([]mboxTaskResult, s.n)
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
		var blockedAction, blockedOn *string
		if state == "blocked" {
			what := s.blockedWhat[i]
			blockedAction = &what
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
		received := s.received[i]
		if received == nil {
			received = []mboxReceived{}
		}
		results[i] = mboxTaskResult{
			Executed:       s.executed[i],
			Completion:     s.completion[i],
			State:          state,
			BlockedAction:  blockedAction,
			BlockedOn:      blockedOn,
			Received:       received,
			DeadlineStatus: deadlineStatus,
		}
	}

	status := "horizon"
	switch {
	case s.allCompleted():
		status = "completed"
	case stalled:
		status = "stalled"
	}

	return mboxResponse{
		Status:    status,
		StoppedAt: s.now,
		Timeline:  timeline,
		Results:   results,
	}
}
