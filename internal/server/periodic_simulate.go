package server

import (
	"encoding/json"
	"net/http"
)

const (
	periodicSimulatePath = "/v1/schedules/periodic/simulate"
	// maxSimulateJobs bounds the total number of jobs released within the
	// horizon across all tasks, keeping the expanded simulation finite.
	maxSimulateJobs = 100000
)

type periodicSimulateTaskIn struct {
	ID        string `json:"id"`
	Priority  *int64 `json:"priority"`
	Execution *int64 `json:"execution"`
	Period    *int64 `json:"period"`
	Deadline  *int64 `json:"deadline"`
	Offset    *int64 `json:"offset"`
}

type periodicSimulateRequest struct {
	Horizon *int64                    `json:"horizon"`
	Tasks   *[]periodicSimulateTaskIn `json:"tasks"`
}

type periodicSimInterval struct {
	TaskID string `json:"taskId"`
	Job    int    `json:"job"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

type periodicSimJobResult struct {
	Job              int    `json:"job"`
	Release          int64  `json:"release"`
	AbsoluteDeadline int64  `json:"absoluteDeadline"`
	Executed         int64  `json:"executed"`
	Remaining        int64  `json:"remaining"`
	Completion       *int64 `json:"completion"`
	DeadlineStatus   string `json:"deadlineStatus"`
}

type periodicSimTaskResult struct {
	ID   string                 `json:"id"`
	Jobs []periodicSimJobResult `json:"jobs"`
}

type periodicSimulateResponse struct {
	Status   string                  `json:"status"`
	Timeline []periodicSimInterval   `json:"timeline"`
	Results  []periodicSimTaskResult `json:"results"`
}

// simJob is one released job of a periodic task; deadline is absolute.
type simJob struct {
	release    int64
	deadline   int64
	executed   int64
	remaining  int64
	completion *int64
}

// simTask carries the validated task parameters plus its expanded jobs.
// released counts jobs with release <= now; active is the oldest released
// job not yet complete (jobs of a task always run in job-number order).
type simTask struct {
	id        string
	priority  int64
	execution int64
	period    int64
	deadline  int64
	offset    int64
	jobs      []simJob
	released  int
	active    int
}

type simSegment struct {
	task, job  int
	start, end int64
}

func handlePeriodicSimulate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req periodicSimulateRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	tasks, horizon, ok := validatePeriodicSimulateRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(simulatePeriodic(tasks, horizon))
}

func validatePeriodicSimulateRequest(req *periodicSimulateRequest) ([]simTask, int64, bool) {
	if req.Horizon == nil || *req.Horizon <= 0 || *req.Horizon > maxHorizon {
		return nil, 0, false
	}
	horizon := *req.Horizon
	if req.Tasks == nil || len(*req.Tasks) == 0 || len(*req.Tasks) > maxTasks {
		return nil, 0, false
	}
	tasks := make([]simTask, 0, len(*req.Tasks))
	ids := make(map[string]struct{}, len(*req.Tasks))
	priorities := make(map[int64]struct{}, len(*req.Tasks))
	totalJobs := 0
	for i := range *req.Tasks {
		in := (*req.Tasks)[i]
		if !validTaskID(in.ID) {
			return nil, 0, false
		}
		if _, dup := ids[in.ID]; dup {
			return nil, 0, false
		}
		ids[in.ID] = struct{}{}
		if in.Priority == nil || *in.Priority < 0 || *in.Priority > 255 {
			return nil, 0, false
		}
		if _, dup := priorities[*in.Priority]; dup {
			return nil, 0, false
		}
		priorities[*in.Priority] = struct{}{}
		if in.Execution == nil || *in.Execution <= 0 || *in.Execution > maxPeriodicValue {
			return nil, 0, false
		}
		if in.Period == nil || *in.Period <= 0 || *in.Period > maxPeriodicValue {
			return nil, 0, false
		}
		if in.Deadline == nil || *in.Deadline <= 0 || *in.Deadline > maxPeriodicValue ||
			*in.Deadline > *in.Period {
			return nil, 0, false
		}
		if in.Offset == nil || *in.Offset < 0 || *in.Offset >= *in.Period ||
			*in.Offset >= horizon {
			return nil, 0, false
		}
		// Jobs release at offset+k*period < horizon; offset < horizon, so the
		// count is at least 1 and the arithmetic stays well inside int64.
		count := (horizon-*in.Offset-1)/(*in.Period) + 1
		totalJobs += int(count)
		if totalJobs > maxSimulateJobs {
			return nil, 0, false
		}
		jobs := make([]simJob, count)
		for k := range jobs {
			release := *in.Offset + int64(k)*(*in.Period)
			jobs[k] = simJob{
				release:   release,
				deadline:  release + *in.Deadline,
				remaining: *in.Execution,
			}
		}
		tasks = append(tasks, simTask{
			id:        in.ID,
			priority:  *in.Priority,
			execution: *in.Execution,
			period:    *in.Period,
			deadline:  *in.Deadline,
			offset:    *in.Offset,
			jobs:      jobs,
		})
	}
	return tasks, horizon, true
}

// simulatePeriodic runs an event-driven single-core fixed-priority
// fully-preemptive simulation of the expanded periodic jobs over
// [0, horizon). Blocking, jitter and switch overheads are ignored; a job
// keeps executing past its absolute deadline.
func simulatePeriodic(tasks []simTask, horizon int64) periodicSimulateResponse {
	totalJobs := 0
	for i := range tasks {
		totalJobs += len(tasks[i].jobs)
	}
	completed := 0

	var segments []simSegment
	current := -1
	var now int64
	for now < horizon {
		for i := range tasks {
			t := &tasks[i]
			for t.released < len(t.jobs) && t.jobs[t.released].release <= now {
				t.released++
			}
		}
		if current >= 0 && tasks[current].jobs[tasks[current].active].remaining == 0 {
			current = -1
		}
		// Ready task with the smallest priority value; its ready job is the
		// oldest released incomplete one. Priorities are unique, so a tie
		// between tasks cannot occur.
		best := -1
		for i := range tasks {
			if tasks[i].active < tasks[i].released &&
				(best == -1 || tasks[i].priority < tasks[best].priority) {
				best = i
			}
		}
		switch {
		case current == -1:
			current = best
		case best != -1 && tasks[best].priority < tasks[current].priority:
			// A strictly higher priority release preempts immediately; a new
			// job of the same or a lower priority never does.
			current = best
		}

		if current == -1 {
			// Idle: jump to the next release (or the horizon).
			next := int64(-1)
			for i := range tasks {
				t := &tasks[i]
				if t.released < len(t.jobs) {
					if r := t.jobs[t.released].release; next == -1 || r < next {
						next = r
					}
				}
			}
			if next == -1 {
				now = horizon
				break
			}
			now = next
			continue
		}

		// Run until the job completes, the horizon, or the next
		// strictly-higher-priority release, whichever comes first.
		cur := &tasks[current]
		job := &cur.jobs[cur.active]
		runEnd := horizon
		if job.remaining < runEnd-now {
			runEnd = now + job.remaining
		}
		for i := range tasks {
			if tasks[i].priority >= cur.priority {
				continue
			}
			t := &tasks[i]
			if t.released < len(t.jobs) {
				if r := t.jobs[t.released].release; r > now && r < runEnd {
					runEnd = r
				}
			}
		}

		segments = append(segments, simSegment{task: current, job: cur.active, start: now, end: runEnd})
		delta := runEnd - now
		job.executed += delta
		job.remaining -= delta
		now = runEnd
		if job.remaining == 0 {
			done := now
			job.completion = &done
			completed++
			cur.active++
			current = -1
		}
	}

	// Merge adjacent intervals of the same job; idle time is omitted.
	timeline := make([]periodicSimInterval, 0, len(segments))
	for _, seg := range segments {
		last := len(timeline) - 1
		if last >= 0 && timeline[last].TaskID == tasks[seg.task].id &&
			timeline[last].Job == seg.job && timeline[last].End == seg.start {
			timeline[last].End = seg.end
			continue
		}
		timeline = append(timeline, periodicSimInterval{
			TaskID: tasks[seg.task].id,
			Job:    seg.job,
			Start:  seg.start,
			End:    seg.end,
		})
	}

	results := make([]periodicSimTaskResult, len(tasks))
	for i := range tasks {
		t := &tasks[i]
		jobs := make([]periodicSimJobResult, len(t.jobs))
		for j := range t.jobs {
			jb := &t.jobs[j]
			var status string
			switch c := jb.completion; {
			case c != nil && *c <= jb.deadline:
				status = "met"
			case c != nil:
				status = "missed"
			case jb.deadline <= horizon:
				status = "missed"
			default:
				status = "pending"
			}
			jobs[j] = periodicSimJobResult{
				Job:              j,
				Release:          jb.release,
				AbsoluteDeadline: jb.deadline,
				Executed:         jb.executed,
				Remaining:        jb.remaining,
				Completion:       jb.completion,
				DeadlineStatus:   status,
			}
		}
		results[i] = periodicSimTaskResult{ID: t.id, Jobs: jobs}
	}

	status := "completed"
	if completed < totalJobs {
		status = "horizon"
	}
	return periodicSimulateResponse{Status: status, Timeline: timeline, Results: results}
}
