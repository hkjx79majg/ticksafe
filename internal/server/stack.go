package server

import (
	"encoding/json"
	"net/http"
)

const (
	stackAnalyzePath = "/v1/stacks/analyze"

	maxStackFunctions = 4096
	maxStackCalls     = 256
	maxStackEntries   = 256
	maxFrameBytes     = 1_000_000_000_000      // 1e12
	maxOverheadBytes  = 1_000_000_000_000      // 1e12
	maxStackLimit     = 10_000_000_000_000_000 // 1e16
)

type stackCallIn struct {
	Callee        string `json:"callee"`
	OverheadBytes *int64 `json:"overheadBytes"`
}

type stackFunctionIn struct {
	ID         string         `json:"id"`
	FrameBytes *int64         `json:"frameBytes"`
	Calls      *[]stackCallIn `json:"calls"`
}

type stackEntryIn struct {
	Entry      string `json:"entry"`
	StackLimit *int64 `json:"stackLimit"`
}

type stackRequest struct {
	Functions   *[]stackFunctionIn `json:"functions"`
	Entrypoints *[]stackEntryIn    `json:"entrypoints"`
}

// stackEdge is one declared call: the resolved callee index and the extra
// bytes pushed by the call itself.
type stackEdge struct {
	callee   int
	overhead int64
}

type stackFunction struct {
	id    string
	frame int64
	calls []stackEdge
}

type stackEntrypoint struct {
	index int
	limit int64
}

type stackResult struct {
	Entry         string   `json:"entry"`
	StackLimit    int64    `json:"stackLimit"`
	RequiredBytes *int64   `json:"requiredBytes"`
	Status        string   `json:"status"`
	WorstPath     []string `json:"worstPath"`
	Cycle         []string `json:"cycle"`
}

type stackAnalyzeResponse struct {
	Safe       bool          `json:"safe"`
	Analyzable bool          `json:"analyzable"`
	Results    []stackResult `json:"results"`
}

func handleStackAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAnalyzeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req stackRequest
	if !decodeStrictJSON(w, r, &req) {
		return
	}
	funcs, entries, ok := validateStackRequest(&req)
	if !ok {
		writeAnalyzeError(w, http.StatusUnprocessableEntity, "validation_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(analyzeStacks(funcs, entries))
}

// validateStackRequest enforces the call-graph contract: 1..4096 functions
// with unique id-shaped ids, frameBytes in [0,1e12], at most 256 calls each
// pointing at declared callees with overheadBytes in [0,1e12], and 1..256
// distinct entrypoints whose stackLimit is in [1,1e16]. A missing calls list
// is equivalent to an empty one.
func validateStackRequest(req *stackRequest) ([]stackFunction, []stackEntrypoint, bool) {
	if req.Functions == nil || len(*req.Functions) == 0 || len(*req.Functions) > maxStackFunctions {
		return nil, nil, false
	}
	funcs := make([]stackFunction, 0, len(*req.Functions))
	index := make(map[string]int, len(*req.Functions))
	for i := range *req.Functions {
		in := (*req.Functions)[i]
		if !validTaskID(in.ID) {
			return nil, nil, false
		}
		if _, dup := index[in.ID]; dup {
			return nil, nil, false
		}
		if in.FrameBytes == nil || *in.FrameBytes < 0 || *in.FrameBytes > maxFrameBytes {
			return nil, nil, false
		}
		if in.Calls != nil && len(*in.Calls) > maxStackCalls {
			return nil, nil, false
		}
		index[in.ID] = i
		funcs = append(funcs, stackFunction{id: in.ID, frame: *in.FrameBytes})
	}
	for i := range *req.Functions {
		in := (*req.Functions)[i]
		if in.Calls == nil {
			continue
		}
		edges := make([]stackEdge, 0, len(*in.Calls))
		for j := range *in.Calls {
			c := (*in.Calls)[j]
			if c.OverheadBytes == nil || *c.OverheadBytes < 0 || *c.OverheadBytes > maxOverheadBytes {
				return nil, nil, false
			}
			v, known := index[c.Callee]
			if !known {
				return nil, nil, false
			}
			edges = append(edges, stackEdge{callee: v, overhead: *c.OverheadBytes})
		}
		funcs[i].calls = edges
	}

	if req.Entrypoints == nil || len(*req.Entrypoints) == 0 || len(*req.Entrypoints) > maxStackEntries {
		return nil, nil, false
	}
	entries := make([]stackEntrypoint, 0, len(*req.Entrypoints))
	seen := make(map[string]struct{}, len(*req.Entrypoints))
	for i := range *req.Entrypoints {
		in := (*req.Entrypoints)[i]
		if in.StackLimit == nil || *in.StackLimit < 1 || *in.StackLimit > maxStackLimit {
			return nil, nil, false
		}
		v, known := index[in.Entry]
		if !known {
			return nil, nil, false
		}
		if _, dup := seen[in.Entry]; dup {
			return nil, nil, false
		}
		seen[in.Entry] = struct{}{}
		entries = append(entries, stackEntrypoint{index: v, limit: *in.StackLimit})
	}
	return funcs, entries, true
}

// DFS vertex colors for directed cycle detection.
const (
	stackColorWhite byte = iota
	stackColorGray
	stackColorBlack
)

// analyzeStacks runs an independent depth-first analysis for every entrypoint
// over only its reachable subgraph. A back edge (an edge to a gray vertex)
// means recursion is reachable and the stack has no finite upper bound; the
// first such edge in calls-order DFS supplies the reported cycle. Otherwise
// the reachable graph is a DAG and requiredBytes is its longest weighted path,
// where each function adds its frame and each edge its overhead.
func analyzeStacks(funcs []stackFunction, entries []stackEntrypoint) stackAnalyzeResponse {
	n := len(funcs)
	results := make([]stackResult, len(entries))
	safe, analyzable := true, true
	for ei, ep := range entries {
		color := make([]byte, n)
		required := make([]int64, n)
		pos := make([]int, n)
		for i := range pos {
			pos[i] = -1
		}
		var stack []int
		var cycleNodes []int
		foundCycle := false

		var dfs func(u int)
		dfs = func(u int) {
			color[u] = stackColorGray
			pos[u] = len(stack)
			stack = append(stack, u)
			for _, e := range funcs[u].calls {
				v := e.callee
				switch color[v] {
				case stackColorGray:
					// First back edge u -> v; the cycle runs from v (the
					// back-edge target) along the call direction to u.
					cycleNodes = append(cycleNodes, stack[pos[v]:]...)
					foundCycle = true
					return
				case stackColorWhite:
					dfs(v)
					if foundCycle {
						return
					}
				}
			}
			color[u] = stackColorBlack
			// Postorder: every reachable successor already has its value.
			best := int64(0)
			for _, e := range funcs[u].calls {
				if cand := e.overhead + required[e.callee]; cand > best {
					best = cand
				}
			}
			required[u] = funcs[u].frame + best
			stack = stack[:len(stack)-1]
			pos[u] = -1
		}
		dfs(ep.index)

		res := stackResult{
			Entry:      funcs[ep.index].id,
			StackLimit: ep.limit,
			WorstPath:  []string{},
			Cycle:      []string{},
		}
		if foundCycle {
			res.Status = "unbounded"
			for _, v := range cycleNodes {
				res.Cycle = append(res.Cycle, funcs[v].id)
			}
			safe = false
			analyzable = false
		} else {
			need := required[ep.index]
			res.RequiredBytes = &need
			if need <= ep.limit {
				res.Status = "within_limit"
			} else {
				res.Status = "exceeded"
				safe = false
			}
			// Rebuild the lexicographically earliest maximum path: at each
			// function, stopping ties every continuation of value 0 (the empty
			// suffix sorts first); otherwise take the earliest call index whose
			// continuation reaches the maximum.
			u := ep.index
			res.WorstPath = append(res.WorstPath, funcs[u].id)
			for {
				best := int64(0)
				for _, e := range funcs[u].calls {
					if cand := e.overhead + required[e.callee]; cand > best {
						best = cand
					}
				}
				if best == 0 {
					break
				}
				for _, e := range funcs[u].calls {
					if e.overhead+required[e.callee] == best {
						u = e.callee
						res.WorstPath = append(res.WorstPath, funcs[u].id)
						break
					}
				}
			}
		}
		results[ei] = res
	}
	return stackAnalyzeResponse{Safe: safe, Analyzable: analyzable, Results: results}
}
