export interface LaneSpec {
  name: string;
  branch: string;
  brief: string;
  effort: 'low' | 'medium' | 'high' | 'xhigh';
}
export interface CodexLanesInput { brief: string; repo: string; lanes: LaneSpec[] }
export interface PrepareLaneInput { brief: string; repo: string; lane: LaneSpec; runId: string }
export interface PreparedLane {
  name: string;
  branch: string;
  effort: LaneSpec['effort'];
  repo: string;
  worktree: string;
  stateDir: string;
  briefPath: string;
  baseCommit: string;
  targetBranch: string;
}
export interface LaneReport extends PreparedLane { laneName: string; reportPath: string; head: string }
export interface MergeResult { name: string; branch: string; head: string }
