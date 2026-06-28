/**
 * Workflow manager for background execution, pause/resume, and run management.
 */

import { EventEmitter } from "node:events";
import type { WorkflowAgent } from "./agent.js";
import { DEFAULT_RUN_STALL_TIMEOUT_MS } from "./config.js";
import { preview, type WorkflowSnapshot } from "./display.js";
import { WorkflowError, WorkflowErrorCode } from "./errors.js";
import {
  createRunPersistence,
  generateRunId,
  type PersistedRunState,
  type RunLease,
  type RunPersistence,
  type RunStatus,
} from "./run-persistence.js";
import { type JournalEntry, parseWorkflowScript, runWorkflow, type WorkflowRunResult } from "./workflow.js";

export interface ManagedRun {
  runId: string;
  status: RunStatus;
  snapshot: WorkflowSnapshot;
  result?: WorkflowRunResult;
  error?: WorkflowError;
  controller: AbortController;
  startedAt: Date;
  /** The real script, kept so the run can be resumed. */
  script: string;
  args?: unknown;
  /** Accumulated agent results for resume (deterministic call index -> result). */
  journal: JournalEntry[];
  /** Cross-process execution lease for this run, when it is actively executing. */
  lease?: RunLease;
  /**
   * True when the run was started in the background (or resumed) and the caller is
   * not awaiting its result inline. Only background runs deliver their result back
   * into the conversation; a foreground sync run already returns it as the tool
   * result, so re-delivering would duplicate it.
   */
  background: boolean;
}

/** Per-execution options shared by sync, background, and resume runs. */
export interface ExecOptions {
  /** Replay these journaled agent results for the unchanged prefix (resume). */
  resumeJournal?: Map<number, JournalEntry>;
  /** Cap on total agents for this run. */
  maxAgents?: number;
  /** Per-agent timeout in milliseconds. null/omitted means no hard timeout. */
  agentTimeoutMs?: number | null;
  /** Host signal (e.g. tool/Esc) that should abort this run when fired. */
  externalSignal?: AbortSignal;
  /** Called with the live snapshot on every progress event. */
  onProgress?: (snapshot: WorkflowSnapshot) => void;
  /** Hard token budget for this run; once spent reaches it, agent() throws. */
  tokenBudget?: number | null;
  /** Max concurrent agents for this execution. */
  concurrency?: number;
  /** Retry attempts after recoverable agent failures for this execution. */
  agentRetries?: number;
  /**
   * Run-level stall watchdog (ms): abort the run if it makes zero progress (no
   * agent event, log, phase change, or token usage) for this long. Guarantees a
   * hung await (dead subagent) releases the lease instead of stranding the run.
   * null/0 disables. Defaults to the manager's defaultRunStallTimeoutMs.
   */
  runStallTimeoutMs?: number | null;
  /** Resolve a checkpoint() question with a human reply (only for UI-bearing runs). */
  confirm?: (promptText: string, options: unknown) => Promise<unknown>;
}

export interface WorkflowManagerOptions {
  cwd?: string;
  concurrency?: number;
  /** Resolve a saved-workflow name to its script, enabling nested `workflow('name')`. */
  loadSavedWorkflow?: (name: string) => string | undefined;
  /** Inject a custom agent runner (tests); defaults to a real subagent session. */
  agent?: Pick<WorkflowAgent, "run">;
  /** The session's main model (provider/id), for auto-tiering explore agents. */
  mainModel?: string;
  /** The pi session id to tag runs with (see setSessionId). */
  sessionId?: string;
  /** Default per-agent timeout when a run does not pass agentTimeoutMs. null means no hard timeout. */
  defaultAgentTimeoutMs?: number | null;
  /** Default retry attempts after recoverable agent failures. */
  defaultAgentRetries?: number;
  /**
   * Default run-level stall watchdog (ms) when a run does not pass runStallTimeoutMs.
   * Aborts a run that makes zero progress for this long (hung await / dead
   * subagent), guaranteeing lease release. null/0 disables. See DEFAULT_RUN_STALL_TIMEOUT_MS.
   */
  defaultRunStallTimeoutMs?: number | null;
}

export class WorkflowManager extends EventEmitter {
  private runs = new Map<string, ManagedRun>();
  private persistence: RunPersistence;
  private cwd: string;
  private concurrency: number;
  private loadSavedWorkflow?: (name: string) => string | undefined;
  private agent?: Pick<WorkflowAgent, "run">;
  /** The session's main model (provider/id), for auto-tiering explore agents. */
  private mainModel?: string;
  /** The current pi session id; runs are stamped with it and listRuns() filters by it. */
  private sessionId?: string;
  private defaultAgentTimeoutMs: number | null;
  private defaultAgentRetries: number;
  private defaultRunStallTimeoutMs: number | null;

  constructor(options: WorkflowManagerOptions = {}) {
    super();
    this.cwd = options.cwd ?? process.cwd();
    this.concurrency = options.concurrency ?? 8;
    this.loadSavedWorkflow = options.loadSavedWorkflow;
    this.agent = options.agent;
    this.mainModel = options.mainModel;
    this.sessionId = options.sessionId;
    this.defaultAgentTimeoutMs = options.defaultAgentTimeoutMs ?? null;
    this.defaultAgentRetries = options.defaultAgentRetries ?? 0;
    this.defaultRunStallTimeoutMs = options.defaultRunStallTimeoutMs ?? DEFAULT_RUN_STALL_TIMEOUT_MS;
    this.persistence = createRunPersistence(this.cwd);
    this.recoverStaleRuns();
  }

  /** Bind the manager to the current pi session, so new runs are tagged with it and
   * the navigator/task-panel show only this session's runs (set on session_start). */
  setSessionId(id: string | undefined): void {
    this.sessionId = id;
  }

  /**
   * On startup, any persisted run still marked "running" belongs to a process
   * that died mid-run (this fresh manager has it nowhere in memory). Reconcile it
   * to "paused" — never "failed" — so its journal is preserved and resume() can
   * replay the completed prefix and finish the rest.
   */
  private recoverStaleRuns(): void {
    try {
      for (const p of this.listAllRuns()) {
        if (p.status === "running" && !this.runs.has(p.runId)) {
          const lease = this.persistence.acquireRunLease(p.runId);
          if (!lease) continue;
          try {
            this.persistence.save({ ...p, status: "paused" });
          } finally {
            this.persistence.releaseRunLease(lease);
          }
        }
      }
    } catch {
      // Recovery is best-effort; never let it block manager construction.
    }
  }

  /** Set the session's main model (provider/id). Used to auto-tier explore agents. */
  setMainModel(spec: string | undefined): void {
    this.mainModel = spec;
  }

  /**
   * Start a workflow in the background.
   * Returns immediately with a run ID; the workflow executes asynchronously.
   */
  startInBackground(
    script: string,
    args?: unknown,
    exec: ExecOptions = {},
  ): { runId: string; promise: Promise<WorkflowRunResult> } {
    const runId = generateRunId();
    const controller = new AbortController();
    const parsed = parseWorkflowScript(script);
    const lease = this.persistence.acquireRunLease(runId);
    if (!lease) throw new Error(`Could not acquire workflow run lease for ${runId}`);

    const managed: ManagedRun = {
      runId,
      status: "running",
      snapshot: {
        name: parsed.meta.name,
        description: parsed.meta.description,
        phases: parsed.meta.phases?.map((p) => p.title) ?? [],
        logs: [],
        agents: [],
        agentCount: 0,
        runningCount: 0,
        doneCount: 0,
        errorCount: 0,
      },
      controller,
      startedAt: new Date(),
      script,
      args,
      journal: [],
      background: true,
      lease,
    };

    this.runs.set(runId, managed);

    try {
      // Persist initial state
      this.persistence.save({
        runId,
        workflowName: parsed.meta.name,
        script,
        args,
        sessionId: this.sessionId,
        status: "running",
        phases: managed.snapshot.phases,
        agents: [],
        logs: [],
        startedAt: managed.startedAt.toISOString(),
        updatedAt: managed.startedAt.toISOString(),
      });
    } catch (err) {
      this.releaseRunLease(managed);
      this.runs.delete(runId);
      throw err;
    }

    // Run workflow asynchronously.
    // Attach a side-channel catch to prevent Node.js unhandled-rejection crashes
    // when a workflow is aborted/paused/stopped — executeRun()'s catch block
    // already records status/event/persist, but the promise still rejects.
    // The original promise is returned so callers can await it in try/catch.
    const promise = this.executeRun(managed, script, args, exec);
    promise.catch(() => {});

    return { runId, promise };
  }

  /**
   * Execute a workflow synchronously (blocking) while still tracking it like a
   * background run, so the `/workflows` navigator and the live task panel see it.
   * `onProgress` fires on every progress event with the current snapshot, letting
   * a caller (e.g. the workflow tool) drive its own inline display.
   */
  async runSync(script: string, args?: unknown, exec: ExecOptions = {}): Promise<WorkflowRunResult> {
    const managed = this.createManaged(script, args);
    const lease = this.persistence.acquireRunLease(managed.runId);
    if (!lease) throw new Error(`Could not acquire workflow run lease for ${managed.runId}`);
    managed.lease = lease;
    this.runs.set(managed.runId, managed);
    // Persist the initial state immediately so listRuns()/the task panel can see
    // the run the moment it starts, not only after the first agent journals.
    this.persistRun(managed);
    return this.executeRun(managed, script, args, exec);
  }

  /** Build a fresh managed run with an empty snapshot. */
  private createManaged(script: string, args?: unknown): ManagedRun {
    const parsed = parseWorkflowScript(script);
    return {
      runId: generateRunId(),
      status: "running",
      snapshot: {
        name: parsed.meta.name,
        description: parsed.meta.description,
        phases: parsed.meta.phases?.map((p) => p.title) ?? [],
        logs: [],
        agents: [],
        agentCount: 0,
        runningCount: 0,
        doneCount: 0,
        errorCount: 0,
      },
      controller: new AbortController(),
      startedAt: new Date(),
      script,
      args,
      journal: [],
      background: false,
    };
  }

  private async executeRun(
    managed: ManagedRun,
    script: string,
    args?: unknown,
    exec: ExecOptions = {},
  ): Promise<WorkflowRunResult> {
    const {
      resumeJournal,
      maxAgents,
      agentTimeoutMs,
      externalSignal,
      onProgress,
      tokenBudget,
      concurrency,
      agentRetries,
      runStallTimeoutMs,
      confirm,
    } = exec;
    const resolvedAgentTimeoutMs = agentTimeoutMs !== undefined ? agentTimeoutMs : this.defaultAgentTimeoutMs;
    const resolvedConcurrency = concurrency ?? this.concurrency;
    const resolvedAgentRetries = agentRetries ?? this.defaultAgentRetries;
    const resolvedRunStallTimeoutMs =
      runStallTimeoutMs !== undefined ? runStallTimeoutMs : this.defaultRunStallTimeoutMs;
    // Stall watchdog (see comment at the Promise.race below). Created here, before
    // `progress`, so every progress() call resets it — covering all agent/log/phase/
    // token-usage callbacks without touching each one.
    const stall = createStallWatchdog(resolvedRunStallTimeoutMs, managed.runId);
    const progress = () => {
      stall.reset();
      onProgress?.(managed.snapshot);
    };
    // Let a host abort (e.g. Esc during a blocking tool call) cancel this run.
    if (externalSignal) {
      if (externalSignal.aborted) managed.controller.abort();
      else externalSignal.addEventListener("abort", () => managed.controller.abort(), { once: true });
    }
    try {
      // Stall watchdog: a run that makes zero progress for resolvedRunStallTimeoutMs
      // is treated as dead (hung await — typically a subagent process that died
      // without rejecting its promise). Without this, the hung await holds the lease
      // forever and the run becomes permanently un-resumable. progress() resets it
      // on every agent start/end, log, phase change, and token usage (see above),
      // so legitimately long but healthy runs never trip it. On fire it rejects via
      // Promise.race so executeRun's catch runs normally (status + lease release).
      stall.arm();
      try {
        const result = await Promise.race([
          runWorkflow(script, {
            cwd: this.cwd,
            args,
            agent: this.agent,
            mainModel: this.mainModel,
            signal: managed.controller.signal,
            concurrency: resolvedConcurrency,
            agentRetries: resolvedAgentRetries,
            maxAgents,
            agentTimeoutMs: resolvedAgentTimeoutMs,
            tokenBudget,
            confirm,
            loadSavedWorkflow: this.loadSavedWorkflow,
            resumeJournal,
            resumeFromRunId: resumeJournal ? managed.runId : undefined,
            onAgentJournal: (entry) => {
              // Append (crash-safe-ish): keep the latest entry per index, then persist.
              managed.journal = managed.journal.filter((e) => e.index !== entry.index);
              managed.journal.push(entry);
              this.persistRun(managed);
              stall.reset(); // a journaled result is progress — keep the watchdog alive
            },
            onLog: (message) => {
              managed.snapshot.logs.push(message);
              this.emit("log", { runId: managed.runId, message });
              progress();
            },
            onPhase: (title) => {
              managed.snapshot.currentPhase = title;
              if (!managed.snapshot.phases.includes(title)) {
                managed.snapshot.phases.push(title);
              }
              this.emit("phase", { runId: managed.runId, title });
              progress();
            },
            onAgentStart: (event) => {
              managed.snapshot.agents.push({
                id: managed.snapshot.agents.length + 1,
                label: event.label,
                phase: event.phase,
                prompt: event.prompt,
                status: "running",
                model: event.model,
              });
              this.emit("agentStart", { runId: managed.runId, ...event });
              progress();
            },
            onAgentEnd: (event) => {
              const agent = [...managed.snapshot.agents]
                .reverse()
                .find((a) => a.label === event.label && a.status === "running");
              if (agent) {
                agent.status = event.result === null ? "error" : "done";
                agent.resultPreview = preview(event.result);
                agent.error = event.error;
                agent.errorCode = event.errorCode;
                agent.recoverable = event.recoverable;
                agent.tokens = event.tokens;
                if (event.model) agent.model = event.model;
              }
              this.emit("agentEnd", { runId: managed.runId, ...event });
              progress();
            },
            onAgentHistory: (event) => {
              const agent = [...managed.snapshot.agents]
                .reverse()
                .find((a) => a.label === event.label && a.status === "running");
              if (agent) {
                agent.history = event.history;
              }
              this.emit("agentHistory", { runId: managed.runId, ...event });
              progress();
            },
            onTokenUsage: (usage) => {
              managed.snapshot.tokenUsage = usage;
              this.emit("tokenUsage", { runId: managed.runId, usage });
              progress();
            },
          }),
          stall.promise(),
        ]);

        managed.status = "completed";
        managed.result = result;
        this.emit("complete", { runId: managed.runId, result });

        // Persist final state
        this.persistRun(managed);
        this.releaseRunLease(managed);

        return result;
      } finally {
        // Disarm the stall watchdog once the run has settled (success or throw).
        // If the run threw, the outer catch below handles status + lease; the stall
        // promise is no longer needed and its timer must not keep firing.
        stall.disarm();
      }
    } catch (error) {
      const workflowError =
        error instanceof WorkflowError
          ? error
          : new WorkflowError(
              error instanceof Error ? error.message : String(error),
              WorkflowErrorCode.WORKFLOW_ABORTED,
              { recoverable: true },
            );

      const usageLimitPaused =
        !managed.controller.signal.aborted && workflowError.code === WorkflowErrorCode.PROVIDER_USAGE_LIMIT;
      if (managed.controller.signal.aborted) {
        // Intentional abort (pause/stop/Esc) — preserve status set by pause()/stop()
        if (managed.status === "running") {
          managed.status = "aborted";
        }
      } else if (usageLimitPaused) {
        // Provider quota/usage limit: NOT a failure. Checkpoint the run as paused so
        // the persisted journal (completed agent results) is replayed by resume()
        // once the budget refills — instead of the user starting from scratch.
        managed.status = "paused";
      } else {
        managed.status = "failed";
      }
      managed.error = workflowError;
      if (usageLimitPaused) {
        this.emit("paused", {
          runId: managed.runId,
          reason: "usage_limit",
          error: workflowError,
          resetHint: workflowError.resetHint,
        });
      } else {
        this.emit("error", { runId: managed.runId, error: workflowError });
      }

      // Persist final state
      this.persistRun(managed);
      this.releaseRunLease(managed);

      throw workflowError;
    }
  }

  private releaseRunLease(managed: ManagedRun): void {
    if (!managed.lease) return;
    this.persistence.releaseRunLease(managed.lease);
    managed.lease = undefined;
  }

  private persistRun(managed: ManagedRun) {
    try {
      this.persistence.save({
        runId: managed.runId,
        workflowName: managed.snapshot.name,
        // Persist the real script + journal so the run can be resumed. Runs live
        // in workflow run storage — protect via directory permissions, not blanking.
        script: managed.script,
        args: managed.args,
        sessionId: this.sessionId,
        journal: managed.journal,
        status: managed.status,
        // Why a usage-limit pause happened, so the navigator / a future cold start
        // can show it and (eventually) re-arm resume after the budget refills.
        pauseReason:
          managed.status === "paused" && managed.error?.code === WorkflowErrorCode.PROVIDER_USAGE_LIMIT
            ? "usage_limit"
            : undefined,
        resetHint:
          managed.status === "paused" && managed.error?.code === WorkflowErrorCode.PROVIDER_USAGE_LIMIT
            ? managed.error.resetHint
            : undefined,
        phases: managed.snapshot.phases,
        currentPhase: managed.snapshot.currentPhase,
        agents: managed.snapshot.agents.map((a) => ({
          ...a,
          startedAt: managed.startedAt.toISOString(),
          endedAt: new Date().toISOString(),
        })),
        logs: managed.snapshot.logs,
        result: managed.result?.result,
        tokenUsage: managed.snapshot.tokenUsage
          ? {
              input: managed.snapshot.tokenUsage.input,
              output: managed.snapshot.tokenUsage.output,
              total: managed.snapshot.tokenUsage.total,
              cost: managed.snapshot.tokenUsage.cost,
              cacheRead: managed.snapshot.tokenUsage.cacheRead,
              cacheWrite: managed.snapshot.tokenUsage.cacheWrite,
            }
          : undefined,
        startedAt: managed.startedAt.toISOString(),
        updatedAt: new Date().toISOString(),
        completedAt: managed.status === "completed" ? new Date().toISOString() : undefined,
        durationMs: managed.result?.durationMs,
      });
    } catch (err) {
      // Persistence is best-effort: the run is still healthy in memory.
      // Log so an operator debugging state-loss has a lead, but never crash
      // the workflow over a disk-full situation.
      console.warn("[workflow-manager] Persist run failed:", err);
    }
  }

  /**
   * Pause a running workflow.
   */
  pause(runId: string): boolean {
    const managed = this.runs.get(runId);
    if (managed?.status !== "running") return false;

    managed.controller.abort();
    managed.status = "paused";
    this.emit("paused", { runId });
    this.persistRun(managed);
    this.releaseRunLease(managed);
    return true;
  }

  /**
   * Resume an interrupted run: replay journaled results for the unchanged prefix
   * and run the rest live. Returns false if there is nothing resumable.
   */
  async resume(runId: string): Promise<boolean> {
    // Guard: refuse to resume a run that is already running, or one that was
    // intentionally aborted (pause/stop/Esc). Paused and failed runs can restart.
    const active = this.runs.get(runId);
    if (active?.status === "running") return false;
    if (active?.status === "aborted") return false;

    const persisted = this.persistence.load(runId);
    if (!persisted?.script || persisted.status === "completed" || persisted.status === "aborted") return false;
    const lease = this.persistence.acquireRunLease(runId);
    if (!lease) return false;

    const controller = new AbortController();
    const managed: ManagedRun = {
      runId,
      status: "running",
      snapshot: {
        name: persisted.workflowName,
        phases: persisted.phases ?? [],
        logs: persisted.logs ?? [],
        agents: [],
        agentCount: 0,
        runningCount: 0,
        doneCount: 0,
        errorCount: 0,
      },
      controller,
      startedAt: new Date(),
      script: persisted.script,
      args: persisted.args,
      journal: persisted.journal ?? [],
      background: true,
      lease,
    };
    this.runs.set(runId, managed);

    // Persist the "running" transition immediately (mirrors startInBackground).
    // Without this, cached-replay (which journals nothing) leaves the persisted
    // state frozen at "paused" — so the UI icon never updates and a crash mid-replay
    // leaves disk/in-memory inconsistent. journal/agents stay as loaded; executeRun
    // will keep persisting as live agents journal.
    this.persistRun(managed);

    const resumeJournal = new Map((persisted.journal ?? []).map((e) => [e.index, e] as const));
    this.emit("resumed", { runId });
    // Run in the background; executeRun records status/errors on the managed run.
    void this.executeRun(managed, persisted.script, persisted.args, { resumeJournal }).catch(() => {});
    return true;
  }

  /**
   * Stop a running workflow.
   */
  stop(runId: string): boolean {
    const managed = this.runs.get(runId);
    if (!managed || (managed.status !== "running" && managed.status !== "paused")) return false;

    managed.controller.abort();
    managed.status = "aborted";
    this.emit("stopped", { runId });
    this.persistRun(managed);
    this.releaseRunLease(managed);
    return true;
  }

  /**
   * Get status of a specific run.
   */
  getRun(runId: string): ManagedRun | undefined {
    return this.runs.get(runId);
  }

  /**
   * List all runs (active + persisted).
   */
  /**
   * Runs for the navigator/task panel. Once bound to a session (setSessionId), only
   * that session's runs are returned — runs from other sessions stay on disk and
   * reappear when you switch back. Unbound (tests/legacy) returns everything.
   */
  listRuns(): PersistedRunState[] {
    const all = this.persistence.list();
    return this.sessionId ? all.filter((r) => r.sessionId === this.sessionId) : all;
  }

  /** All persisted runs regardless of session (used by cross-session recovery). */
  listAllRuns(): PersistedRunState[] {
    return this.persistence.list();
  }

  /**
   * Get snapshot of a run.
   */
  getSnapshot(runId: string): WorkflowSnapshot | null {
    return this.runs.get(runId)?.snapshot ?? null;
  }

  /**
   * Delete a persisted run.
   */
  deleteRun(runId: string): boolean {
    const managed = this.runs.get(runId);
    if (managed) this.releaseRunLease(managed);
    this.runs.delete(runId);
    return this.persistence.delete(runId);
  }

  /**
   * Get the persistence layer (for saving workflows).
   */
  getPersistence(): RunPersistence {
    return this.persistence;
  }
}

/**
 * Run-level stall watchdog: a resettable timer whose expiration rejects a promise,
 * so `executeRun` can `Promise.race` the workflow against it and guarantee it
 * settles even when a live `agent()` await hangs forever (dead subagent process).
 *
 * Lifecycle: create -> arm() -> reset() on every progress event -> on fire,
 * rejects with a recoverable RUN_STALL WorkflowError -> disarm() in a finally.
 * reset() is cheap and safe to call before arm() or after disarm() (no-op).
 * The timer is unref'd so it never keeps the process alive on its own.
 */
interface StallWatchdog {
  arm: () => void;
  reset: () => void;
  disarm: () => void;
  promise: () => Promise<never>;
}

function createStallWatchdog(timeoutMs: number | null, runId: string): StallWatchdog {
  // null/0 = disabled. Return a watchdog whose promise never resolves, so
  // Promise.race reduces to the work promise alone (zero overhead, no timer).
  if (!timeoutMs || timeoutMs <= 0) {
    return {
      arm: () => {},
      reset: () => {},
      disarm: () => {},
      // A promise that never settles; Promise.race ignores it if the work settles.
      promise: () => new Promise<never>(() => {}),
    };
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  let stallPromiseReject: ((err: WorkflowError) => void) | undefined;
  let stalled = false;

  const clear = () => {
    if (timer) {
      clearTimeout(timer);
      timer = undefined;
    }
  };

  return {
    arm() {
      this.reset();
    },
    reset() {
      if (stalled) return; // already fired; further progress is too late
      clear();
      timer = setTimeout(() => {
        stalled = true;
        if (stallPromiseReject) {
          stallPromiseReject(
            new WorkflowError(
              `workflow run "${runId}" stalled: no progress for ${timeoutMs}ms (a live agent likely hung — its subagent process may have died without rejecting)`,
              WorkflowErrorCode.RUN_STALL,
              { recoverable: true },
            ),
          );
        }
      }, timeoutMs);
      timer.unref?.();
    },
    disarm() {
      clear();
      // Leave stallPromiseReject in place; if already rejected, the race already
      // settled on it. If not, the never-called resolver just gets GC'd.
    },
    promise() {
      return new Promise<never>((_resolve, reject) => {
        stallPromiseReject = reject;
      });
    },
  };
}
