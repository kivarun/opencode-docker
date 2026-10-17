/**
 * First-wins ownership of the single active worker operation.
 *
 * The pipeline v2 runner owns one `WorkerSignalHub` per command invocation.
 * The Docker Helper runtime adapter registers the worker operation it is
 * about to start; a user signal delivered through the shared v1 signal
 * wiring is forwarded to that operation, and the operation's own deadline
 * claims the same first-wins slot. The hub answers whether a user signal
 * may still be recorded as a lifecycle abort — the exact contract
 * `SubprocessCliRunner.killActive` has always answered for a signalable
 * `docker-helper run` child process, now owned by the adapter-side
 * operation instead of a CLI subprocess.
 *
 * The hub owns no operation state beyond the active registration: the
 * operation id never passes through it, and a lost registration is simply
 * an operation that is no longer signalable.
 */

/** A terminal cause that claims the active worker operation. */
export type WorkerOperationCause = "user_signal" | "timeout";

/** Delivered to the operation when a terminal cause claims it. */
export interface WorkerOperationHandle {
  /**
   * Called at most once per operation: the first terminal cause wins.
   * `signal` is the concrete POSIX signal for `user_signal` and the
   * runner's deadline signal for `timeout`.
   */
  readonly abort: (cause: WorkerOperationCause, signal: "SIGINT" | "SIGTERM") => void;
}

interface ActiveOperation {
  readonly handle: WorkerOperationHandle;
  cause: WorkerOperationCause | null;
}

export class WorkerSignalHub {
  private active: ActiveOperation | null = null;

  /**
   * Registers the operation that is about to start. Exactly one operation
   * may be active at a time; a second registration while one is active is
   * a lifecycle violation and fails closed.
   */
  register(handle: WorkerOperationHandle): void {
    if (this.active !== null) {
      throw new Error("a worker operation is already active");
    }
    this.active = { handle, cause: null };
  }

  /** Removes the registration; late causes after this are ignored. */
  unregister(handle: WorkerOperationHandle): void {
    if (this.active !== null && this.active.handle === handle) {
      this.active = null;
    }
  }

  /**
   * Forwards a user signal to the active operation. Returns whether the
   * signal may still be recorded as a user abort: `true` when no
   * operation is active or the operation already ran to a terminal cause,
   * `false` only when the operation's own deadline claimed it first — the
   * signal is then neither delivered nor recorded, exactly like a signal
   * losing to the runner timeout in the CLI-mediated path.
   */
  forwardUserSignal(signal: "SIGINT" | "SIGTERM"): boolean {
    const active = this.active;
    if (active === null) {
      return true;
    }
    if (active.cause === "timeout") {
      // The operation's own deadline claimed it first: the signal is
      // neither delivered nor recorded, exactly like a signal losing to
      // the runner timeout in the CLI-mediated path.
      return false;
    }
    if (active.cause === "user_signal") {
      return true;
    }
    active.cause = "user_signal";
    active.handle.abort("user_signal", signal);
    return true;
  }

  /**
   * Claims the active operation for its own deadline. Returns `false`
   * when the operation is not the active one or a terminal cause already
   * claimed it (first-wins, no second cancel).
   */
  claimTimeout(handle: WorkerOperationHandle): boolean {
    const active = this.active;
    if (active === null || active.handle !== handle || active.cause !== null) {
      return false;
    }
    active.cause = "timeout";
    active.handle.abort("timeout", "SIGTERM");
    return true;
  }
}
