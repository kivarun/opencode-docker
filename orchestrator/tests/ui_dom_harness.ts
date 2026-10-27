/**
 * Test harness that drives the REAL browser UI module (`orchestrator/ui/app.js`)
 * end to end: a minimal fake DOM plus controllable globals are installed
 * before a cache-busted dynamic import, so the module's own `boot()` runs
 * and every production call chain (event listeners → adapter functions →
 * transport → `requestApiJson`) is exercised exactly as the browser runs
 * it. Nothing about the module is re-implemented or stubbed: only the
 * browser environment (document, fetch, timers) is faked.
 *
 * The harness is test-only infrastructure; it never imports production
 * orchestration modules.
 */
import type { UiApiModel } from "../ui/app.js";

export interface FakeListener {
  readonly type: string;
  readonly handler: (event?: unknown) => void;
}

export class FakeElement {
  readonly tagName: string;
  readonly id: string;
  children: FakeElement[] = [];
  parent: FakeElement | null = null;
  textContent = "";
  value = "";
  disabled = false;
  hidden = false;
  checked = false;
  className = "";
  htmlFor = "";
  type = "";
  autocomplete = "";
  spellcheck = false;
  private readonly listeners: FakeListener[] = [];

  constructor(tagName: string, id = "") {
    this.tagName = tagName.toUpperCase();
    this.id = id;
  }

  addEventListener(type: string, handler: (event?: unknown) => void): void {
    this.listeners.push({ type, handler });
  }

  dispatch(type: string): void {
    const event = { preventDefault: () => undefined };
    for (const listener of [...this.listeners]) {
      if (listener.type === type) {
        listener.handler(event);
      }
    }
  }

  click(): void {
    this.dispatch("click");
  }

  appendChild(child: FakeElement): FakeElement {
    child.parent = this;
    this.children.push(child);
    return child;
  }

  replaceChildren(): void {
    for (const child of this.children) {
      child.parent = null;
    }
    this.children = [];
  }

  remove(): void {
    if (this.parent !== null) {
      const index = this.parent.children.indexOf(this);
      if (index >= 0) {
        this.parent.children.splice(index, 1);
      }
      this.parent = null;
    }
  }

  /** Setting text content clears the children (real DOM semantics). */
  setText(text: string): void {
    this.textContent = text;
    this.replaceChildren();
  }

  matches(selector: string): boolean {
    if (selector.startsWith(".")) {
      return this.className.split(" ").includes(selector.slice(1));
    }
    return this.tagName === selector.toUpperCase();
  }

  querySelector(selector: string): FakeElement | null {
    for (const child of this.children) {
      if (child.matches(selector)) {
        return child;
      }
      const found = child.querySelector(selector);
      if (found !== null) {
        return found;
      }
    }
    return null;
  }

  querySelectorAll(selector: string): FakeElement[] {
    const found: FakeElement[] = [];
    for (const child of this.children) {
      if (child.matches(selector)) {
        found.push(child);
      }
      found.push(...child.querySelectorAll(selector));
    }
    return found;
  }
}

export interface UiHarnessClock {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
  /** Runs every due callback; returns how many fired. */
  advance(ms: number): number;
  hasPending(): boolean;
}

export interface UiHarness {
  /** The freshly imported module instance (one per harness). */
  readonly module: Record<string, unknown>;
  element(id: string): FakeElement;
  click(id: string): void;
  /** Clicks the first descendant button of `id` whose text contains `text`. */
  clickButton(id: string, text: string): void;
  submit(id: string): void;
  setVisibility(visible: boolean): void;
  setLiveToggle(checked: boolean): void;
  status(): string;
  error(): string;
  liveStateLine(): string;
  detailRunId(): string;
  detailStatus(): string;
  /** The `textContent` of every rendered run-controls child, joined by `|`. */
  controlsText(): string;
  /** The `Revision|Kind|References` triple of every rendered event row. */
  eventsRows(): string[];
  /** The lifecycle events aria-live line. */
  eventsStatus(): string;
  /** The timeline meta line (detail revision, events revision, cursor). */
  eventsMeta(): string;
  /** The hidden/disabled state of one timeline action button. */
  eventsButtonState(id: string): { hidden: boolean; disabled: boolean };
  /** The `Execution|State|Status|Bytes|Truncated` rows of the logs table. */
  logsRows(): string[];
  /** The worker-logs aria-live status line. */
  logsStatus(): string;
  /** The decoded content text of the logs frame. */
  logsContent(): string;
  /** The logs cursor meta line. */
  logsCursor(): string;
  /** The read-only status explanation of the selected execution. */
  logsHint(): string;
  /** The logs empty-hint line. */
  logsEmpty(): string;
  /** The hidden/disabled state of one logs action button. */
  logsButtonState(id: string): { hidden: boolean; disabled: boolean };
  /** The `Path|Change|Source relation|Baseline|Result` rows of the changes table. */
  changesRows(): string[];
  /** The project-changes aria-live status line. */
  changesStatus(): string;
  /** The changes meta line (snapshot revision, presentation, loaded/total). */
  changesMeta(): string;
  /** The changes summary counts line. */
  changesSummary(): string;
  /** The changes empty-hint line. */
  changesEmpty(): string;
  /** The hidden/disabled state of one changes action button. */
  changesButtonState(id: string): { hidden: boolean; disabled: boolean };
  /** The apply panel's aria-live status line. */
  applyStatus(): string;
  /** The apply panel's recovery-record line. */
  applyRecord(): string;
  /** The apply panel's operation meta line. */
  applyMeta(): string;
  /** The joined confirmation detail lines. */
  applyConfirmation(): string;
  /** Whether the confirmation panel is visible. */
  applyConfirmationVisible(): boolean;
  /** The hidden/disabled state of one apply action button. */
  applyButtonState(id: string): { hidden: boolean; disabled: boolean };
  readonly clock: UiHarnessClock | null;
  /** Clicks the Inspect button of the run row at `index` in the runs table. */
  clickRunInspect(index: number): void;
  /** The rendered run id cells of the runs table. */
  runRows(): string[];
  /** Removes the installed globals; the module instance becomes inert. */
  dispose(): void;
}

const UI_ELEMENT_IDS = [
  "refresh-projects",
  "refresh-runs",
  "load-more",
  "inspect-selected",
  "live-updates",
  "generate-id",
  "add-input",
  "new-run-form",
  "input-rows",
  "projects-body",
  "new-run-project",
  "runs-body",
  "run-detail",
  "detail-status",
  "detail-revision",
  "detail-boundary",
  "outputs-body",
  "outputs-table",
  "detail-run-id",
  "run-controls",
  "status",
  "error",
  "live-state",
  "new-run-id",
  "submit-run",
  "events-section",
  "events-meta",
  "events-status",
  "events-table",
  "events-body",
  "events-load-more",
  "events-reload",
  "events-retry",
  "worker-logs-section",
  "logs-load",
  "logs-empty",
  "logs-table",
  "logs-body",
  "logs-cursor",
  "logs-hint",
  "logs-content",
  "logs-status",
  "logs-next-chunk",
  "logs-retry",
  "project-changes-section",
  "changes-meta",
  "changes-status",
  "changes-summary",
  "changes-table",
  "changes-body",
  "changes-empty",
  "changes-reload",
  "changes-load-more",
  "content-review-panel",
  "content-review-meta",
  "content-review-status",
  "content-baseline-status",
  "content-baseline-view",
  "content-result-status",
  "content-result-view",
  "content-diff-status",
  "content-diff-view",
  "content-review-load-baseline",
  "content-review-load-result",
  "content-review-retry",
  "content-review-reload-changes",
  "content-review-close",
  "project-apply-section",
  "project-apply-status",
  "project-apply-record",
  "project-apply-meta",
  "project-apply-confirmation",
  "project-apply-confirmation-details",
  "project-apply-confirm",
  "project-apply-cancel",
  "project-apply-review",
  "project-apply-retry",
  "project-apply-check",
  "project-apply-continue",
] as const;

export interface UiHarnessOptions {
  readonly fetchImpl: (path: string, init?: RequestInit) => Promise<Response>;
  /** When omitted the harness uses the real timers. */
  readonly clock?: UiHarnessClock;
  /**
   * The browser storage the module's closed recovery-record owner
   * binds; when omitted a fresh in-memory store is installed (the
   * harness never touches a real browser storage).
   */
  readonly storage?: Map<string, string> | null;
}

export async function createUiHarness(options: UiHarnessOptions): Promise<UiHarness> {
  const byId = new Map<string, FakeElement>();
  const all: FakeElement[] = [];
  const element = (tag: string, id = ""): FakeElement => {
    const node = new FakeElement(tag, id);
    all.push(node);
    if (id !== "") {
      byId.set(id, node);
    }
    return node;
  };
  for (const id of UI_ELEMENT_IDS) {
    element("div", id);
  }

  let visibilityHandler: (() => void) | null = null;
  const documentShim = {
    createElement: (tag: string) => element(tag),
    getElementById: (id: string) => byId.get(id) ?? null,
    addEventListener: (_type: string, handler: (event?: unknown) => void) => {
      // the module registers exactly one visibilitychange listener at boot
      visibilityHandler = handler;
    },
    visibilityState: "visible",
    querySelectorAll: (selector: string) => {
      const found: FakeElement[] = [];
      for (const node of all) {
        if (node.matches(selector)) {
          found.push(node);
        }
        found.push(...node.querySelectorAll(selector));
      }
      return found;
    },
  };

  const previousDocument = (globalThis as Record<string, unknown>)["document"];
  const previousFetch = (globalThis as Record<string, unknown>)["fetch"];
  const previousSetTimeout = (globalThis as Record<string, unknown>)["setTimeout"];
  const previousClearTimeout = (globalThis as Record<string, unknown>)["clearTimeout"];
  const previousLocalStorage = (globalThis as Record<string, unknown>)["localStorage"];
  const storageMap = options.storage === undefined ? new Map<string, string>() : options.storage;
  const storageShim =
    storageMap === null
      ? undefined
      : {
          getItem: (key: string): string | null => (storageMap.has(key) ? (storageMap.get(key) as string) : null),
          setItem: (key: string, value: string): void => {
            storageMap.set(key, value);
          },
          removeItem: (key: string): void => {
            storageMap.delete(key);
          },
        };
  (globalThis as Record<string, unknown>)["document"] = documentShim;
  (globalThis as Record<string, unknown>)["fetch"] = options.fetchImpl;
  (globalThis as Record<string, unknown>)["localStorage"] = storageShim;
  // Real timers scheduled through the module-level adapter must never
  // outlive the harness: without a clock the shims pass through to the
  // real timers but track every handle so dispose() can cancel all
  // still-pending ones (a fired poll timer into a restored document
  // would otherwise surface inside a later test file).
  const trackedRealTimers = new Set<unknown>();
  const realSetTimeout = previousSetTimeout as ((callback: () => void, delayMs?: number) => unknown) | undefined;
  const realClearTimeout = previousClearTimeout as ((handle: unknown) => void) | undefined;
  (globalThis as Record<string, unknown>)["document"] = documentShim;
  (globalThis as Record<string, unknown>)["fetch"] = options.fetchImpl;
  if (options.clock !== undefined) {
    (globalThis as Record<string, unknown>)["setTimeout"] = options.clock.setTimeout;
    (globalThis as Record<string, unknown>)["clearTimeout"] = options.clock.clearTimeout;
  } else if (typeof realSetTimeout === "function" && typeof realClearTimeout === "function") {
    (globalThis as Record<string, unknown>)["setTimeout"] = (callback: () => void, delayMs?: number) => {
      const handle = realSetTimeout(callback, delayMs);
      trackedRealTimers.add(handle);
      return handle;
    };
    (globalThis as Record<string, unknown>)["clearTimeout"] = (handle: unknown) => {
      trackedRealTimers.delete(handle);
      realClearTimeout(handle);
    };
  }

  let imported: Record<string, unknown> | null = null;
  try {
    imported = (await import(`../ui/app.js?harness=${Date.now()}-${Math.random()}`)) as Record<string, unknown>;
  } finally {
    if (imported === null) {
      (globalThis as Record<string, unknown>)["document"] = previousDocument;
      (globalThis as Record<string, unknown>)["fetch"] = previousFetch;
      (globalThis as Record<string, unknown>)["setTimeout"] = previousSetTimeout;
      (globalThis as Record<string, unknown>)["clearTimeout"] = previousClearTimeout;
      (globalThis as Record<string, unknown>)["localStorage"] = previousLocalStorage;
    }
  }

  const clock = options.clock ?? null;
  let disposed = false;

  const harness: UiHarness = {
    module: imported as Record<string, unknown>,
    element: (id) => {
      const found = byId.get(id);
      if (found === undefined) {
        throw new Error(`harness element missing: ${id}`);
      }
      return found;
    },
    click: (id) => harness.element(id).click(),
    clickButton: (id, text) => {
      const host = harness.element(id);
      const button = host.querySelectorAll("button").find((node) => node.textContent.includes(text));
      if (button === undefined) {
        throw new Error(`harness button missing: ${text} under ${id}`);
      }
      button.click();
    },
    submit: (id) => harness.element(id).dispatch("submit"),
    clickRunInspect: (index) => {
      const row = harness.element("runs-body").children[index];
      if (row === undefined) {
        throw new Error(`harness run row missing: ${index}`);
      }
      const button = row.querySelectorAll("button").find((node) => node.textContent === "Inspect");
      if (button === undefined) {
        throw new Error("harness inspect button missing in run row");
      }
      button.click();
    },
    runRows: () => harness.element("runs-body").children.map((row) => row.children[0]?.textContent ?? ""),
    setVisibility: (visible) => {
      documentShim.visibilityState = visible ? "visible" : "hidden";
      visibilityHandler?.();
    },
    setLiveToggle: (checked) => {
      harness.element("live-updates").checked = checked;
      harness.element("live-updates").dispatch("change");
    },
    status: () => harness.element("status").textContent,
    error: () => harness.element("error").textContent,
    liveStateLine: () => harness.element("live-state").textContent,
    detailRunId: () => harness.element("detail-run-id").textContent,
    detailStatus: () => harness.element("detail-status").textContent,
    controlsText: () => harness.element("run-controls").children.map((node) => node.textContent).join("|"),
    eventsRows: () =>
      harness.element("events-body").children.map((row) => row.children.map((cell) => cell.textContent).join("|")),
    eventsStatus: () => harness.element("events-status").textContent,
    eventsMeta: () => harness.element("events-meta").textContent,
    eventsButtonState: (id) => {
      const button = harness.element(id);
      return { hidden: button.hidden, disabled: button.disabled };
    },
    logsRows: () =>
      harness.element("logs-body").children.map((row) => row.children.map((cell) => cell.textContent).join("|")),
    logsStatus: () => harness.element("logs-status").textContent,
    logsContent: () => harness.element("logs-content").textContent,
    logsCursor: () => harness.element("logs-cursor").textContent,
    logsHint: () => harness.element("logs-hint").textContent,
    logsEmpty: () => harness.element("logs-empty").textContent,
    logsButtonState: (id) => {
      const button = harness.element(id);
      return { hidden: button.hidden, disabled: button.disabled };
    },
    /** The `Path|Change|Source relation|Baseline|Result` rows of the changes table. */
    changesRows: () =>
      harness.element("changes-body").children.map((row) => row.children.map((cell) => cell.textContent).join("|")),
    /** The project-changes aria-live status line. */
    changesStatus: () => harness.element("changes-status").textContent,
    /** The changes meta line (snapshot revision, presentation, loaded/total). */
    changesMeta: () => harness.element("changes-meta").textContent,
    /** The changes summary counts line. */
    changesSummary: () => harness.element("changes-summary").textContent,
    /** The changes empty-hint line. */
    changesEmpty: () => harness.element("changes-empty").textContent,
    /** The hidden/disabled state of one changes action button. */
    changesButtonState: (id) => {
      const button = harness.element(id);
      return { hidden: button.hidden, disabled: button.disabled };
    },
    applyStatus: () => harness.element("project-apply-status").textContent,
    applyRecord: () => harness.element("project-apply-record").textContent,
    applyMeta: () => harness.element("project-apply-meta").textContent,
    applyConfirmation: () =>
      harness.element("project-apply-confirmation-details").children.map((node) => node.textContent).join("|"),
    applyConfirmationVisible: () => !harness.element("project-apply-confirmation").hidden,
    applyButtonState: (id) => {
      const button = harness.element(id);
      return { hidden: button.hidden, disabled: button.disabled };
    },
    clock,
    dispose: () => {
      if (disposed) {
        return;
      }
      disposed = true;
      for (const handle of trackedRealTimers) {
        try {
          realClearTimeout?.(handle);
        } catch {
          // best effort: the timer target is already gone
        }
      }
      trackedRealTimers.clear();
      (globalThis as Record<string, unknown>)["document"] = previousDocument;
      (globalThis as Record<string, unknown>)["fetch"] = previousFetch;
      (globalThis as Record<string, unknown>)["setTimeout"] = previousSetTimeout;
      (globalThis as Record<string, unknown>)["clearTimeout"] = previousClearTimeout;
      (globalThis as Record<string, unknown>)["localStorage"] = previousLocalStorage;
    },
  };

  return harness;
}

/** A JSON `Response` with the fixed API content type. */
export function jsonResponse(model: UiApiModel | Record<string, unknown>): Response {
  return new Response(JSON.stringify(model), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

/** A canonical run document fixture for the inspect endpoints. */
export function runDocument(
  runId: string,
  revision: number,
  status: string,
  boundary: Record<string, unknown>,
): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "run",
    run_id: runId,
    revision,
    status,
    boundary,
    outputs: [],
  };
}

export function runsPageDocument(runIds: readonly string[]): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "runs",
    runs: runIds.map((runId) => ({ run_id: runId, project_id: "project-a", project_available: true })),
    next_after: null,
  };
}

export function projectsDocument(): Record<string, unknown> {
  return {
    schema_version: 2,
    kind: "projects",
    projects: [{ id: "project-a", input_roots: [{ id: "tasks" }], project_apply: false }],
  };
}
