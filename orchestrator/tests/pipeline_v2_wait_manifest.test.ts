import { describe, expect, test } from "bun:test";
import {
  PipelineV2WaitManifestError,
  acceptPipelineV2WaitResponse,
  parsePipelineV2WaitRequest,
  preparePipelineV2WaitRequest,
  type AcceptedPipelineV2WaitResponse,
  type PreparedPipelineV2WaitRequest,
} from "../src/pipeline_v2_wait_manifest.ts";
import { canonicalJson } from "../src/canonical_json.ts";
import {
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunPipelineIdentity,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";

const hex = (char: string): string => char.repeat(64);

const REQUEST = {
  schema_version: 1,
  run_id: "run-1",
  wait_index: 1,
  transition_count: 2,
  state_id: "architect",
  reason: "stage_iteration_limit_exhausted",
  actions: [
    { id: "continue_stage", to: "coder" },
    { id: "revise_task", to: "architect" },
  ],
};

const RESPONSE = {
  schema_version: 1,
  run_id: "run-1",
  wait_index: 1,
  request_sha256: hex("7"),
  action_id: "continue_stage",
};

function prepareWith(overrides: Record<string, unknown>): PreparedPipelineV2WaitRequest {
  return preparePipelineV2WaitRequest({ ...REQUEST, ...overrides });
}

function expectMessage(cause: unknown, message: string): void {
  expect(cause).toBeInstanceOf(PipelineV2WaitManifestError);
  expect((cause as Error).message).toBe(message);
}

function expectDeepFrozen(value: unknown, path = "result"): void {
  if (Array.isArray(value)) {
    expect(Object.isFrozen(value), `array ${path} is frozen`).toBe(true);
    value.forEach((entry, index) => expectDeepFrozen(entry, `${path}[${index}]`));
    return;
  }
  if (value !== null && typeof value === "object") {
    expect(Object.isFrozen(value), `object ${path} is frozen`).toBe(true);
    for (const child of Object.values(value)) {
      expectDeepFrozen(child, `${path}.*`);
    }
  }
}

function collectKeys(value: unknown, into: Set<string>): void {
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectKeys(entry, into);
    }
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const [key, child] of Object.entries(value)) {
      into.add(key);
      collectKeys(child, into);
    }
  }
}

function domainDigest(domain: string, canonical: string): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(domain);
  hasher.update(canonical);
  return hasher.digest("hex");
}

describe("pipeline v2 wait request manifest", () => {
  test("exact positive request shape with canonical form and domain-bound digest", () => {
    const prepared = preparePipelineV2WaitRequest(REQUEST);
    const canonical = canonicalJson(REQUEST);
    expect(prepared.canonical_json).toBe(canonical);
    expect(canonical).toBe(
      '{"actions":[{"id":"continue_stage","to":"coder"},{"id":"revise_task","to":"architect"}],"reason":"stage_iteration_limit_exhausted","run_id":"run-1","schema_version":1,"state_id":"architect","transition_count":2,"wait_index":1}',
    );
    expect(prepared.sha256).toBe(domainDigest("pipeline-v2-wait-request\0", canonical));
    expect(Object.keys(prepared.manifest)).toEqual([
      "schema_version",
      "run_id",
      "wait_index",
      "transition_count",
      "state_id",
      "reason",
      "actions",
    ]);
    expect(Object.keys(prepared)).toEqual(["manifest", "canonical_json", "sha256"]);
    expectDeepFrozen(prepared);
  });

  test("parse runs the same chain: identical snapshot from raw JSON with scrambled key order", () => {
    const scrambled =
      '{"actions":[{"to":"coder","id":"continue_stage"},{"to":"architect","id":"revise_task"}],"wait_index":1,"state_id":"architect","reason":"stage_iteration_limit_exhausted","transition_count":2,"run_id":"run-1","schema_version":1}';
    const parsed = parsePipelineV2WaitRequest(scrambled);
    const prepared = preparePipelineV2WaitRequest(REQUEST);
    expect(parsed).toEqual(prepared);
    expect(parsed.sha256).toBe(prepared.sha256);
    expect(parsed.canonical_json).toBe(prepared.canonical_json);
    // deterministic repeat
    expect(parsePipelineV2WaitRequest(scrambled)).toEqual(parsed);
    expect(preparePipelineV2WaitRequest(REQUEST)).toEqual(prepared);
  });

  test("JSON mapping key order never changes the canonical form or the digest", () => {
    const orderA = JSON.stringify(REQUEST);
    const orderB = JSON.stringify({
      actions: REQUEST.actions,
      reason: REQUEST.reason,
      run_id: REQUEST.run_id,
      schema_version: REQUEST.schema_version,
      state_id: REQUEST.state_id,
      transition_count: REQUEST.transition_count,
      wait_index: REQUEST.wait_index,
    });
    expect(orderA).not.toBe(orderB);
    const a = parsePipelineV2WaitRequest(orderA);
    const b = parsePipelineV2WaitRequest(orderB);
    expect(a.canonical_json).toBe(b.canonical_json);
    expect(a.sha256).toBe(b.sha256);
  });

  test("action array order changes the digest; every semantic field change changes the digest", () => {
    const base = preparePipelineV2WaitRequest(REQUEST);
    const singleAction = prepareWith({
      actions: [{ id: "continue_stage", to: "coder" }],
    });
    expect(singleAction.sha256).not.toBe(base.sha256);
    const reordered = prepareWith({
      actions: [
        { id: "revise_task", to: "architect" },
        { id: "continue_stage", to: "coder" },
      ],
    });
    expect(reordered.sha256).not.toBe(base.sha256);
    expect(reordered.sha256).not.toBe(singleAction.sha256);
    for (const [field, override] of [
      ["run_id", { run_id: "run-2" }],
      ["wait_index", { wait_index: 2 }],
      ["transition_count", { transition_count: 3 }],
      ["state_id", { state_id: "coder" }],
      ["reason", { reason: "major_issue_reported" }],
      ["action id", { actions: [{ id: "continue_stage_renamed", to: "coder" }] }],
      ["action target", { actions: [{ id: "continue_stage", to: "architect" }] }],
    ] as const) {
      const changed = prepareWith(override as Record<string, unknown>);
      expect(changed.sha256, `${field} must change the digest`).not.toBe(base.sha256);
    }
  });

  test("request and response digest domains differ from each other and from every existing domain", () => {
    const prepared = preparePipelineV2WaitRequest(REQUEST);
    const canonical = prepared.canonical_json;
    const responseDomainDigest = domainDigest("pipeline-v2-wait-response\0", canonical);
    expect(prepared.sha256).not.toBe(responseDomainDigest);
    const existingDomains = [
      "pipeline-v2-execution-snapshot\0",
      "pipeline-v2-input\0",
      "pipeline-v2-output\0",
      "pipeline-v2-run-output\0",
      "pipeline-v2-decision-input\0",
    ];
    for (const domain of existingDomains) {
      expect("pipeline-v2-wait-request\0").not.toBe(domain);
      expect("pipeline-v2-wait-response\0").not.toBe(domain);
    }
  });

  test("duplicate, empty and malformed action lists are rejected", () => {
    const cases: [unknown, string][] = [
      [{ ...REQUEST, actions: [] }, "the wait request manifest.actions must not be empty"],
      [{ ...REQUEST, actions: "continue_stage" }, "the wait request manifest.actions must be an array"],
      [
        {
          ...REQUEST,
          actions: [
            { id: "continue_stage", to: "coder" },
            { id: "continue_stage", to: "architect" },
          ],
        },
        "the wait request manifest declares a duplicate action id at position 1",
      ],
    ];
    for (const [value, message] of cases) {
      let caught: unknown = null;
      try {
        preparePipelineV2WaitRequest(value);
      } catch (cause) {
        caught = cause;
      }
      expectMessage(caught, message);
    }
  });

  test("unknown and missing fields are rejected at every level without naming user keys", () => {
    const secretCanary = "SECRET-BEARER-dht_deadbeef";
    const cases: [unknown, string][] = [
      [{ ...REQUEST, evidence: "acceptance test 7 fails" }, "the wait request manifest has unknown fields"],
      [{ ...REQUEST, [secretCanary]: "x" }, "the wait request manifest has unknown fields"],
      [
        {
          schema_version: 1,
          run_id: "run-1",
          wait_index: 1,
          transition_count: 2,
          state_id: "architect",
          actions: REQUEST.actions,
        },
        'the wait request manifest is missing required field "reason"',
      ],
      [
        {
          run_id: "run-1",
          wait_index: 1,
          transition_count: 2,
          state_id: "architect",
          reason: "stage_iteration_limit_exhausted",
          actions: REQUEST.actions,
        },
        'the wait request manifest is missing required field "schema_version"',
      ],
      [{ ...REQUEST, actions: [{ id: "continue_stage", to: "coder", target: "coder" }] }, "the wait request manifest action at position 0 has unknown fields"],
      [{ ...REQUEST, actions: [{ id: "continue_stage" }] }, 'the wait request manifest action at position 0 is missing required field "to"'],
      [{ ...REQUEST, actions: [{ to: "coder" }] }, 'the wait request manifest action at position 0 is missing required field "id"'],
      [null, "the wait request manifest is not a JSON object"],
      [["x"], "the wait request manifest is not a JSON object"],
      ["request", "the wait request manifest is not a JSON object"],
      [1, "the wait request manifest is not a JSON object"],
    ];
    for (const [value, message] of cases) {
      let caught: unknown = null;
      try {
        preparePipelineV2WaitRequest(value);
      } catch (cause) {
        caught = cause;
      }
      expectMessage(caught, message);
      expect((caught as Error).message).not.toContain(secretCanary);
    }
  });

  test("safe-id grammar and integer boundaries are enforced value-free", () => {
    const unsafeIds = ["", "../bad", "a..b", "/coder", "x y", "x\ny", "x".repeat(129), 1, null, {}];
    for (const id of unsafeIds) {
      let caught: unknown = null;
      try {
        preparePipelineV2WaitRequest({ ...REQUEST, run_id: id });
      } catch (cause) {
        caught = cause;
      }
      expectMessage(caught, "the wait request manifest run_id must be a safe non-empty identifier");
    }
    // the maximum-length safe id (128 characters) is accepted
    expect(prepareWith({ run_id: "x".repeat(128) }).manifest.run_id).toBe("x".repeat(128));
    for (const [field, value] of [
      ["wait_index", 0],
      ["wait_index", -1],
      ["wait_index", 1.5],
      ["wait_index", "1"],
      ["transition_count", -1],
      ["transition_count", 1.5],
      ["transition_count", null],
    ] as const) {
      let caught: unknown = null;
      try {
        preparePipelineV2WaitRequest({ ...REQUEST, [field]: value });
      } catch (cause) {
        caught = cause;
      }
      expectMessage(caught, `the wait request manifest ${field} must be a ${field === "wait_index" ? "positive" : "non-negative"} safe integer`);
    }
    // boundary acceptances
    expect(
      prepareWith({ wait_index: Number.MAX_SAFE_INTEGER }).manifest.wait_index,
    ).toBe(Number.MAX_SAFE_INTEGER);
    expect(prepareWith({ transition_count: 0 }).manifest.transition_count).toBe(0);
  });

  test("the request loader never includes raw JSON in diagnostics", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    let caught: unknown = null;
    try {
      parsePipelineV2WaitRequest(`{"run_id": "${canary}", "schema_version":1, "wait_i`);
    } catch (cause) {
      caught = cause;
    }
    expectMessage(caught, "the wait request document is not valid JSON");
    expect((caught as Error).message).not.toContain(canary);
    expect((caught as Error).message.toLowerCase()).not.toContain("position");
    expect((caught as Error).message.toLowerCase()).not.toContain("unexpected");
  });

  test("the result snapshot is independent of later input mutations", () => {
    const value: Record<string, unknown> = {
      ...REQUEST,
      actions: [
        { id: "continue_stage", to: "coder" },
        { id: "revise_task", to: "architect" },
      ],
    };
    const prepared = preparePipelineV2WaitRequest(value);
    value.reason = "mutated_after_prepare";
    (value.actions as { id: string; to: string }[]).push({ id: "sneak", to: "coder" });
    (value.actions as { id: string; to: string }[])[0]!.id = "mutated";
    expect(prepared.manifest.reason).toBe("stage_iteration_limit_exhausted");
    expect(prepared.manifest.actions).toEqual([
      { id: "continue_stage", to: "coder" },
      { id: "revise_task", to: "architect" },
    ]);
    expect(prepared.canonical_json).toBe(canonicalJson(REQUEST));
    expectDeepFrozen(prepared);
  });
});

describe("pipeline v2 wait response manifest", () => {
  function prepareRequest(): PreparedPipelineV2WaitRequest {
    return preparePipelineV2WaitRequest(REQUEST);
  }

  function acceptWith(
    request: PreparedPipelineV2WaitRequest,
    overrides: Record<string, unknown> = {},
    raw = JSON.stringify({ ...RESPONSE, request_sha256: request.sha256, ...overrides }),
  ): AcceptedPipelineV2WaitResponse {
    return acceptPipelineV2WaitResponse(request, raw);
  }

  test("exact positive response shape: digest bound, action_to taken only from the request", () => {
    const request = prepareRequest();
    const accepted = acceptWith(request);
    expect(accepted.manifest).toEqual({
      schema_version: 1,
      run_id: "run-1",
      wait_index: 1,
      request_sha256: request.sha256,
      action_id: "continue_stage",
    });
    expect(Object.keys(accepted.manifest)).toEqual([
      "schema_version",
      "run_id",
      "wait_index",
      "request_sha256",
      "action_id",
    ]);
    expect(accepted.action_to).toBe("coder");
    expect(accepted.canonical_json).toBe(canonicalJson(accepted.manifest));
    expect(accepted.sha256).toBe(domainDigest("pipeline-v2-wait-response\0", accepted.canonical_json));
    expectDeepFrozen(accepted);
    // the same response against another action of the same request picks
    // that action's own target
    const revise = acceptWith(request, { action_id: "revise_task" });
    expect(revise.action_to).toBe("architect");
  });

  test("a response is accepted only for the exact matching run, wait and request digest", () => {
    const request = prepareRequest();
    const cases: [Record<string, unknown>, string][] = [
      [{ run_id: "run-2" }, "the wait response names another run"],
      [{ wait_index: 2 }, "the wait response names another wait"],
      [{ request_sha256: hex("9") }, "the wait response carries a different request digest"],
      [{ action_id: "sneak" }, "the wait response names an action the request does not declare"],
    ];
    for (const [override, message] of cases) {
      let caught: unknown = null;
      try {
        acceptWith(request, override);
      } catch (cause) {
        caught = cause;
      }
      expectMessage(caught, message);
    }
  });

  test("the response cannot pass a target and every unknown field is rejected value-free", () => {
    const request = prepareRequest();
    const secretCanary = "SECRET-BEARER-dht_deadbeef";
    const cases: [Record<string, unknown>, string][] = [
      [{ to: "sneaky_target" }, "the wait response manifest has unknown fields"],
      [{ target: "sneaky_target" }, "the wait response manifest has unknown fields"],
      [{ action_to: "architect" }, "the wait response manifest has unknown fields"],
      [{ comment: "please fix the parser" }, "the wait response manifest has unknown fields"],
      [{ [secretCanary]: "value" }, "the wait response manifest has unknown fields"],
      [{ TASK: "rewrite the parser" }, "the wait response manifest has unknown fields"],
    ];
    for (const [override, message] of cases) {
      let caught: unknown = null;
      try {
        acceptWith(request, override);
      } catch (cause) {
        caught = cause;
      }
      expectMessage(caught, message);
      expect((caught as Error).message).not.toContain(secretCanary);
      expect((caught as Error).message).not.toContain("sneaky_target");
      expect((caught as Error).message).not.toContain("please fix the parser");
      expect((caught as Error).message).not.toContain("rewrite the parser");
    }
    for (const [override, message] of [
      [{ schema_version: 2 }, "the wait response manifest.schema_version must be 1"],
      [{ wait_index: 0 }, "the wait response manifest wait_index must be a positive safe integer"],
      [{ action_id: "../bad" }, "the wait response manifest action_id must be a safe non-empty identifier"],
      [{ run_id: "" }, "the wait response manifest run_id must be a safe non-empty identifier"],
      [{ request_sha256: "ABCDEF" }, "the wait response manifest request_sha256 must be a lowercase hex SHA-256 digest"],
      [{ request_sha256: "a".repeat(63) }, "the wait response manifest request_sha256 must be a lowercase hex SHA-256 digest"],
      [{ request_sha256: "a".repeat(65) }, "the wait response manifest request_sha256 must be a lowercase hex SHA-256 digest"],
    ] as const) {
      let caught: unknown = null;
      try {
        acceptWith(request, override);
      } catch (cause) {
        caught = cause;
      }
      expectMessage(caught, message);
    }
    let nullCaught: unknown = null;
    try {
      acceptPipelineV2WaitResponse(request, "null");
    } catch (cause) {
      nullCaught = cause;
    }
    expectMessage(nullCaught, "the wait response manifest is not a JSON object");
  });

  test("malformed response JSON produces a content-free error and canaries stay hidden", () => {
    const request = prepareRequest();
    const canary = "SECRET-BEARER-dht_deadbeef";
    let caught: unknown = null;
    try {
      acceptPipelineV2WaitResponse(request, `{"action_id": "${canary}", "schema_`);
    } catch (cause) {
      caught = cause;
    }
    expectMessage(caught, "the wait response document is not valid JSON");
    expect((caught as Error).message).not.toContain(canary);
    expect((caught as Error).message.toLowerCase()).not.toContain("position");
    expect((caught as Error).message.toLowerCase()).not.toContain("unexpected");
  });

  test("the response result is independent of later input mutations and is deep-frozen", () => {
    const request = prepareRequest();
    const raw = JSON.stringify({ ...RESPONSE, request_sha256: request.sha256 });
    const accepted = acceptPipelineV2WaitResponse(request, raw);
    expectDeepFrozen(accepted);
    expectDeepFrozen(request);
  });
});

describe("pipeline v2 wait manifest provenance boundary", () => {
  const raw = JSON.stringify(RESPONSE);

  test("only the exact prepared request is accepted", () => {
    const request = preparePipelineV2WaitRequest(REQUEST);
    const lookAlike: Record<string, unknown> = {
      manifest: JSON.parse(JSON.stringify(request.manifest)),
      canonical_json: request.canonical_json,
      sha256: request.sha256,
    };
    const cases: unknown[] = [
      null,
      undefined,
      "request",
      7,
      [],
      lookAlike,
      { ...request },
      structuredClone(request),
    ];
    for (const forged of cases) {
      let caught: unknown = null;
      try {
        acceptPipelineV2WaitResponse(forged as PreparedPipelineV2WaitRequest, raw);
      } catch (cause) {
        caught = cause;
      }
      expectMessage(
        caught,
        "acceptPipelineV2WaitResponse requires the exact prepared wait request returned by preparePipelineV2WaitRequest or parsePipelineV2WaitRequest",
      );
    }
  });

  test("a Proxy around the prepared request is rejected before any getter runs", () => {
    const request = preparePipelineV2WaitRequest(REQUEST);
    let getterHits = 0;
    const proxy = new Proxy(request, {
      get(target, property, receiver) {
        getterHits += 1;
        return Reflect.get(target, property, receiver);
      },
      has(target, property) {
        getterHits += 1;
        return Reflect.has(target, property);
      },
    });
    let caught: unknown = null;
    try {
      acceptPipelineV2WaitResponse(proxy as unknown as PreparedPipelineV2WaitRequest, raw);
    } catch (cause) {
      caught = cause;
    }
    expectMessage(
      caught,
      "acceptPipelineV2WaitResponse requires the exact prepared wait request returned by preparePipelineV2WaitRequest or parsePipelineV2WaitRequest",
    );
    expect(getterHits).toBe(0);
  });

  test("a prepared request keeps working after an unrelated prepare call (no registry cross-talk)", () => {
    const first = preparePipelineV2WaitRequest(REQUEST);
    preparePipelineV2WaitRequest({ ...REQUEST, run_id: "run-2" });
    const accepted = acceptPipelineV2WaitResponse(first, JSON.stringify({ ...RESPONSE, request_sha256: first.sha256 }));
    expect(accepted.action_to).toBe("coder");
  });
});

describe("pipeline v2 wait manifests in durable state v6 (pure round-trip proof)", () => {
  const IDENTITY: PipelineV2RunPipelineIdentity = {
    schema_version: 2,
    bundle_root: "/opt/orchestrator/pipelines/v2",
    execution_snapshot_sha256: hex("a"),
    entry_state: "architect",
    max_transitions: 6,
  };

  const TICKS = Array.from({ length: 8 }, (_, t) => new Date(Date.UTC(2026, 0, 1, 0, 0, t + 1)));

  function reduce(state: PipelineV2RunState | null, command: PipelineV2RunCommand, tick: number): PipelineV2RunState {
    const next = reducePipelineV2RunCommand(state, command, TICKS[tick] as Date);
    return next;
  }

  test("prepared request and accepted response drive the durable wait journal end to end", () => {
    // the entry wait: the manifest's transition_count matches the clean
    // boundary the run actually waits at
    const requestRaw = JSON.stringify({ ...REQUEST, transition_count: 0 });
    const prepared = parsePipelineV2WaitRequest(requestRaw);

    let state = reduce(null, { kind: "create_run", runId: "run-1", pipeline: IDENTITY, inputs: [] }, 0);
    state = reduce(state, {
      kind: "run_waiting",
      stateId: prepared.manifest.state_id,
      reason: prepared.manifest.reason,
      requestSha256: prepared.sha256,
      actions: prepared.manifest.actions,
    }, 1);

    // 1+2: the request digest fits run_waiting.requestSha256 without any
    // transformation, and the manifest fields match the durable wait record
    expect(state.waits).toHaveLength(1);
    expect(state.waits[0]).toEqual({
      index: prepared.manifest.wait_index,
      transition_count: prepared.manifest.transition_count,
      state_id: prepared.manifest.state_id,
      reason: prepared.manifest.reason,
      request_sha256: prepared.sha256,
      actions: prepared.manifest.actions,
    });
    expect(state.status).toBe("waiting");
    expect(state.phase).toBe("waiting");
    const loadedWaiting = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
    expect(loadedWaiting).toEqual(state);

    // 3+4: the response digest fits wait_response_recorded.responseSha256;
    // run id, wait index and request digest come from the manifests and the
    // routing target comes only from the request's declared action
    const responseRaw = JSON.stringify({
      schema_version: 1,
      run_id: prepared.manifest.run_id,
      wait_index: prepared.manifest.wait_index,
      request_sha256: prepared.sha256,
      action_id: "continue_stage",
    });
    const accepted = acceptPipelineV2WaitResponse(prepared, responseRaw);
    expect(accepted.action_to).toBe("coder");
    expect(accepted.manifest.request_sha256).toBe(prepared.sha256);

    state = reduce(state, {
      kind: "wait_response_recorded",
      waitIndex: accepted.manifest.wait_index,
      expectedRequestSha256: accepted.manifest.request_sha256,
      actionId: accepted.manifest.action_id,
      responseSha256: accepted.sha256,
    }, 2);

    expect(state.status).toBe("active");
    expect(state.phase).toBe("running");
    expect(state.cursor).toEqual({
      current_state: accepted.action_to,
      transition_count: prepared.manifest.transition_count,
    });
    expect(state.waits[0]?.response).toEqual({
      action_id: accepted.manifest.action_id,
      response_sha256: accepted.sha256,
    });
    // 5: the reducer-produced state passes the serialize -> loader round-trip
    const loaded = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
    expect(loaded).toEqual(state);
    expectDeepFrozen(loaded);

    // ordinary execution rules resume at the response target
    state = reduce(state, { kind: "start_agent_execution", stateId: "coder", profile: "coder" }, 3);
    expect(state.executions[0]).toMatchObject({ index: 1, state_id: "coder" });
    const finalLoaded = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
    expect(finalLoaded).toEqual(state);
  });
});

describe("pipeline v2 wait manifest structural scan", () => {
  test("no paths, timestamps, env, credentials, profile config, task/plan bodies, facts or payloads", () => {
    const prepared = parsePipelineV2WaitRequest(JSON.stringify(REQUEST));
    const accepted = acceptPipelineV2WaitResponse(
      prepared,
      JSON.stringify({ ...RESPONSE, request_sha256: prepared.sha256, action_id: "revise_task" }),
    );
    const text = `${JSON.stringify(prepared)}\n${JSON.stringify(accepted)}`;
    const canaries = [
      "/opt/orchestrator",
      "/home/michael",
      "/var/lib/orchestrator",
      "2026-01-01T00:00:00",
      "OPENCODE_CONFIG_CONTENT",
      "dhc_0392",
      "bearer_token",
      "LLM_KEY",
      "coder_stronger",
      "reviewer_alternative",
      "TASK.md revision 2",
      "PLAN: stage 3 of 5",
      '{"intent":"continue_stage","additional_iterations":2}',
      "unmet acceptance criteria",
      "stage_iteration=4_of_4",
      "endpoint",
      "apiKey",
    ];
    for (const canary of canaries) {
      expect(text).not.toContain(canary);
    }
    const keys = new Set<string>();
    collectKeys(prepared, keys);
    collectKeys(accepted, keys);
    for (const banned of [
      "evidence",
      "manifest_body",
      "request",
      "user_response",
      "task",
      "plan",
      "profile_bindings",
      "facts",
      "endpoint",
      "token",
      "path",
      "timestamp",
      "created_at",
      "payload",
      "comment",
    ]) {
      expect(keys.has(banned), `the manifest substrate must not carry a ${banned} field`).toBe(false);
    }
  });
});
