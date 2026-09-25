import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import * as freezeModule from "../src/pipeline_v2_freeze_internal.ts";
import { deepFreezeValue } from "../src/pipeline_v2_freeze_internal.ts";

const SRC = join(import.meta.dir, "..", "src");

function srcPath(name: string): string {
  return join(SRC, name);
}

describe("pipeline v2 neutral freeze internal module", () => {
  test("1. the runtime export surface is exactly one key", () => {
    expect(Object.keys(freezeModule).sort()).toEqual(["deepFreezeValue"]);
  });

  test("2. deepFreezeValue freezes plain objects and arrays deeply and returns the same value", () => {
    const value = {
      list: [1, { nested: "x" }, [2, 3]],
      record: { a: { b: [{ c: 1 }] } },
      scalar: 5,
      nil: null,
    };
    const returned = deepFreezeValue(value);
    expect(returned).toBe(value);
    expect(Object.isFrozen(value)).toBe(true);
    expect(Object.isFrozen(value.list)).toBe(true);
    expect(Object.isFrozen(value.list[1])).toBe(true);
    expect(Object.isFrozen(value.list[2])).toBe(true);
    expect(Object.isFrozen(value.record)).toBe(true);
    expect(Object.isFrozen(value.record.a)).toBe(true);
    expect(Object.isFrozen(value.record.a.b)).toBe(true);
    expect(Object.isFrozen(value.record.a.b[0])).toBe(true);
    expect(value.scalar).toBe(5);
    expect(value.nil).toBeNull();
    // frozen values cannot be mutated
    expect(() => {
      (value.record.a as Record<string, unknown>).b = [];
    }).toThrow(TypeError);
    // primitives and null pass through unchanged
    expect(deepFreezeValue(7)).toBe(7);
    expect(deepFreezeValue("x")).toBe("x");
    expect(deepFreezeValue(null)).toBeNull();
    expect(deepFreezeValue(undefined)).toBeUndefined();
    // frozen objects are accepted unchanged
    const frozen = Object.freeze({ a: 1 });
    expect(deepFreezeValue(frozen)).toBe(frozen);
  });

  test("3. deepFreezeValue is defined exactly once in the source tree", () => {
    const files = readdirSync(SRC).filter((name) => name.endsWith(".ts"));
    const defining: string[] = [];
    for (const name of files) {
      const source = readFileSync(srcPath(name), "utf8");
      if (/export function deepFreezeValue|function deepFreezeValue/.test(source)) {
        defining.push(name);
      }
    }
    expect(defining).toEqual(["pipeline_v2_freeze_internal.ts"]);
  });

  test("4. the neutral freeze module imports nothing at all", () => {
    const source = readFileSync(srcPath("pipeline_v2_freeze_internal.ts"), "utf8");
    const importTargets = [...source.matchAll(/from "([^"]+)"/g)].map((match) => match[1] ?? "");
    // pin exact allowed import targets: there are none
    expect(importTargets).toEqual([]);
    expect([...source.matchAll(/from "node:([^"]+)"/g)]).toEqual([]);
    expect(source.includes("require(")).toBe(false);
    expect(source.includes("Bun.")).toBe(false);
  });

  test("5. the filesystem substrate no longer owns or exports the helper", () => {
    const source = readFileSync(srcPath("pipeline_v2_immutable_document_store_internal.ts"), "utf8");
    expect(source.includes("function deepFreezeValue")).toBe(false);
    expect(source).toContain('from "./pipeline_v2_freeze_internal.ts"');
  });

  test("6. the pure candidate and controller layers gained no filesystem dependency for freeze", () => {
    for (const name of [
      "pipeline_v2_run_plan_candidate_internal.ts",
      "pipeline_v2_stage_iteration_controller.ts",
    ]) {
      const source = readFileSync(srcPath(name), "utf8");
      expect(source).toContain('from "./pipeline_v2_freeze_internal.ts"');
      // the only freeze owner; no transitively-loaded filesystem substrate
      expect(source.includes('from "./pipeline_v2_immutable_document_store_internal.ts"')).toBe(false);
      expect([...source.matchAll(/from "node:([^"]+)"/g)]).toEqual([]);
    }
    // the run-plan controller's only node import is its pre-existing
    // run-root binding (node:path basename); the freeze migration added
    // no filesystem dependency to it
    const controllerSource = readFileSync(srcPath("pipeline_v2_run_plan_controller_internal.ts"), "utf8");
    expect(controllerSource).toContain('from "./pipeline_v2_freeze_internal.ts"');
    expect(controllerSource.includes('from "./pipeline_v2_immutable_document_store_internal.ts"')).toBe(false);
    expect([...controllerSource.matchAll(/from "(node:[^"]+)"/g)].map((match) => match[1])).toEqual(["node:path"]);
  });

  test("7. the filesystem stores keep using the same neutral helper", () => {
    for (const name of ["pipeline_v2_wait_store_internal.ts", "pipeline_v2_run_plan_store_internal.ts"]) {
      const source = readFileSync(srcPath(name), "utf8");
      expect(source).toContain('import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";');
      expect(source.includes("function deepFreezeValue")).toBe(false);
    }
  });
});
