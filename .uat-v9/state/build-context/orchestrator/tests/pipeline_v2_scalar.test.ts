import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import * as scalar from "../src/pipeline_v2_scalar.ts";
import {
  isLowercaseSha256,
  isNonNegativeSafeInteger,
  isPipelineV2SafeId,
  isPositiveSafeInteger,
} from "../src/pipeline_v2_scalar.ts";

const SCALAR_SOURCE = readFileSync(join(import.meta.dir, "../src/pipeline_v2_scalar.ts"), "utf8");
const STATE_SOURCE = readFileSync(join(import.meta.dir, "../src/pipeline_v2_state.ts"), "utf8");
const MANIFEST_SOURCE = readFileSync(
  join(import.meta.dir, "../src/pipeline_v2_wait_manifest.ts"),
  "utf8",
);

describe("pipeline v2 scalar predicates", () => {
  describe("isPipelineV2SafeId", () => {
    test("accepts minimal, ordinary and exactly-128-character ids", () => {
      expect(isPipelineV2SafeId("a")).toBe(true);
      expect(isPipelineV2SafeId("run-01_x.y")).toBe(true);
      expect(isPipelineV2SafeId("Z09")).toBe(true);
      expect(isPipelineV2SafeId("a".repeat(128))).toBe(true);
      expect(isPipelineV2SafeId("0".repeat(128))).toBe(true);
    });

    test("rejects empty, 129 characters, traversal, separators, whitespace and unicode", () => {
      expect(isPipelineV2SafeId("")).toBe(false);
      expect(isPipelineV2SafeId("a".repeat(129))).toBe(false);
      expect(isPipelineV2SafeId("a..b")).toBe(false);
      expect(isPipelineV2SafeId("..")).toBe(false);
      expect(isPipelineV2SafeId("a/../b")).toBe(false);
      expect(isPipelineV2SafeId("a/b")).toBe(false);
      expect(isPipelineV2SafeId("a\\b")).toBe(false);
      expect(isPipelineV2SafeId("a b")).toBe(false);
      expect(isPipelineV2SafeId(" a")).toBe(false);
      expect(isPipelineV2SafeId("a ")).toBe(false);
      expect(isPipelineV2SafeId("a\tb")).toBe(false);
      expect(isPipelineV2SafeId("a\nb")).toBe(false);
      expect(isPipelineV2SafeId("-a")).toBe(false);
      expect(isPipelineV2SafeId(".a")).toBe(false);
      expect(isPipelineV2SafeId("caf\u00e9")).toBe(false);
      expect(isPipelineV2SafeId("\u00fc")).toBe(false);
      expect(isPipelineV2SafeId("run\u2013id")).toBe(false);
    });

    test("rejects non-string values including canary-shaped objects", () => {
      expect(isPipelineV2SafeId(null)).toBe(false);
      expect(isPipelineV2SafeId(undefined)).toBe(false);
      expect(isPipelineV2SafeId(1)).toBe(false);
      expect(isPipelineV2SafeId(["run"])).toBe(false);
      expect(isPipelineV2SafeId({ toString: () => "run" })).toBe(false);
      expect(isPipelineV2SafeId(true)).toBe(false);
    });
  });

  describe("isLowercaseSha256", () => {
    test("accepts a correct lowercase 64-hex digest", () => {
      expect(isLowercaseSha256("0".repeat(64))).toBe(true);
      expect(isLowercaseSha256("a".repeat(64))).toBe(true);
      expect(isLowercaseSha256("f".repeat(64))).toBe(true);
      expect(isLowercaseSha256("abc123def4567890".repeat(4))).toBe(true);
    });

    test("rejects uppercase, wrong lengths, non-hex and non-strings", () => {
      expect(isLowercaseSha256("A".repeat(64))).toBe(false);
      expect(isLowercaseSha256("0".repeat(63))).toBe(false);
      expect(isLowercaseSha256("0".repeat(65))).toBe(false);
      expect(isLowercaseSha256("g".repeat(64))).toBe(false);
      expect(isLowercaseSha256("0".repeat(32))).toBe(false);
      expect(isLowercaseSha256("")).toBe(false);
      expect(isLowercaseSha256("0x" + "0".repeat(62))).toBe(false);
      expect(isLowercaseSha256(null)).toBe(false);
      expect(isLowercaseSha256(undefined)).toBe(false);
      expect(isLowercaseSha256(0)).toBe(false);
      expect(isLowercaseSha256(["0".repeat(64)])).toBe(false);
    });
  });

  describe("isPositiveSafeInteger", () => {
    test("accepts 1 and Number.MAX_SAFE_INTEGER", () => {
      expect(isPositiveSafeInteger(1)).toBe(true);
      expect(isPositiveSafeInteger(Number.MAX_SAFE_INTEGER)).toBe(true);
      expect(isPositiveSafeInteger(42)).toBe(true);
    });

    test("rejects zero, negatives, fractions, NaN, infinities, unsafe and non-numbers", () => {
      expect(isPositiveSafeInteger(0)).toBe(false);
      expect(isPositiveSafeInteger(-1)).toBe(false);
      expect(isPositiveSafeInteger(-Number.MAX_SAFE_INTEGER)).toBe(false);
      expect(isPositiveSafeInteger(1.5)).toBe(false);
      expect(isPositiveSafeInteger(0.5)).toBe(false);
      expect(isPositiveSafeInteger(Number.NaN)).toBe(false);
      expect(isPositiveSafeInteger(Number.POSITIVE_INFINITY)).toBe(false);
      expect(isPositiveSafeInteger(Number.NEGATIVE_INFINITY)).toBe(false);
      expect(isPositiveSafeInteger(Number.MAX_SAFE_INTEGER + 2)).toBe(false);
      expect(isPositiveSafeInteger(Number.MIN_SAFE_INTEGER)).toBe(false);
      expect(isPositiveSafeInteger("1")).toBe(false);
      expect(isPositiveSafeInteger(null)).toBe(false);
      expect(isPositiveSafeInteger(undefined)).toBe(false);
      expect(isPositiveSafeInteger(true)).toBe(false);
    });
  });

  describe("isNonNegativeSafeInteger", () => {
    test("accepts 0, 1 and Number.MAX_SAFE_INTEGER", () => {
      expect(isNonNegativeSafeInteger(0)).toBe(true);
      expect(isNonNegativeSafeInteger(1)).toBe(true);
      expect(isNonNegativeSafeInteger(Number.MAX_SAFE_INTEGER)).toBe(true);
    });

    test("rejects negatives, fractions, NaN, infinities, unsafe and non-numbers", () => {
      expect(isNonNegativeSafeInteger(-1)).toBe(false);
      expect(isNonNegativeSafeInteger(-0.5)).toBe(false);
      expect(isNonNegativeSafeInteger(-Number.MAX_SAFE_INTEGER)).toBe(false);
      expect(isNonNegativeSafeInteger(0.5)).toBe(false);
      expect(isNonNegativeSafeInteger(Number.NaN)).toBe(false);
      expect(isNonNegativeSafeInteger(Number.POSITIVE_INFINITY)).toBe(false);
      expect(isNonNegativeSafeInteger(Number.NEGATIVE_INFINITY)).toBe(false);
      expect(isNonNegativeSafeInteger(Number.MAX_SAFE_INTEGER + 2)).toBe(false);
      expect(isNonNegativeSafeInteger(Number.MIN_SAFE_INTEGER)).toBe(false);
      expect(isNonNegativeSafeInteger("0")).toBe(false);
      expect(isNonNegativeSafeInteger(null)).toBe(false);
      expect(isNonNegativeSafeInteger(undefined)).toBe(false);
      expect(isNonNegativeSafeInteger(false)).toBe(false);
    });
  });

  test("predicates are pure: exotic inputs are untouched, nothing is frozen", () => {
    const value = { accessed: 0 };
    const proxy = new Proxy(value, {
      get(target, prop) {
        if (prop !== "accessed") {
          target.accessed += 1;
        }
        return (target as Record<string | symbol, unknown>)[prop];
      },
    });
    expect(isPipelineV2SafeId(proxy)).toBe(false);
    expect(isLowercaseSha256(proxy)).toBe(false);
    expect(isPositiveSafeInteger(proxy)).toBe(false);
    expect(isNonNegativeSafeInteger(proxy)).toBe(false);
    expect(value.accessed).toBe(0);
    const frozen = Object.freeze("4".repeat(64));
    expect(isLowercaseSha256(frozen)).toBe(true);
  });

  test("export surface is exactly the four predicates", () => {
    expect(Object.keys(scalar).sort()).toEqual([
      "isLowercaseSha256",
      "isNonNegativeSafeInteger",
      "isPipelineV2SafeId",
      "isPositiveSafeInteger",
    ]);
  });
});

describe("pipeline v2 scalar single-source proof", () => {
  test("the neutral module owns the only SAFE_ID_PATTERN and SHA256_PATTERN", () => {
    expect(SCALAR_SOURCE).toContain("const SAFE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;");
    expect(SCALAR_SOURCE).toContain("const SHA256_PATTERN = /^[0-9a-f]{64}$/;");
  });

  test("both consumers carry no second pattern and no local predicate copies", () => {
    for (const [name, source] of [
      ["pipeline_v2_state.ts", STATE_SOURCE],
      ["pipeline_v2_wait_manifest.ts", MANIFEST_SOURCE],
    ] as const) {
      const assertNo = (needle: string) => {
        expect(source.includes(needle), `${name} must not contain ${needle}`).toBe(false);
      };
      assertNo("SAFE_ID_PATTERN");
      assertNo("SHA256_PATTERN");
      assertNo("function isSafeId(");
      assertNo("function isSha256Hex(");
      assertNo("function isSafePositiveInteger(");
      assertNo("function isSafeNonNegativeInteger(");
      assertNo("function isPositiveSafeInteger(");
      assertNo("function isNonNegativeSafeInteger(");
      expect(source.includes('from "./pipeline_v2_scalar.ts"'), `${name} imports the neutral module`).toBe(
        true,
      );
    }
  });
});
