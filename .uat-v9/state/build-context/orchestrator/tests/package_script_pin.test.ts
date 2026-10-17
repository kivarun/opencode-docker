import { describe, expect, test } from "bun:test";
import { readFileSync, existsSync } from "node:fs";

/**
 * Configuration pin: the standard full-suite command is the package script
 * `bun test --parallel=2` (see AGENTS.md). This pin guards the command
 * contract without running the suite: the script shape, the absence of
 * forbidden runner flags, the absence of environment or CPU-count
 * heuristics, and the absence of a bunfig.toml that could silently change
 * test semantics.
 */

const packageJsonUrl = new URL("../package.json", import.meta.url);
const packageJson: {
  scripts?: Record<string, string>;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
  overrides?: Record<string, string>;
} = JSON.parse(readFileSync(packageJsonUrl, "utf8"));

const FORBIDDEN_FLAGS = ["--timeout", "--retry", "--concurrent", "--no-isolate", "--shard"];

describe("package test-script pin", () => {
  test("scripts.test is exactly the measured parallel=2 suite command", () => {
    expect(packageJson.scripts?.test).toBe("bun test --parallel=2");
  });

  test("no test script carries forbidden runner flags or a shell wrapper", () => {
    const testScripts = Object.entries(packageJson.scripts ?? {}).filter(([name]) =>
      name.startsWith("test"),
    );
    expect(testScripts.length).toBeGreaterThanOrEqual(1);
    for (const [name, command] of testScripts) {
      for (const flag of FORBIDDEN_FLAGS) {
        expect(command.includes(flag)).toBe(false);
      }
      expect(command.includes("sh -c")).toBe(false);
      expect(command.includes("&&")).toBe(false);
      expect(command.includes("cd ")).toBe(false);
    }
  });

  test("the test command does not depend on environment or CPU count", () => {
    const command = packageJson.scripts?.test ?? "";
    expect(command.includes("nproc")).toBe(false);
    expect(command.includes("$(")).toBe(false);
    expect(command.includes("${")).toBe(false);
    expect(command.toUpperCase().includes("CPU")).toBe(false);
  });

  test("no bunfig.toml exists that could silently change test semantics", () => {
    expect(existsSync(new URL("../bunfig.toml", import.meta.url))).toBe(false);
    expect(existsSync(new URL("../../bunfig.toml", import.meta.url))).toBe(false);
  });

  test("the pinned runtime dependency set is unchanged", () => {
    expect(packageJson.dependencies).toEqual({ ajv: "8.17.1" });
    expect(packageJson.devDependencies).toEqual({ "@types/bun": "1.4.1" });
    expect(packageJson.overrides).toEqual({ "@types/node": "26.5.0" });
  });
});
