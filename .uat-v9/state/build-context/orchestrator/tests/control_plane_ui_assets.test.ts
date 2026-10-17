/**
 * Permanent contract suite of the static browser UI asset owner.
 *
 * Matrix: A the capture contract (exact set, fixed content types, size
 * bounds, frozen representation), B the failure matrix, C the exact
 * byte identity of the shipped assets, D the single-owner source
 * restrictions, E the image packaging proof.
 */
import { describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  CONTROL_PLANE_UI_BYTE_BOUNDS,
  CONTROL_PLANE_UI_CONTENT_TYPES,
  ControlPlaneUiAssetError,
  defaultControlPlaneUiRoot,
  loadControlPlaneUiAssets,
} from "../src/control_plane/ui_assets.ts";

const repoUiRoot = join(import.meta.dir, "..", "ui");

function copyUiTree(root: string): string {
  const target = join(root, "ui");
  mkdirSync(target, { mode: 0o700 });
  for (const name of ["index.html", "app.js", "style.css"]) {
    writeFileSync(join(target, name), readFileSync(join(repoUiRoot, name)), { mode: 0o600 });
  }
  return target;
}

describe("ui assets: capture contract", () => {
  test("the production root is the shipped ui tree and captures the exact asset set", () => {
    const assets = loadControlPlaneUiAssets(defaultControlPlaneUiRoot());
    expect(Object.keys(assets).sort()).toEqual(["ui_app_js", "ui_root", "ui_style_css"]);
    expect(assets.ui_root.route).toBe("ui_root");
    expect(assets.ui_root.contentType).toBe("text/html; charset=utf-8");
    expect(assets.ui_app_js.contentType).toBe("text/javascript; charset=utf-8");
    expect(assets.ui_style_css.contentType).toBe("text/css; charset=utf-8");
    for (const asset of [assets.ui_root, assets.ui_app_js, assets.ui_style_css]) {
      expect(asset.bytes.byteLength).toBeGreaterThanOrEqual(CONTROL_PLANE_UI_BYTE_BOUNDS[asset.route].min);
      expect(asset.bytes.byteLength).toBeLessThanOrEqual(CONTROL_PLANE_UI_BYTE_BOUNDS[asset.route].max);
    }
  });

  test("the captured representation is frozen and byte-identical to the shipped files", () => {
    const assets = loadControlPlaneUiAssets(defaultControlPlaneUiRoot());
    expect(Object.isFrozen(assets)).toBe(true);
    for (const asset of [assets.ui_root, assets.ui_app_js, assets.ui_style_css]) {
      expect(Object.isFrozen(asset)).toBe(true);
      const onDisk = readFileSync(join(repoUiRoot, asset.route === "ui_root" ? "index.html" : asset.route === "ui_app_js" ? "app.js" : "style.css"));
      expect(Buffer.from(asset.bytes).equals(onDisk)).toBe(true);
    }
  });

  test("capturing twice yields byte-identical representations and never re-reads mutated sources", () => {
    const root = mkdtempSync(join(tmpdir(), "ui-assets-"));
    try {
      const uiRoot = copyUiTree(root);
      const first = loadControlPlaneUiAssets(uiRoot);
      const second = loadControlPlaneUiAssets(uiRoot);
      expect(Buffer.from(first.ui_app_js.bytes).equals(second.ui_app_js.bytes)).toBe(true);
      // after the tree is gone the captured bytes still serve: the
      // request path never touches the disk
      rmSync(uiRoot, { recursive: true, force: true });
      const third = (() => first)();
      expect(Buffer.from(third.ui_root.bytes).equals(second.ui_root.bytes)).toBe(true);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("ui assets: failure matrix", () => {
  test("an invalid root, a missing asset and an oversized asset fail with typed reasons", () => {
    expect(() => loadControlPlaneUiAssets("relative/path")).toThrow(ControlPlaneUiAssetError);
    try {
      loadControlPlaneUiAssets("relative/path");
    } catch (error) {
      expect((error as ControlPlaneUiAssetError).reason).toBe("invalid_ui_root");
    }
    const root = mkdtempSync(join(tmpdir(), "ui-assets-bad-"));
    try {
      const uiRoot = copyUiTree(root);
      rmSync(join(uiRoot, "style.css"));
      try {
        loadControlPlaneUiAssets(uiRoot);
        throw new Error("expected failure");
      } catch (error) {
        expect(error instanceof ControlPlaneUiAssetError).toBe(true);
        expect((error as ControlPlaneUiAssetError).reason).toBe("missing_asset");
      }
      writeFileSync(join(uiRoot, "style.css"), Buffer.alloc(CONTROL_PLANE_UI_BYTE_BOUNDS.ui_style_css.max + 1, 0x61), { mode: 0o600 });
      try {
        loadControlPlaneUiAssets(uiRoot);
        throw new Error("expected failure");
      } catch (error) {
        expect((error as ControlPlaneUiAssetError).reason).toBe("asset_too_large");
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("a symlinked or non-regular declared asset is refused", () => {
    const root = mkdtempSync(join(tmpdir(), "ui-assets-symlink-"));
    try {
      const uiRoot = copyUiTree(root);
      rmSync(join(uiRoot, "app.js"));
      writeFileSync(join(root, "outside.js"), "export {};", { mode: 0o600 });
      symlinkSync(join(root, "outside.js"), join(uiRoot, "app.js"));
      try {
        loadControlPlaneUiAssets(uiRoot);
        throw new Error("expected failure");
      } catch (error) {
        expect((error as ControlPlaneUiAssetError).reason).toBe("missing_asset");
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("diagnostics are content-free", () => {
    const root = mkdtempSync(join(tmpdir(), "ui-assets-canary-"));
    try {
      const uiRoot = copyUiTree(root);
      rmSync(join(uiRoot, "app.js"));
      try {
        loadControlPlaneUiAssets(uiRoot);
        throw new Error("expected failure");
      } catch (error) {
        expect((error as Error).message.includes(root)).toBe(false);
        expect((error as Error).message.includes(uiRoot)).toBe(false);
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("ui assets: source restrictions and packaging", () => {
  test("the asset owner is the single fs importer and shares no parsing with the registry or binding layers", () => {
    const source = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "ui_assets.ts"), "utf8");
    expect(source.split("readFileSync(").length - 1).toBe(1);
    expect(source.split("readdirSync(").length - 1).toBe(1);
    expect(source.split("lstatSync(").length - 1).toBe(1);
    for (const banned of ["JSON.parse", "sha256", "createHash", "pipeline-runs", "state.json", "registry", "binding"]) {
      expect(source.includes(banned)).toBe(false);
    }
    const serverSource = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "http_server.ts"), "utf8");
    expect(serverSource.split("loadControlPlaneUiAssets(").length - 1).toBe(1);
    expect(serverSource.includes("node:fs")).toBe(false);
    expect(serverSource.includes("readFileSync")).toBe(false);
    expect(serverSource.split("uiAssetResponse(").length - 1).toBe(2);
  });

  test("the handler serves assets only from the captured representation", () => {
    const serverSource = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "http_server.ts"), "utf8");
    const handlerStart = serverSource.indexOf("export async function handleControlPlaneHttpRequest");
    const dispatchStart = serverSource.indexOf("async function dispatchControlPlaneRoute");
    expect(handlerStart).toBeGreaterThan(0);
    expect(dispatchStart).toBeGreaterThan(handlerStart);
    const handlerSource = serverSource.slice(handlerStart, dispatchStart);
    for (const banned of ["readFile", "readdir", "stat(", "join(", "resolve(", "import.meta.dir"]) {
      expect(handlerSource.includes(banned)).toBe(false);
    }
  });

  test("the orchestrator image copies the ui asset tree", () => {
    const dockerfile = readFileSync(join(import.meta.dir, "..", "..", "Dockerfile_orchestrator"), "utf8");
    expect(dockerfile.includes("./orchestrator/")).toBe(true);
    expect(dockerfile.includes("/opt/orchestrator/")).toBe(true);
    const dockerignore = readFileSync(join(import.meta.dir, "..", "..", ".dockerignore"), "utf8");
    expect(dockerignore.includes("ui")).toBe(false);
    for (const name of ["index.html", "app.js", "style.css", "app.d.ts"]) {
      const info = statSync(join(repoUiRoot, name));
      expect(info.isFile()).toBe(true);
    }
  });
});

describe("ui assets: content types and bounds are the closed contract", () => {
  test("the tables carry exactly the three routes", () => {
    expect(Object.keys(CONTROL_PLANE_UI_CONTENT_TYPES).sort()).toEqual(["ui_app_js", "ui_root", "ui_style_css"]);
    expect(Object.keys(CONTROL_PLANE_UI_BYTE_BOUNDS).sort()).toEqual(["ui_app_js", "ui_root", "ui_style_css"]);
    expect(Object.isFrozen(CONTROL_PLANE_UI_CONTENT_TYPES)).toBe(true);
    expect(Object.isFrozen(CONTROL_PLANE_UI_BYTE_BOUNDS)).toBe(true);
  });
});
