/**
 * The single production owner of the static browser UI assets.
 *
 * The owner captures the exact asset set (`index.html`, `app.js`,
 * `style.css`) once at service startup, before `Bun.serve` runs: every
 * asset is read from disk exactly once, validated against fixed bounds
 * and its fixed content type, and returned as a deeply frozen captured
 * representation. The request handler never imports `fs`, never computes
 * a filesystem path and never touches the disk — it only reads the
 * captured bytes it was handed.
 *
 * The exact asset set is enforced: every declared asset must exist in
 * the asset root as a regular non-symlink file within its fixed size
 * bounds, and the served set is exactly the three declared routes. The
 * module also owns the production asset root computation; diagnostics
 * are content-free: no paths, no file contents, no system error text.
 */

import { lstatSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { isCleanAbsolutePath } from "../clean_path.ts";

/** The closed set of UI routes (one asset each). */
export type ControlPlaneUiRoute = "ui_root" | "ui_app_js" | "ui_style_css";

/** One captured asset: its route, fixed content type and exact bytes. */
export interface ControlPlaneUiAsset {
  readonly route: ControlPlaneUiRoute;
  readonly contentType: string;
  readonly bytes: Uint8Array<ArrayBuffer>;
}

/** The deeply frozen captured representation of the whole asset set. */
export interface ControlPlaneUiAssets {
  readonly ui_root: ControlPlaneUiAsset;
  readonly ui_app_js: ControlPlaneUiAsset;
  readonly ui_style_css: ControlPlaneUiAsset;
}

export type ControlPlaneUiAssetFailureReason =
  | "invalid_ui_root"
  | "missing_asset"
  | "asset_too_large";

export class ControlPlaneUiAssetError extends Error {
  readonly reason!: ControlPlaneUiAssetFailureReason;

  constructor(reason: ControlPlaneUiAssetFailureReason, message: string) {
    super(message);
    this.name = "ControlPlaneUiAssetError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** The fixed content type of every asset route. */
export const CONTROL_PLANE_UI_CONTENT_TYPES: Readonly<Record<ControlPlaneUiRoute, string>> = Object.freeze({
  ui_root: "text/html; charset=utf-8",
  ui_app_js: "text/javascript; charset=utf-8",
  ui_style_css: "text/css; charset=utf-8",
});

/** The fixed inclusive byte bounds of every asset route. */
export const CONTROL_PLANE_UI_BYTE_BOUNDS: Readonly<Record<ControlPlaneUiRoute, { readonly min: number; readonly max: number }>> =
  Object.freeze({
    ui_root: { min: 1, max: 65536 },
    ui_app_js: { min: 1, max: 524288 },
    ui_style_css: { min: 1, max: 131072 },
  });

const ASSET_FILE_NAMES: Readonly<Record<ControlPlaneUiRoute, string>> = Object.freeze({
  ui_root: "index.html",
  ui_app_js: "app.js",
  ui_style_css: "style.css",
});

const ALL_ROUTES: readonly ControlPlaneUiRoute[] = ["ui_root", "ui_app_js", "ui_style_css"];

/** The production asset root of the shipped orchestrator tree. */
export function defaultControlPlaneUiRoot(): string {
  return join(import.meta.dir, "..", "..", "ui");
}

/**
 * Captures the exact asset set from one explicit absolute root, once:
 * fixed content types, fixed size bounds, regular non-symlink files
 * only, and a frozen captured representation (the asset objects are
 * frozen; the byte arrays are exact one-time copies that are never
 * mutated and never re-read from disk). The served asset set is exactly
 * the three declared routes — any other entry in the asset root is
 * never served.
 */
export function loadControlPlaneUiAssets(uiRoot: string): ControlPlaneUiAssets {
  if (!isCleanAbsolutePath(uiRoot)) {
    throw new ControlPlaneUiAssetError("invalid_ui_root", "the ui asset root must be a clean absolute path");
  }
  let entries: string[];
  try {
    entries = readdirSync(uiRoot);
  } catch {
    throw new ControlPlaneUiAssetError("invalid_ui_root", "the ui asset root must be a readable directory");
  }
  if (entries.length < ALL_ROUTES.length) {
    throw new ControlPlaneUiAssetError("missing_asset", "the ui asset root must carry every declared asset");
  }
  const assets: { [K in ControlPlaneUiRoute]: ControlPlaneUiAsset } = {
    ui_root: captureAsset("ui_root", join(uiRoot, ASSET_FILE_NAMES["ui_root"])),
    ui_app_js: captureAsset("ui_app_js", join(uiRoot, ASSET_FILE_NAMES["ui_app_js"])),
    ui_style_css: captureAsset("ui_style_css", join(uiRoot, ASSET_FILE_NAMES["ui_style_css"])),
  };
  const frozen = Object.freeze({
    ui_root: Object.freeze(assets["ui_root"]),
    ui_app_js: Object.freeze(assets["ui_app_js"]),
    ui_style_css: Object.freeze(assets["ui_style_css"]),
  }) as ControlPlaneUiAssets;
  return frozen;
}

function captureAsset(route: ControlPlaneUiRoute, path: string): ControlPlaneUiAsset {
  const info = lstatSync(path, { throwIfNoEntry: false });
  if (info === undefined || !info.isFile()) {
    throw new ControlPlaneUiAssetError("missing_asset", "the ui asset must be a regular file");
  }
  const bounds = CONTROL_PLANE_UI_BYTE_BOUNDS[route];
  if (info.size < bounds.min || info.size > bounds.max) {
    throw new ControlPlaneUiAssetError("asset_too_large", "the ui asset must stay within its fixed size bounds");
  }
  const captured = readFileSync(path);
  if (captured.byteLength < bounds.min || captured.byteLength > bounds.max) {
    throw new ControlPlaneUiAssetError("asset_too_large", "the ui asset must stay within its fixed size bounds");
  }
  const bytes = new Uint8Array(captured);
  return { route, contentType: CONTROL_PLANE_UI_CONTENT_TYPES[route], bytes };
}
