/**
 * The neutral deep-freeze helper of the pipeline v2 layers (internal,
 * filesystem-free).
 *
 * The single owner of `deepFreezeValue`: recursively `Object.freeze`es
 * every array and plain object of a value tree and returns the same
 * value. Symbols, getters, prototypes and non-plain objects are left
 * untouched; the semantics are the ones the previous inline copies used.
 *
 * This module deliberately imports nothing — no `node:*` modules, no
 * filesystem, crypto, store, sink or controller dependencies — so pure
 * layers (the compiled run plan projection, the run-plan candidate, the
 * acceptance and stage iteration controllers) can deep-freeze their
 * results without transitively loading a filesystem substrate.
 *
 * Runtime surface: exactly `deepFreezeValue`.
 */
export function deepFreezeValue<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const entry of value) {
      deepFreezeValue(entry);
    }
    Object.freeze(value);
    return value;
  }
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) {
      deepFreezeValue(child);
    }
    Object.freeze(value);
  }
  return value;
}
