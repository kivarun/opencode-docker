/**
 * The crash-fault marker of the apply writer test seams: an error thrown
 * by an injected filesystem or journal seam and marked with this property
 * simulates abrupt process death at exactly that point. The writer treats
 * a marked error as unrecoverable process state: it rethrows immediately
 * without any cleanup, temp removal, journal write or status transition —
 * exactly what a real SIGKILL leaves behind. Real runtime errors never
 * carry the marker and always take the typed graceful path.
 */
export const APPLY_CRASH_FAULT_MARKER = "applyCrashFault";

export function applyCrashFault(message: string): Error {
  const error = new Error(message);
  Object.defineProperty(error, APPLY_CRASH_FAULT_MARKER, {
    value: true,
    enumerable: false,
    writable: false,
    configurable: false,
  });
  return error;
}

export function isApplyCrashFault(cause: unknown): boolean {
  return (
    typeof cause === "object" &&
    cause !== null &&
    (cause as Record<string, unknown>)[APPLY_CRASH_FAULT_MARKER] === true
  );
}
