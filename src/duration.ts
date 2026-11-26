type DurationUnit = 'ms' | 's' | 'm' | 'h' | 'd';

/**
 * A length of time: a number of milliseconds, or a string with a unit,
 * e.g. `'500ms'`, `'10s'`, `'1m'`, `'2h'`, `'1d'`.
 */
export type Duration = number | `${number}${DurationUnit}`;

const UNIT_MS: Record<DurationUnit, number> = {
  ms: 1,
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000,
  d: 24 * 60 * 60 * 1000,
};

const DURATION_PATTERN = /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$/;

/** Converts a `Duration` to milliseconds. Throws on anything that isn't a positive duration. */
export function parseDuration(value: Duration, name = 'duration'): number {
  let ms: number;
  if (typeof value === 'number') {
    ms = value;
  } else {
    const match = typeof value === 'string' ? DURATION_PATTERN.exec(value) : null;
    if (!match) {
      throw new Error(`Invalid ${name} '${value}': use a number of ms or a string like '500ms', '10s', '1m', '2h', '1d'`);
    }
    ms = Number(match[1]) * UNIT_MS[match[2] as DurationUnit];
  }
  if (!(ms > 0) || !isFinite(ms)) {
    throw new Error(`Invalid ${name} '${value}': must be greater than 0`);
  }
  return ms;
}
