import type { Configurations, Selections, AxisId, Resolved } from './types.js';

// Substitute {region}/{quality}/{mode}/etc. placeholders in a path
// against the user's current selections.
function substitute(path: string, selections: Selections): string {
  return path.replace(/\{([a-z_]+)\}/g, (_, k: AxisId) => selections[k] ?? `{${k}}`);
}

// Walk the components map for the chosen axes and gather the union of
// files, applying any `extends` chain and `replaces` exclusions.
export function resolve(cfg: Configurations, sel: Selections): Resolved {
  // Component IDs to include for these selections. Order matters for
  // `replaces` — a later component's `replaces` takes precedence over
  // an earlier component's `files`.
  const componentIds: string[] = ['always'];
  for (const axis of ['mode', 'quality', 'poi_tier'] as const) {
    const v = sel[axis];
    if (v) componentIds.push(`${axis}.${v}`);
  }

  const filesSet = new Map<string, void>(); // ordered insertion
  const replacedSet = new Set<string>();

  // Resolve a single component, walking `extends` ancestors first.
  const visit = (id: string, seen: Set<string>) => {
    if (seen.has(id)) return;
    seen.add(id);
    const c = cfg.components[id];
    if (!c) return;
    if (c.extends) visit(c.extends, seen);
    for (const f of c.files) filesSet.set(substitute(f, sel));
    for (const r of c.replaces ?? []) replacedSet.add(substitute(r, sel));
  };
  for (const id of componentIds) visit(id, new Set());

  // Apply `replaces` to drop files superseded by a more specific component.
  const out: Resolved = { files: [], total_zst: 0, total_raw: 0, missing: [] };
  for (const path of filesSet.keys()) {
    if (replacedSet.has(path)) continue;
    const entry = cfg.files[path];
    if (!entry) {
      out.missing.push(path);
      continue;
    }
    out.files.push({ path, ...entry });
    out.total_zst += entry.size_zst;
    out.total_raw += entry.size_raw;
  }
  return out;
}

// Check every constraint against the current selections; return a list
// of violations (so the UI can surface them or auto-correct).
export interface Violation {
  constraint_id: string;
  message: string;
  forced: Partial<Selections>; // axis -> what the user would need to switch to
}

function matches(rule: Record<string, string | string[]>, sel: Selections): boolean {
  for (const [axis, want] of Object.entries(rule)) {
    const have = sel[axis as AxisId];
    if (have === undefined) return false;
    const wantList = Array.isArray(want) ? want : [want];
    if (!wantList.includes(have)) return false;
  }
  return true;
}

export function violations(cfg: Configurations, sel: Selections): Violation[] {
  const result: Violation[] = [];
  for (const c of cfg.constraints) {
    if (!matches(c.when, sel)) continue;
    if (c.requires && !matches(c.requires, sel)) {
      // Find the first axis the user would need to change.
      const forced: Partial<Selections> = {};
      for (const [axis, want] of Object.entries(c.requires)) {
        const wantList = Array.isArray(want) ? want : [want];
        forced[axis as AxisId] = wantList[0];
      }
      result.push({ constraint_id: c.id, message: c.message ?? c.id, forced });
    }
    if (c.excludes && matches(c.excludes, sel)) {
      result.push({ constraint_id: c.id, message: c.message ?? c.id, forced: {} });
    }
  }
  return result;
}

// Decide whether a given axis value is selectable under the current
// selections — used to grey out incompatible options in the UI.
export function isAvailable(
  cfg: Configurations,
  sel: Selections,
  axis: AxisId,
  value: string
): boolean {
  const probe: Selections = { ...sel, [axis]: value };
  return violations(cfg, probe).length === 0;
}
