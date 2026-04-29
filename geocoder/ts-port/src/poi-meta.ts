// poi_meta.json loader. Mirrors Rust PoiMeta::load.  The file maps
// numeric category IDs (as JSON-string keys) to per-category metadata
// the server uses to score a POI's relevance to a query: a reference
// distance for the proximity-decay falloff, a hard max distance gate,
// and a default importance when the POI's own importance byte is 0.

import { readFileSync } from "node:fs";
import { join } from "node:path";

interface CategoryMeta {
  name: string;
  reference_distance: number;
  max_distance: number;
  default_importance: number;
}

const FALLBACK: CategoryMeta = {
  name: "unknown",
  reference_distance: 100,
  max_distance: 500,
  default_importance: 5,
};

export class PoiMeta {
  private categories = new Map<number, CategoryMeta>();

  static load(dir: string): PoiMeta {
    const m = new PoiMeta();
    try {
      const raw = readFileSync(join(dir, "poi_meta.json"), "utf8");
      const obj = JSON.parse(raw) as Record<string, Partial<CategoryMeta>>;
      for (const [k, v] of Object.entries(obj)) {
        const id = Number(k);
        if (Number.isNaN(id)) continue;
        m.categories.set(id, {
          name: typeof v.name === "string" ? v.name : "unknown",
          reference_distance: typeof v.reference_distance === "number" ? v.reference_distance : FALLBACK.reference_distance,
          max_distance: typeof v.max_distance === "number" ? v.max_distance : FALLBACK.max_distance,
          default_importance: typeof v.default_importance === "number" ? v.default_importance : FALLBACK.default_importance,
        });
      }
    } catch {
      // Missing poi_meta.json — geocoder still works, just everything
      // ranks as "unknown" with default thresholds.
    }
    return m;
  }

  categoryName(id: number): string { return (this.categories.get(id) ?? FALLBACK).name; }
  referenceDistance(id: number): number { return (this.categories.get(id) ?? FALLBACK).reference_distance; }
  maxDistance(id: number): number { return (this.categories.get(id) ?? FALLBACK).max_distance; }
  defaultImportance(id: number): number { return (this.categories.get(id) ?? FALLBACK).default_importance; }
}
