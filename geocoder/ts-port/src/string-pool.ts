// Tier-aware string pool reader. Mirrors StringPool::get in
// geocoder/server/src/main.rs.  Records carry a global offset that
// addresses a virtual concatenation of up to 5 tier files; only loaded
// tiers resolve, the rest return "" (graceful degradation).

import { NO_DATA, STR_TIER_FILENAMES } from "./types.js";

interface TierLayout {
  name: string;
  file: string;
  start: number;
  end: number;
}

export class StringPool {
  private readonly tiers: (Buffer | null)[] = new Array(STR_TIER_FILENAMES.length).fill(null);
  private readonly bases: number[] = new Array(STR_TIER_FILENAMES.length + 1).fill(0);

  constructor(layoutJson: string, tierBuffers: (Buffer | null)[]) {
    const layout = JSON.parse(layoutJson) as { tiers: TierLayout[] };
    if (layout.tiers.length !== STR_TIER_FILENAMES.length) {
      throw new Error(`strings_layout.json has ${layout.tiers.length} tiers, expected ${STR_TIER_FILENAMES.length}`);
    }
    for (let t = 0; t < STR_TIER_FILENAMES.length; t++) {
      const entry = layout.tiers[t];
      this.bases[t] = entry.start;
      this.bases[t + 1] = entry.end;
      this.tiers[t] = tierBuffers[t];
    }
    if (this.tiers[0] === null) {
      throw new Error("Required strings_core.bin missing");
    }
  }

  get(offset: number): string {
    if (offset === NO_DATA || offset === 0xFFFFFFFF) return "";
    for (let t = 0; t < STR_TIER_FILENAMES.length; t++) {
      if (offset < this.bases[t + 1]) {
        const buf = this.tiers[t];
        if (buf === null) return "";
        const local = offset - this.bases[t];
        if (local >= buf.length) return "";
        // Find null terminator
        let end = local;
        while (end < buf.length && buf[end] !== 0) end++;
        return buf.toString("utf8", local, end);
      }
    }
    return "";
  }
}
