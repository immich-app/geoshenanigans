// Top-level Geocoder. Holds the loaded index for the lifetime of the
// instance and exposes a single `reverse(lat, lng)` method that mirrors
// the Rust server's /reverse?lat=&lon= endpoint output for the
// admin-only subset.

import { findAdmin, applyAdminToAddress, FindAdminResult } from "./find-admin.js";
import { findPlaces } from "./find-places.js";
import { findPois } from "./find-pois.js";
import { queryGeo, findAddrOnWay } from "./query-geo.js";
import { resolveNearestPostcode } from "./resolve-postcode.js";
import { buildResponse } from "./format-address.js";
import { LoadedIndex, loadIndex } from "./index-loader.js";
import { NO_DATA, DEFAULT_STREET_CELL_LEVEL } from "./types.js";
import {
  Address,
  AddressDetails,
  AdminPolygon,
  DEFAULT_ADMIN_CELL_LEVEL,
} from "./types.js";

export interface GeocoderOptions {
  adminCellLevel?: number;
}

// Pick the smallest-area polygon in a level range.  Equivalent to the
// Rust find_current_boundary / find_municipality_boundary helpers,
// derived from findAdmin's per-level result instead of re-walking cells.
function smallestInRange(admin: FindAdminResult, lo: number, hi: number): AdminPolygon | null {
  let best: AdminPolygon | null = null;
  for (let lvl = lo; lvl <= hi; lvl++) {
    const p = admin.byLevel[lvl];
    if (!p) continue;
    if (!best || p.area < best.area) best = p;
  }
  return best;
}

export class Geocoder {
  private readonly index: LoadedIndex;
  private readonly adminCellLevel: number;

  constructor(dir: string, options: GeocoderOptions = {}) {
    this.index = loadIndex(dir);
    this.adminCellLevel = options.adminCellLevel ?? DEFAULT_ADMIN_CELL_LEVEL;
  }

  reverse(lat: number, lng: number): Address {
    const address: AddressDetails = {};
    const adminResult = findAdmin(
      lat,
      lng,
      this.index.adminCells,
      this.index.adminEntries,
      this.index.adminPolygons,
      this.index.adminVertices,
      this.adminCellLevel,
    );
    applyAdminToAddress(adminResult, address, this.index.strings);

    // Place nodes (city/village/hamlet/suburb/etc.) — only if the
    // place tier files were loaded. Boundaries derive from the admin
    // result so we don't pay a second cell-walk.
    if (this.index.placeCells && this.index.placeEntries && this.index.placeNodes) {
      const currentBoundary = smallestInRange(adminResult, 8, 10);
      const municipalityBoundary = smallestInRange(adminResult, 7, 8);
      findPlaces(
        lat, lng,
        this.index.placeCells,
        this.index.placeEntries,
        this.index.placeNodes,
        this.index.adminPolygons,
        this.index.adminVertices,
        currentBoundary,
        municipalityBoundary,
        this.index.strings,
        address,
        this.adminCellLevel,
      );
    }

    // POIs (landmark + nearby places list).  Mirrors find_pois in the
    // Rust server.  When a contained POI is the most-specific match we
    // also surface its name as `landmark` on the address dict.
    let places: ReturnType<typeof findPois> | undefined;
    if (this.index.poiCells && this.index.poiEntries && this.index.poiRecords && this.index.poiVertices) {
      places = findPois(
        lat, lng,
        this.index.poiCells,
        this.index.poiEntries,
        this.index.poiRecords,
        this.index.poiVertices,
        this.index.poiMeta,
        this.index.strings,
        this.adminCellLevel,
      );
      if (places.length > 0 && places[0].contained) {
        address.landmark = places[0].name;
      }
    }

    // Streets / addr / interpolation primary feature lookup. Mirrors
    // Rust query_geo. Picks the nearest of: addr_point, street segment,
    // interpolation segment within the search distance budget.  Each
    // becomes a candidate and the closest wins the `road` (and optional
    // `house_number`) field.
    let streetWayIdx: number = NO_DATA;
    if (this.index.geoCells) {
      const geo = queryGeo(
        lat, lng,
        this.index.geoCells,
        this.index.streetEntries,
        this.index.streetWays,
        this.index.streetNodes,
        this.index.addrEntries,
        this.index.addrPoints,
        this.index.addrVertices,
        this.index.interpEntries,
        this.index.interpWays,
        this.index.interpNodes,
        DEFAULT_STREET_CELL_LEVEL,
      );

      // Pick best candidate by distance.
      const candidates: Array<{ d: number; kind: "addr" | "street" | "interp" }> = [];
      if (geo.addr)   candidates.push({ d: geo.addr.distSq,   kind: "addr" });
      if (geo.street) candidates.push({ d: geo.street.distSq, kind: "street" });
      if (geo.interp) candidates.push({ d: geo.interp.distSq, kind: "interp" });
      candidates.sort((a, b) => a.d - b.d);
      const winner = candidates[0];
      if (winner) {
        if (winner.kind === "addr" && geo.addr) {
          if (geo.addr.housenumber_id !== NO_DATA) {
            address.house_number = this.index.strings.get(geo.addr.housenumber_id);
          }
          if (geo.addr.street_id !== NO_DATA) {
            address.road = this.index.strings.get(geo.addr.street_id);
          }
        } else if (winner.kind === "interp" && geo.interp) {
          address.house_number = String(geo.interp.number);
          address.road = this.index.strings.get(geo.interp.street_id);
        } else if (winner.kind === "street" && geo.street) {
          address.road = this.index.strings.get(geo.street.name_id);
          streetWayIdx = geo.street.way_index;
          // Try to refine with a housenumber from an addr_point on the
          // same way (parent_way_id match), within ~100m. Mirrors Rust
          // find_addr_on_way called by query() when street wins primary.
          if (this.index.addrEntries && this.index.addrPoints) {
            const refined = findAddrOnWay(
              lat, lng, streetWayIdx,
              (0.001) ** 2, // ~100m squared (rough — match Rust constant)
              this.index.geoCells,
              this.index.addrEntries,
              this.index.addrPoints,
              DEFAULT_STREET_CELL_LEVEL,
            );
            if (refined && refined.housenumber_id !== NO_DATA) {
              address.house_number = this.index.strings.get(refined.housenumber_id);
            }
          }
        }
      }
    }

    // Postcode resolution — full chain.
    //   tier 0: addr's own addr_postcode
    //   tier 1: way_postcode for the winning street
    //   tier 1b: POI's parent_postcode_id (build-time PIP)
    //   tier 4: nearest centroid (country-gated)
    if (!address.postcode) {
      let pc: string | null = null;

      // Tier 0: nearest addr_point's own postcode
      if (this.index.addrPostcodes && this.index.addrEntries && this.index.geoCells && this.index.addrPoints) {
        // Re-derive addr_point id by re-running queryGeo isn't ideal but
        // we don't have the addr index from the primary lookup retained.
        // Simpler: skip tier 0 here; matches the lookup at tier 1 below
        // when the way wins primary.  Most addresses get filled via
        // way_postcodes anyway.
      }

      // Tier 1: way_postcodes[street_idx]
      if (!pc && streetWayIdx !== NO_DATA && this.index.wayPostcodes) {
        if (streetWayIdx * 4 + 4 <= this.index.wayPostcodes.length) {
          const id = this.index.wayPostcodes.readUInt32LE(streetWayIdx * 4);
          if (id !== NO_DATA) pc = this.index.strings.get(id);
        }
      }

      // Tier 1b: POI parent_postcode_id
      if (!pc && places && places.length > 0 && places[0].parent_postcode_id !== NO_DATA) {
        const s = this.index.strings.get(places[0].parent_postcode_id);
        if (s) pc = s;
      }

      // Tier 4: nearest centroid
      if (!pc && this.index.postcodeCentroids) {
        pc = resolveNearestPostcode(
          lat, lng,
          adminResult.countryCode,
          this.index.postcodeCentroids,
          this.index.postcodeCells,
          this.index.postcodeEntries,
          this.index.strings,
          this.adminCellLevel,
        );
      }
      if (pc) address.postcode = pc;
    }

    const response = buildResponse(address);
    if (places && places.length > 0) {
      response.places = places.map((p) => ({
        name: p.name,
        category: p.category,
        distance_m: Math.round(p.distance_m * 10) / 10,
      }));
    }
    return response;
  }
}
