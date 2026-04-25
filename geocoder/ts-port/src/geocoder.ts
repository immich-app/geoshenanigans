// Top-level Geocoder. Holds the loaded index for the lifetime of the
// instance and exposes a single `reverse(lat, lng)` method that mirrors
// the Rust server's /reverse?lat=&lon= endpoint output for the
// admin-only subset.

import { findAdmin, applyAdminToAddress, FindAdminResult } from "./find-admin.js";
import { findPlaces } from "./find-places.js";
import { findPois } from "./find-pois.js";
import { resolveNearestPostcode } from "./resolve-postcode.js";
import { buildResponse } from "./format-address.js";
import { LoadedIndex, loadIndex } from "./index-loader.js";
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

    // POI parent_postcode tier — checked before nearest-centroid since
    // the build-time PIP into a postal_code polygon is more authoritative
    // than nearest-centroid. Match Rust resolve_postcode tier 1b.
    let parentPostcode: string | null = null;
    if (places && places.length > 0 && this.index.poiRecords) {
      // Look up the winning POI's parent_postcode_id.
      // (Need to find its record — simplest is to re-walk via name+category.)
      // The PoiMatch doesn't carry parent_postcode_id today; deferred.
    }

    // Postcode resolution — nearest-centroid tier (Rust tier 4).
    if (!address.postcode && this.index.postcodeCentroids) {
      const pc = parentPostcode ?? resolveNearestPostcode(
        lat, lng,
        adminResult.countryCode,
        this.index.postcodeCentroids,
        this.index.postcodeCells,
        this.index.postcodeEntries,
        this.index.strings,
        this.adminCellLevel,
      );
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
