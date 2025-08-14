import { IDatabaseRepository, IMemCacheRepository, IMetricsRepository, IStorageRepository } from './interface';
import { Directory, Entry, Header, JsonResponse, Metadata } from './pmtiles/types';
import {
  bytesToHeader,
  decompress,
  deserializeIndex,
  fromRadix64,
  getJsonCacheKey,
  toRadix64,
  zxyToTileId,
} from './pmtiles/utils';

const HEADER_SIZE_BYTES = 127;

export class DirectoryString {
  constructor(private entry: string) {}

  static fromDirectory(directory: Directory): DirectoryString {
    let entryString = `${toRadix64(directory.tileIdStart)}|${toRadix64(directory.offsetStart)}:`;
    for (const entry of directory.entries) {
      entryString += `${toRadix64(entry.tileId - directory.tileIdStart)}|${toRadix64(entry.offset - directory.offsetStart)}|${toRadix64(entry.length)}|${toRadix64(entry.runLength)}|${toRadix64(entry.chunkId)}:`;
    }
    return new DirectoryString(entryString);
  }

  toString(): string {
    return this.entry;
  }

  async findTile(searchedTileId: number): Promise<Entry | undefined> {
    let offsetStart: number | undefined;
    let tileIdStart: number | undefined;

    const lines = this.entry.split(':');
    for (const line of lines) {
      const parts = line.split('|');
      if (parts.length === 2) {
        tileIdStart = fromRadix64(parts[0]);
        offsetStart = fromRadix64(parts[1]);
        continue;
      }
      if (tileIdStart === undefined || offsetStart === undefined) {
        throw new Error('Invalid entry, we have no tileIdStart or offsetStart');
      }
      const tileId = fromRadix64(parts[0]) + tileIdStart;
      const runLength = fromRadix64(parts[3]);

      // If these conditions pass, we have found the correct tile
      if (tileId === searchedTileId || (tileId < searchedTileId && tileId + runLength > searchedTileId)) {
        return {
          tileId,
          offset: fromRadix64(parts[1]) + offsetStart,
          length: fromRadix64(parts[2]),
          chunkId: fromRadix64(parts[4]),
          runLength,
        };
      }
    }
  }
}

export class PMTilesService {
  private constructor(
    private source: IStorageRepository,
    private memCache: IMemCacheRepository,
    private metrics: IMetricsRepository,
    private db: IDatabaseRepository,
  ) {}

  static async init(
    source: IStorageRepository,
    memCache: IMemCacheRepository,
    metrics: IMetricsRepository,
    db: IDatabaseRepository,
  ): Promise<PMTilesService> {
    const p = new PMTilesService(source, memCache, metrics, db);
    const jsonCacheKey = getJsonCacheKey(source.getDeploymentKey());
    if (memCache.get(jsonCacheKey)) {
      return p;
    }
    memCache.set(jsonCacheKey, await p.getJson());
    return p;
  }

  async getHeaderAndRootFromSource(): Promise<[Header, Directory]> {
    const resp = await this.source.getRange({ offset: 0, length: 16384 });
    const v = new DataView(resp);
    if (v.getUint16(0, true) !== 0x4d50) {
      throw new Error('Wrong magic number for PMTiles archive');
    }

    const headerData = resp.slice(0, HEADER_SIZE_BYTES);
    const header = await bytesToHeader(headerData);
    const rootDirData = resp.slice(header.rootDirectoryOffset, header.rootDirectoryOffset + header.rootDirectoryLength);
    const rootDirEntries = deserializeIndex(
      await new Response(await decompress(rootDirData, header.internalCompression)).arrayBuffer(),
    );
    const rootDir: Directory = {
      offsetStart: rootDirEntries[0].offset,
      tileIdStart: rootDirEntries[0].tileId,
      entries: rootDirEntries,
    };
    return [header, rootDir];
  }

  private async getJson(): Promise<JsonResponse> {
    const cacheKey = getJsonCacheKey(this.source.getDeploymentKey());
    const cache = this.memCache.get<JsonResponse>(cacheKey);
    if (cache) {
      return cache;
    }
    const query = await this.db.query(
      `SELECT * FROM cache_entries_${this.source.getDeploymentKey()} WHERE startTileId = -1 LIMIT 1`,
    );

    if (query.error || !query.success || query.results.length === 0) {
      throw new Error('Error while looking up tile location');
    }

    return JSON.parse(query.results[0].entry as string);
  }

  async getJsonResponse(version: string, url: URL): Promise<JsonResponse> {
    const json = await this.getJson();
    json.tiles = [
      `${url.protocol}//` +
        url.hostname +
        `${url.port ? `:${url.port}` : ''}` +
        `/v${version}` +
        '/{z}/{x}/{y}' +
        '.mvt',
    ];
    return json;
  }

  async getTile(z: number, x: number, y: number): Promise<ReadableStream | undefined> {
    console.log('getTile', z, x, y);
    const tileId = zxyToTileId(z, x, y);
    const json = await this.getJson();

    if (z < json.minzoom || z > json.maxzoom) {
      return;
    }

    const dbLookup = await this.metrics.monitorAsyncFunction({ name: 'd1_lookup' }, (tileId) =>
      this.db.query(
        `SELECT * FROM cache_entries_${this.source.getDeploymentKey()} WHERE startTileId <= ? ORDER BY startTileId DESC LIMIT 1`,
        tileId,
      ),
    )(tileId);

    if (dbLookup.error) {
      throw new Error('Error while looking up tile location');
    }

    if (!dbLookup.success || dbLookup.results.length === 0) {
      return;
    }

    const leafDirectory = new DirectoryString(dbLookup.results[0].entry as string);

    const entry = await this.metrics.monitorAsyncFunction({ name: 'find_tile_leaf_directory' }, (tileId) =>
      leafDirectory.findTile(tileId),
    )(tileId);

    if (!entry) {
      return;
    }

    const tileOffset = entry.offset;
    const tileLength = entry.length;
    const chunkId = entry.chunkId;

    const tile = await this.metrics.monitorAsyncFunction({ name: 'get_tile' }, (offset, length) =>
      this.source.getRangeAsStream({ offset, length }, `chunk_${chunkId}.pmtiles`),
    )(tileOffset, tileLength);
    return tile;
  }

  async getMetadata(header: Header): Promise<Metadata> {
    const resp = await this.source.getRange({
      offset: header.jsonMetadataOffset,
      length: header.jsonMetadataLength,
    });
    const decompressed = await decompress(resp, header.internalCompression);
    const dec = new TextDecoder('utf-8');
    return JSON.parse(dec.decode(decompressed));
  }
}
