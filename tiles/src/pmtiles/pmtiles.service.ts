import { IDatabaseRepository, IMemCacheRepository, IMetricsRepository, IStorageRepository } from '../interface';
import { Directory, Entry, Header, Metadata } from './types';
import {
  bytesToHeader,
  decompress,
  deserializeIndex,
  fromRadix64,
  getDirectoryCacheKey,
  getHeaderCacheKey,
  tileJSON,
  toRadix64,
  zxyToTileId,
} from './utils';

const HEADER_SIZE_BYTES = 127;

export class DirectoryString {
  constructor(private entry: string) {}

  static fromDirectory(directory: Directory): DirectoryString {
    let entryString = `${toRadix64(directory.tileIdStart)}|${toRadix64(directory.offsetStart)}:`;
    for (const entry of directory.entries) {
      entryString += `${toRadix64(entry.tileId - directory.tileIdStart)}|${toRadix64(entry.offset - directory.offsetStart)}|${toRadix64(entry.length)}|${toRadix64(entry.runLength)}:`;
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
    const headerCacheKey = getHeaderCacheKey(source.getDeploymentKey());
    if (memCache.get(headerCacheKey)) {
      return p;
    }
    const [header, root] = await p.getHeaderAndRootFromSource();
    memCache.set(headerCacheKey, header);
    memCache.set(
      getDirectoryCacheKey(source.getDeploymentKey(), {
        offset: header.rootDirectoryOffset,
        length: header.rootDirectoryLength,
      }),
      root,
    );
    return p;
  }

  private getHeader(): Header {
    const key = getHeaderCacheKey(this.source.getDeploymentKey());
    const memCached = this.memCache.get<Header>(key);
    if (!memCached) {
      throw new Error('Header not found in cache');
    }
    return memCached;
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

  async getJsonResponse(version: string, url: URL) {
    const header = this.getHeader();
    const metadata = await this.getMetadata();
    return tileJSON({ header, metadata, url, version });
  }

  async getTile(z: number, x: number, y: number): Promise<ReadableStream | undefined> {
    console.log('getTile', z, x, y);
    const tileId = zxyToTileId(z, x, y);
    const header = this.getHeader();

    if (z < header.minZoom || z > header.maxZoom) {
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

    const tile = await this.metrics.monitorAsyncFunction({ name: 'get_tile' }, (offset, length) =>
      this.source.getRangeAsStream({ offset, length }),
    )(header.tileDataOffset + tileOffset, tileLength);
    return tile;
  }

  async getMetadata(): Promise<Metadata> {
    const header = this.getHeader();

    const resp = await this.source.getRange({
      offset: header.jsonMetadataOffset,
      length: header.jsonMetadataLength,
    });
    const decompressed = await decompress(resp, header.internalCompression);
    const dec = new TextDecoder('utf-8');
    return JSON.parse(dec.decode(decompressed));
  }
}
