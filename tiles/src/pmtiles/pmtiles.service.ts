import { IKeyValueRepository, IMemCacheRepository, IStorageRepository } from '../interface';
import { Metrics } from '../monitor';
import { Directory, Header, Metadata } from './types';
import {
  bytesToHeader,
  decompress,
  deserializeIndex,
  findTile,
  getDirectoryCacheKey,
  getHeaderCacheKey,
  tileJSON,
  zxyToTileId,
} from './utils';

const HEADER_SIZE_BYTES = 127;

export class PMTilesService {
  private constructor(
    private source: IStorageRepository,
    private memCache: IMemCacheRepository,
    private kvCache: IKeyValueRepository,
    private ctx: ExecutionContext,
  ) {}

  static async init(
    source: IStorageRepository,
    memCache: IMemCacheRepository,
    kvCache: IKeyValueRepository,
    ctx: ExecutionContext,
  ): Promise<PMTilesService> {
    const p = new PMTilesService(source, memCache, kvCache, ctx);
    const headerCacheKey = getHeaderCacheKey(source.getFileName());
    if (memCache.get(headerCacheKey)) {
      return p;
    }
    const [header, root] = await p.getHeaderAndRootFromSource();
    memCache.set(headerCacheKey, header);
    memCache.set(
      getDirectoryCacheKey(source.getFileName(), {
        offset: header.rootDirectoryOffset,
        length: header.rootDirectoryLength,
      }),
      root,
    );
    return p;
  }

  private getHeader(): Header {
    const key = getHeaderCacheKey(this.source.getFileName());
    const memCached = this.memCache.get<Header>(key);
    if (!memCached) {
      throw new Error('Header not found in cache');
    }
    return memCached;
  }

  private getRootDirectory(header: Header): Directory {
    const key = getDirectoryCacheKey(this.source.getFileName(), {
      offset: header.rootDirectoryOffset,
      length: header.rootDirectoryLength,
    });
    const root = this.memCache.get<Directory>(key);
    if (!root) {
      throw new Error('Root directory not found in cache');
    }
    return root as Directory;
  }

  private async getHeaderAndRootFromSource(): Promise<[Header, Directory]> {
    const resp = await this.source.get({ offset: 0, length: 16384 });
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

  private async getDirectory(offset: number, length: number, header: Header): Promise<Directory> {
    const cacheKey = getDirectoryCacheKey(this.source.getFileName(), { offset, length });
    const kvValue = await this.kvCache.get(cacheKey);
    if (kvValue) {
      const directory = JSON.parse(kvValue) as Directory;
      return directory;
    }
    const resp = await this.source.get({ offset, length });
    const data = await decompress(resp, header.internalCompression);
    const entries = deserializeIndex(data);
    if (entries.length === 0) {
      throw new Error('Empty directory is invalid');
    }
    const directory: Directory = { offsetStart: entries[0].offset, tileIdStart: entries[0].tileId, entries };
    this.ctx.waitUntil(this.kvCache.put(cacheKey, JSON.stringify(directory)));
    return directory;
  }

  async getJsonResponse(version: string, url: URL) {
    const header = this.getHeader();
    const metadata = await this.getMetadata();
    return tileJSON({ header, metadata, hostname: url.hostname, version });
  }

  async getTile(z: number, x: number, y: number): Promise<ReadableStream | undefined> {
    const tileId = zxyToTileId(z, x, y);
    const header = this.getHeader();
    const rootDirectory = this.getRootDirectory(header);

    if (z < header.minZoom || z > header.maxZoom) {
      return;
    }

    let offset = header.rootDirectoryOffset;
    let length = header.rootDirectoryLength;
    let entry = findTile(rootDirectory.entries, tileId);
    for (let i = 0; i < 2; ++i) {
      if (!entry) return;
      offset = entry.offset;
      length = entry.length;
      if (entry.runLength !== 0) break; // Run length of 0 is a directory, anything else is a tile
      const leafDirectory = await Metrics.getMetrics().monitorAsyncFunction(
        { name: 'get_leaf_directory' },
        (offset, length, header) => this.getDirectory(offset, length, header),
      )(header.leafDirectoryOffset + offset, length, header);
      entry = findTile(leafDirectory.entries, tileId);
    }
    const tile = await this.source.getAsStream({
      offset: header.tileDataOffset + offset,
      length,
    });
    return tile;
  }

  private async getMetadata(): Promise<Metadata> {
    const header = this.getHeader();

    const resp = await this.source.get({
      offset: header.jsonMetadataOffset,
      length: header.jsonMetadataLength,
    });
    const decompressed = await decompress(resp, header.internalCompression);
    const dec = new TextDecoder('utf-8');
    return JSON.parse(dec.decode(decompressed));
  }
}
