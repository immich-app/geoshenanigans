import { monitorAsyncFunction } from '../monitor';
import { R2Source, RangeResponse } from '../r2';
import { Directory, Header, Metadata } from './types';
import { bytesToHeader, decompress, deserializeIndex, findTile, zxyToTileId } from './utils';

const HEADER_SIZE_BYTES = 127;

export class PMTiles {
  source: R2Source;
  memCache: Map<string, Header | Directory>;
  kvCache: KVNamespace;
  ctx: ExecutionContext;

  private constructor(
    source: R2Source,
    memCache: Map<string, Header | Directory>,
    kvCache: KVNamespace,
    ctx: ExecutionContext,
  ) {
    this.source = source;
    this.memCache = memCache;
    this.kvCache = kvCache;
    this.ctx = ctx;
  }

  static async init(
    source: R2Source,
    memCache: Map<string, Header | Directory>,
    kvCache: KVNamespace,
    ctx: ExecutionContext,
  ): Promise<PMTiles> {
    const p = new PMTiles(source, memCache, kvCache, ctx);
    if (memCache.get(source.getKey())) {
      return p;
    }
    const [header, root] = await p.getHeaderAndRootFromSource();
    memCache.set(source.getKey(), header);
    memCache.set(`${source.getKey()}|${header.rootDirectoryOffset}|${header.rootDirectoryLength}`, root);
    return p;
  }

  async getHeader(): Promise<Header> {
    const key = this.source.getKey();
    const memCached = this.memCache.get(key);
    if (!memCached) {
      throw new Error('Header not found in cache');
    }
    return memCached as Header;
  }

  async getRootDirectory(header: Header): Promise<Directory> {
    const key = `${this.source.getKey()}|${header.rootDirectoryOffset}|${header.rootDirectoryLength}`;
    const root = this.memCache.get(key);
    if (!root) {
      throw new Error('Root directory not found in cache');
    }
    return root as Directory;
  }

  private async getHeaderAndRootFromSource(): Promise<[Header, Directory]> {
    const resp = await this.source.getBytesFromArchive({ offset: 0, length: 16384 });
    const v = new DataView(resp.data);
    if (v.getUint16(0, true) !== 0x4d50) {
      throw new Error('Wrong magic number for PMTiles archive');
    }

    const headerData = resp.data.slice(0, HEADER_SIZE_BYTES);
    const header = await bytesToHeader(headerData);
    const rootDirData = resp.data.slice(
      header.rootDirectoryOffset,
      header.rootDirectoryOffset + header.rootDirectoryLength,
    );
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

  async getDirectory(offset: number, length: number, header: Header): Promise<Directory> {
    const cacheKey = `${this.source.getKey()}|${offset}|${length}`;
    const kvValue = await this.kvCache.get(cacheKey, { type: 'json', cacheTtl: 2629800 });
    if (kvValue) {
      const directory = kvValue as Directory;
      return directory;
    }
    const resp = await this.source.getBytesFromArchive({ offset, length });
    const data = await decompress(resp.data, header.internalCompression);
    const entries = deserializeIndex(data);
    if (entries.length === 0) {
      throw new Error('Empty directory is invalid');
    }
    const directory: Directory = { offsetStart: entries[0].offset, tileIdStart: entries[0].tileId, entries };
    this.ctx.waitUntil(this.kvCache.put(cacheKey, JSON.stringify(directory)));
    return directory;
  }

  async getTile(z: number, x: number, y: number): Promise<RangeResponse | undefined> {
    const tileId = zxyToTileId(z, x, y);
    const header = await this.getHeader();
    const rootDirectory = await this.getRootDirectory(header);

    if (z < header.minZoom || z > header.maxZoom) {
      return;
    }

    const leafDirectoryEntry = findTile(rootDirectory.entries, tileId);
    if (!leafDirectoryEntry) {
      return;
    }
    const directoryOffset = header.leafDirectoryOffset + leafDirectoryEntry?.offset;
    const directoryLength = leafDirectoryEntry.length;
    const leafDirectory = await monitorAsyncFunction('get-leaf-directory', this.getDirectory, { thisArg: this })(
      directoryOffset,
      directoryLength,
      header,
    );
    const tileEntry = findTile(leafDirectory.entries, tileId);
    if (!tileEntry || tileEntry.runLength === 0) {
      return;
    }
    const tile = await this.source.getBytesFromArchive({
      offset: header.tileDataOffset + tileEntry.offset,
      length: tileEntry.length,
    });
    return tile;
  }

  async getMetadata(): Promise<Metadata> {
    const header = await this.getHeader();

    const resp = await this.source.getBytesFromArchive({
      offset: header.jsonMetadataOffset,
      length: header.jsonMetadataLength,
    });
    const decompressed = await decompress(resp.data, header.internalCompression);
    const dec = new TextDecoder('utf-8');
    return JSON.parse(dec.decode(decompressed));
  }
}
