import { IDeferredRepository, IKeyValueRepository, IMemCacheRepository, IStorageRepository } from '../interface';
import { Metrics } from '../monitor';
import { Directory, Entry, Header, Metadata } from './types';
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

class DirectoryStream {
  constructor(readonly stream: ReadableStream) {}
  static fromDirectory(directory: Directory): DirectoryStream {
    const stream = new ReadableStream({
      start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode(`${directory.tileIdStart}|${directory.offsetStart}:`));
        directory.entries.forEach((entry) => {
          controller.enqueue(
            encoder.encode(
              `${entry.tileId - directory.tileIdStart}|${entry.offset - directory.offsetStart}|${entry.length}|${entry.runLength}:`,
            ),
          );
        });
        controller.close();
      },
    });
    return new DirectoryStream(stream);
  }

  async findTile(searchedTileId: number): Promise<Entry | undefined> {
    let buffer = '';
    let offsetStart: number | undefined;
    let tileIdStart: number | undefined;

    const decoder = new TextDecoder();

    try {
      for await (const chunk of this.stream) {
        buffer += decoder.decode(chunk);
        // console.log(buffer);
        const lines = buffer.split(':');
        // Last line is always incomplete or empty, save it for next time
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          const parts = line.split('|');
          if (parts.length === 2) {
            tileIdStart = parseInt(parts[0], 10);
            offsetStart = parseInt(parts[1], 10);
            continue;
          }
          if (tileIdStart === undefined || offsetStart === undefined) {
            throw new Error('Invalid stream, we have no tileIdStart or offsetStart');
          }
          const tileId = parseInt(parts[0], 10) + tileIdStart;
          const runLength = parseInt(parts[3], 10);

          // Handle directories
          if (runLength === 0) {
            // As tileIds are always sequential, if we're in a directory and we pass the tileId, we can stop searching
            if (searchedTileId > tileId) {
              return {
                tileId,
                offset: parseInt(parts[1], 10) + offsetStart,
                length: parseInt(parts[2], 10),
                runLength,
              };
            }
          }

          // If these conditions pass, we have found the correct tile
          if (tileId === searchedTileId || (tileId < searchedTileId && tileId + runLength > searchedTileId)) {
            return {
              tileId,
              offset: parseInt(parts[1], 10) + offsetStart,
              length: parseInt(parts[2], 10),
              runLength,
            };
          }
        }
      }
    } finally {
      this.stream
        .cancel()
        .then(() => console.log('stream cancelled'))
        .catch((e) => console.error(e));
    }
  }
}

export class PMTilesService {
  private constructor(
    private source: IStorageRepository,
    private memCache: IMemCacheRepository,
    private kvCache: IKeyValueRepository,
    private deferredRepository: IDeferredRepository,
  ) {}

  static async init(
    source: IStorageRepository,
    memCache: IMemCacheRepository,
    kvCache: IKeyValueRepository,
    deferredRepository: IDeferredRepository,
  ): Promise<PMTilesService> {
    const p = new PMTilesService(source, memCache, kvCache, deferredRepository);
    const headerCacheKey = getHeaderCacheKey(source.getFileName());
    if (memCache.get(headerCacheKey)) {
      return p;
    }
    const [header, root] = await p.getHeaderAndRootFromSource();
    memCache.set(headerCacheKey, header);
    memCache.set(
      getDirectoryCacheKey(source.getFileName(), source.getFileHash(), {
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
    const key = getDirectoryCacheKey(this.source.getFileName(), this.source.getFileHash(), {
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

  private async getDirectory(offset: number, length: number, header: Header): Promise<DirectoryStream> {
    let timer = performance.now();
    const cacheKey = getDirectoryCacheKey(this.source.getFileName(), this.source.getFileHash(), { offset, length });
    const kvValueStream = await this.kvCache.getAsStream(cacheKey);
    if (kvValueStream) {
      console.log('time to get directory from kv', performance.now() - timer);
      return new DirectoryStream(kvValueStream);
    }
    console.log('time to check directory from kv', performance.now() - timer);
    timer = performance.now();
    const resp = await this.source.get({ offset, length });
    console.log('time to get directory from source', performance.now() - timer);
    const data = await decompress(resp, header.internalCompression);
    const entries = deserializeIndex(data);
    if (entries.length === 0) {
      throw new Error('Empty directory is invalid');
    }
    console.log('time to get directory from source and decompress and deserialise', performance.now() - timer);
    const directory: Directory = { offsetStart: entries[0].offset, tileIdStart: entries[0].tileId, entries };
    this.deferredRepository.defer(this.kvCache.putStream(cacheKey, DirectoryStream.fromDirectory(directory).stream));
    const dirStream = DirectoryStream.fromDirectory(directory);
    return dirStream;
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
    const rootDirectory = this.getRootDirectory(header);

    if (z < header.minZoom || z > header.maxZoom) {
      return;
    }

    let offset = header.rootDirectoryOffset;
    let length = header.rootDirectoryLength;
    let entry = findTile(rootDirectory.entries, tileId);
    for (let i = 0; i < 2; ++i) {
      if (!entry) {
        return;
      }
      offset = entry.offset;
      length = entry.length;
      // Run length of 0 is a directory, anything else is a tile
      if (entry.runLength !== 0) {
        break;
      }
      const leafDirectory = await Metrics.getMetrics().monitorAsyncFunction(
        { name: 'get_leaf_directory' },
        (offset, length, header) => this.getDirectory(offset, length, header),
      )(header.leafDirectoryOffset + offset, length, header);
      entry = await Metrics.getMetrics().monitorAsyncFunction({ name: 'find_tile_leaf_directory' }, (tileId) =>
        leafDirectory.findTile(tileId),
      )(tileId);
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
