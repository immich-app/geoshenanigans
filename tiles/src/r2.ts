export interface RangeResponse {
  data: ArrayBuffer;
  etag?: string;
  expires?: string;
  cacheControl?: string;
}

export class R2Source {
  env: Env;

  constructor(env: Env) {
    this.env = env;
  }

  getKey() {
    return `${this.env.PMTILES_FILE_NAME}`;
  }

  async getBytesFromArchive(args: { offset: number; length: number }): Promise<RangeResponse> {
    const { offset, length } = args;
    const resp = await this.env.BUCKET.get(this.env.PMTILES_FILE_NAME, {
      range: { offset: offset, length: length },
    });
    if (!resp) {
      throw new Error('Archive not found');
    }

    const o = resp as R2ObjectBody;

    const a = await o.arrayBuffer();
    return {
      data: a,
      etag: o.etag,
      cacheControl: o.httpMetadata?.cacheControl,
      expires: o.httpMetadata?.cacheExpiry?.toISOString(),
    };
  }
}
