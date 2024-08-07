// eslint-disable-next-line @typescript-eslint/no-explicit-any

import { Point } from '@influxdata/influxdb-client';

type AsyncFn = (...args: any[]) => Promise<any>;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Class = { new (...args: any[]): any };

type Options = Partial<{
  monitorInvocations?: boolean;
  acceptedErrors?: Class[];
}>;

const startTimer = () => {
  const startTime = Date.now();
  return {
    elapsedMs: (): number => {
      return Date.now() - startTime;
    },
  };
};

export class Metrics {
  private static _instance: Metrics;
  private readonly ctx: ExecutionContext;
  private readonly request: Request;
  private readonly env: Env;
  private readonly defaultTags: { [key: string]: string };
  private readonly operationPrefix: string;

  private constructor(
    operationPrefix: string,
    request: Request<unknown, IncomingRequestCfProperties>,
    ctx: ExecutionContext,
    env: Env,
  ) {
    this.request = request;
    this.ctx = ctx;
    this.env = env;
    this.defaultTags = {
      colo: request.cf?.colo ?? '',
      rayId: request.headers.get('cf-ray') ?? '',
      asOrg: request.cf?.asOrganization ?? '',
      scriptTag: env.CF_VERSION_METADATA.tag,
      scriptId: env.CF_VERSION_METADATA.id,
    };
    this.operationPrefix = operationPrefix;
  }

  public static initialiseMetrics(
    operationPrefix: string,
    request: Request<unknown, IncomingRequestCfProperties>,
    ctx: ExecutionContext,
    env: Env,
  ) {
    this._instance = new Metrics(operationPrefix, request, ctx, env);
    return this._instance;
  }

  static getMetrics() {
    return this._instance;
  }

  monitorAsyncFunction<T extends AsyncFn>(
    operation: { name: string; extraTags?: { [key: string]: string } },
    call: T,
    options: Options = {},
  ): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>> {
    const { name: operationName, extraTags = {} } = operation;
    const { monitorInvocations = true, acceptedErrors = [] } = options;

    const point = new Point(`${this.operationPrefix}_${operationName}`);
    for (const [key, value] of Object.entries({ ...this.defaultTags, ...extraTags })) {
      point.tag(key, value);
    }
    return async (...args: Parameters<T>) => {
      if (monitorInvocations) {
        console.log(`${operationName}_invocation`);
        point.intField('invocation', 1);
      }
      const timer = startTimer();
      return call(...args)
        .catch((e) => {
          if (!acceptedErrors || !acceptedErrors.some((acceptedError) => e instanceof acceptedError)) {
            console.log(`${operationName}_errors`);
            point.intField('errors', 1);
          }
          throw e;
        })
        .finally(() => {
          console.log(`${operationName}_duration`, timer.elapsedMs());
          point.intField('duration', timer.elapsedMs());
          console.log(point.toLineProtocol()?.toString());
          this.ctx.waitUntil(
            fetch('https://cf-workers.monitoring.immich.cloud/write', {
              method: 'POST',
              body: point.toLineProtocol()?.toString(),
              headers: {
                Authorization: `Token `,
              },
            }),
          );
        });
    };
  }
}
