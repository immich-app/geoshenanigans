import { Point } from '@influxdata/influxdb-client';
import { AsyncFn, Operation, Options } from './interface';

const startTimer = () => {
  const startTime = performance.now();
  return {
    elapsedMs: (): number => {
      return performance.now() - startTime;
    },
  };
};

export function monitorAsyncFunction<T extends AsyncFn>(
  operationPrefix: string,
  operation: Operation,
  call: T,
  metricsWriteCallback: (point: Point) => void,
  options: Options = {},
): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>> {
  const { name: operationName, tags } = operation;
  const { monitorInvocations = true, acceptedErrors = [] } = options;

  const point = new Point(`${operationPrefix}_${operationName}`);
  for (const [key, value] of Object.entries({ ...tags })) {
    point.tag(key, value);
  }
  return async (...args: Parameters<T>) => {
    if (monitorInvocations) {
      point.intField('invocation', 1);
    }
    const timer = startTimer();
    return call(...args)
      .catch((e) => {
        if (!acceptedErrors || !acceptedErrors.some((acceptedError) => e instanceof acceptedError)) {
          console.error(e, `${operationName}_errors`);
          point.intField('errors', 1);
        }
        throw e;
      })
      .finally(() => {
        point.intField('duration', timer.elapsedMs());
        metricsWriteCallback(point);
      });
  };
}
