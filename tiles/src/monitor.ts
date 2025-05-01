import { AsyncFn, Operation, Options } from './interface';
import { Metric } from './repository';

export function monitorAsyncFunction<T extends AsyncFn>(
  operationPrefix: string,
  operation: Operation,
  call: T,
  metricsWriteCallback: (metric: Metric) => void,
  options: Options = {},
): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>> {
  const { name: operationName, tags = {} } = operation;
  const { monitorInvocations = true, acceptedErrors = [] } = options;

  return async (...args: Parameters<T>) => {
    const metric = Metric.create(`${operationPrefix}_${operationName}`);
    metric.addTags(tags);

    if (monitorInvocations) {
      metric.intField('invocation', 1);
    }

    try {
      return await call(...args);
    } catch (e) {
      if (!acceptedErrors || !acceptedErrors.some((acceptedError) => e instanceof acceptedError)) {
        console.error(e, `${operationName}_errors`);
        metric.intField('errors', 1);
      }
      throw e;
    } finally {
      metric.durationField('duration');
      metricsWriteCallback(metric);
    }
  };
}
