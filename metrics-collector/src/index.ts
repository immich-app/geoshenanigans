// /home/zack/Source/immich/geoshenanigans/metrics-collector/src/index.ts
import { Router } from 'itty-router';

interface Env {
  ACCOUNT_ID: string;
  API_TOKEN: string;
  // If you have other bindings, like KV, D1, R2, define them here.
}

const router = Router();

const fetchCloudflareMetrics = async (env: Env) => {
  const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
  const query = `
    query GetAnalytics(
      $accountTag: string,
      $filter: AccountWorkersInvocationsAdaptiveFilter_InputObject,
      $queueOperationsFilter: AccountQueueMessageOperationsAdaptiveGroupsFilter_InputObject,
      $queueBacklogFilter: AccountQueueBacklogAdaptiveGroupsFilter_InputObject
    ) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          workersInvocationsAdaptive(
            limit: 10000
            filter: $filter
          ) {
            sum {
              requests
              errors
              subrequests
            }
            quantiles {
              cpuTimeP50
              cpuTimeP75
              cpuTimeP90
              cpuTimeP99
            }
            dimensions {
              scriptName
              status
            }
          }
          queueMessageOperationsAdaptiveGroups(
            limit: 10000
            filter: $queueOperationsFilter
          ) {
            sum {
              billableOperations
              bytes
            }
            dimensions {
              queue
              operationType
            }
          }
          queueBacklogAdaptiveGroups(
            limit: 10000
            filter: $queueBacklogFilter
          ) {
            sum {
              messages
            }
            dimensions {
              queue
            }
          }
          workersQueueAdaptiveGroups(
            limit: 10000
            filter: $filter
          ) {
            sum {
              messagesSent
              messagesFailed
              messagesPulled
              messagesDeleted
              messagesPurged
              eventDeliveryAttempts
              eventDeliveryFailures
            }
            dimensions {
              queue
              scriptName
            }
          }
        }
      }
    }
  `;

  const variables = {
    accountTag: env.ACCOUNT_ID,
    filter: {
      // Filter for workersInvocationsAdaptive
      datetime_geq: fiveMinutesAgo,
    },
    queueOperationsFilter: {
      // Renamed filter for workersQueueOperationsAdaptive
      datetime_geq: fiveMinutesAgo,
      // Add other specific queue filters here if needed, e.g. queueName_in: ["my-queue"]
    },
    queueBacklogFilter: {
      // Filter for workersQueueBacklogAdaptiveGroups
      datetime_geq: fiveMinutesAgo, // Assuming datetime filter is applicable
      // Add other specific backlog filters if needed
    },
  };

  const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${env.API_TOKEN}`,
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error(
      `Error fetching metrics: ${response.status} ${response.statusText}`,
      errorText,
    );
    throw new Error(
      `Failed to fetch metrics from Cloudflare API: ${errorText}`,
    );
  }

  const { data, errors } = (await response.json()) as {
    data: any;
    errors?: any[];
  };

  if (errors && errors.length > 0) {
    console.error('GraphQL API errors:', JSON.stringify(errors, null, 2));
    throw new Error(`GraphQL API returned errors: ${JSON.stringify(errors)}`);
  }

  return data;
};

const formatMetricsForPrometheus = (data: any): string => {
  let metricsString = '';

  // Worker Invocation Metrics
  const workerInvocations =
    data?.viewer?.accounts?.[0]?.workersInvocationsAdaptive || [];
  workerInvocations.forEach((item: any) => {
    const scriptName = item.dimensions.scriptName || 'unknown_script';
    metricsString += `cloudflare_worker_requests_total{worker="${scriptName}"} ${item.sum.requests || 0}\n`;
    metricsString += `cloudflare_worker_errors_total{worker="${scriptName}"} ${item.sum.errors || 0}\n`;
    metricsString += `cloudflare_worker_subrequests_total{worker="${scriptName}"} ${item.sum.subrequests || 0}\n`;

    if (item.quantiles) {
      metricsString += `cloudflare_worker_cpu_time_p50_ms{worker="${scriptName}"} ${item.quantiles.cpuTimeP50 || 0}\n`;
      metricsString += `cloudflare_worker_cpu_time_p75_ms{worker="${scriptName}"} ${item.quantiles.cpuTimeP75 || 0}\n`;
      metricsString += `cloudflare_worker_cpu_time_p90_ms{worker="${scriptName}"} ${item.quantiles.cpuTimeP90 || 0}\n`;
      metricsString += `cloudflare_worker_cpu_time_p99_ms{worker="${scriptName}"} ${item.quantiles.cpuTimeP99 || 0}\n`;
    }
  });

  // Worker Queue Metrics - Operations
  const queueOperations =
    data?.viewer?.accounts?.[0]?.queueMessageOperationsAdaptiveGroups || [];
  queueOperations.forEach((item: any) => {
    const queueName = item.dimensions?.queue || 'unknown_queue';
    const operationType = item.dimensions?.operationType || 'unknown_operation';
    metricsString += `cloudflare_queue_billable_operations_total{queue="${queueName}",operation="${operationType}"} ${item.sum.billableOperations || 0}\n`;
    metricsString += `cloudflare_queue_bytes_total{queue="${queueName}",operation="${operationType}"} ${item.sum.bytes || 0}\n`;
  });

  // Worker Queue Metrics - Backlog
  const queueBacklog =
    data?.viewer?.accounts?.[0]?.queueBacklogAdaptiveGroups || [];
  queueBacklog.forEach((item: any) => {
    const queueName = item.dimensions?.queue || 'unknown_queue';
    metricsString += `cloudflare_queue_backlog_messages{queue="${queueName}"} ${item.sum.messages || 0}\n`;
  });

  // Worker Queue Metrics - Detailed
  const workersQueue =
    data?.viewer?.accounts?.[0]?.workersQueueAdaptiveGroups || [];
  workersQueue.forEach((item: any) => {
    const queueName = item.dimensions?.queue || 'unknown_queue';
    const scriptName = item.dimensions?.scriptName || 'unknown_script';
    metricsString += `cloudflare_queue_messages_sent_total{queue="${queueName}",worker="${scriptName}"} ${item.sum.messagesSent || 0}\n`;
    metricsString += `cloudflare_queue_messages_failed_total{queue="${queueName}",worker="${scriptName}"} ${item.sum.messagesFailed || 0}\n`;
    metricsString += `cloudflare_queue_messages_pulled_total{queue="${queueName}",worker="${scriptName}"} ${item.sum.messagesPulled || 0}\n`;
    metricsString += `cloudflare_queue_messages_deleted_total{queue="${queueName}",worker="${scriptName}"} ${item.sum.messagesDeleted || 0}\n`;
    metricsString += `cloudflare_queue_messages_purged_total{queue="${queueName}",worker="${scriptName}"} ${item.sum.messagesPurged || 0}\n`;
    metricsString += `cloudflare_queue_event_delivery_attempts_total{queue="${queueName}",worker="${scriptName}"} ${item.sum.eventDeliveryAttempts || 0}\n`;
    metricsString += `cloudflare_queue_event_delivery_failures_total{queue="${queueName}",worker="${scriptName}"} ${item.sum.eventDeliveryFailures || 0}\n`;
  });

  return metricsString;
};

router.get(
  '/metrics',
  async (request: Request, env: Env, ctx: ExecutionContext) => {
    try {
      const cfData = await fetchCloudflareMetrics(env);
      const promMetrics = formatMetricsForPrometheus(cfData);
      return new Response(promMetrics, {
        headers: { 'Content-Type': 'text/plain; version=0.0.4' },
      });
    } catch (e: any) {
      console.error('Error in /metrics endpoint:', e.message, e.stack);
      return new Response(
        `Error fetching or formatting metrics: ${e.message}`,
        { status: 500 },
      );
    }
  },
);

// Catch-all for other requests
router.all('*', () => new Response('Not Found.', { status: 404 }));

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    return router.handle(request, env, ctx);
  },
  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    console.log(`Cron triggered: ${controller.cron}`);
    try {
      const cfData = await fetchCloudflareMetrics(env);
      // In a real scenario, you might do something with this data,
      // like storing it or checking its validity.
      // For now, we just log success or failure.
      console.log(
        'Successfully fetched metrics in scheduled event.',
        JSON.stringify(cfData, null, 2).substring(0, 500) + '...',
      );
    } catch (e: any) {
      console.error(
        'Error in scheduled event fetching metrics:',
        e.message,
        e.stack,
      );
    }
  },
};
