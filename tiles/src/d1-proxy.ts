/**
 * Write access to the tiles D1 databases for the tiles-deploy workflow, which runs outside Cloudflare.
 * Requests go to /<environment> with that environment's bearer token (terraform generates one per environment), so a
 * dev deploy can't touch prod's database.
 */

export type ProxyStatement = { sql: string; params?: unknown[] };
export type ProxyRequest = { statement: ProxyStatement } | { batch: ProxyStatement[] };
export type ProxyResponse = { results: Record<string, unknown>[][] } | { error: string };

type Environment = {
  database: (env: WorkerEnv) => D1Database | undefined;
  token: (env: WorkerEnv) => string | undefined;
};

const environments = new Map<string, Environment>([
  ['prod', { database: (env) => env.D1_PROD, token: (env) => env.D1_PROXY_TOKEN_PROD }],
  ['dev', { database: (env) => env.D1_DEV, token: (env) => env.D1_PROXY_TOKEN_DEV }],
]);

const json = (body: ProxyResponse, status: number) =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });

// Hashing both sides first gives equal-length inputs, so the comparison leaks neither content nor length.
async function tokenMatches(header: string | null, token: string): Promise<boolean> {
  const encoder = new TextEncoder();
  const [given, expected] = await Promise.all([
    crypto.subtle.digest('SHA-256', encoder.encode(header ?? '')),
    crypto.subtle.digest('SHA-256', encoder.encode(`Bearer ${token}`)),
  ]);
  return crypto.subtle.timingSafeEqual(given, expected);
}

function isStatement(value: unknown): value is ProxyStatement {
  const statement = value as ProxyStatement;
  return typeof statement?.sql === 'string' && (statement.params === undefined || Array.isArray(statement.params));
}

function parseRequest(body: unknown): ProxyRequest | undefined {
  const request = body as Partial<Record<'statement' | 'batch', unknown>>;
  if (isStatement(request?.statement)) {
    return { statement: request.statement };
  }
  if (Array.isArray(request?.batch) && request.batch.length > 0 && request.batch.every(isStatement)) {
    return { batch: request.batch };
  }
}

export async function handleD1ProxyRequest(request: Request, env: WorkerEnv): Promise<Response> {
  const target = environments.get(new URL(request.url).pathname.slice(1));
  const token = target?.token(env);
  if (!target || !token || !(await tokenMatches(request.headers.get('Authorization'), token))) {
    return new Response('Unauthorized', { status: 401 });
  }
  if (request.method.toUpperCase() !== 'POST') {
    return new Response('Method Not Allowed', { status: 405 });
  }

  const body = parseRequest(await request.json().catch(() => undefined));
  if (!body) {
    return json({ error: 'Expected { statement: { sql, params? } } or { batch: [{ sql, params? }] }' }, 400);
  }
  const database = target.database(env);
  if (!database) {
    return json({ error: 'No database bound for this environment' }, 500);
  }

  const prepare = ({ sql, params }: ProxyStatement) => database.prepare(sql).bind(...(params ?? []));
  try {
    // batch() runs its statements as one transaction, so a retried batch is all-or-nothing.
    const results =
      'batch' in body ? await database.batch(body.batch.map(prepare)) : [await prepare(body.statement).run()];
    return json({ results: results.map((result) => result.results as Record<string, unknown>[]) }, 200);
  } catch (error) {
    return json({ error: error instanceof Error ? error.message : String(error) }, 500);
  }
}
