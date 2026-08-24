import { SqlAdapter, type SqlDriver } from "./sql";

/**
 * Any tagged-query or parameterized client. Works with `pg`, `postgres.js`,
 * Neon, and PGLite:
 *
 *   const adapter = new PostgresAdapter({
 *     query: async (sql, params) => {
 *       const { rows } = await client.query(sql, params)
 *       return rows
 *     },
 *     exec: async (sql) => { await client.query(sql) },
 *   })
 */
export type PostgresClient = {
  query: (sql: string, params?: unknown[]) => Promise<unknown[] | { rows: unknown[] }>;
  exec?: (sql: string) => Promise<unknown>;
};

function rowsOf(result: unknown[] | { rows: unknown[] }): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  return (result.rows ?? []) as Record<string, unknown>[];
}

export class PostgresAdapter extends SqlAdapter {
  constructor(client: PostgresClient) {
    const driver: SqlDriver = {
      dialect: "postgres",
      exec: async (sql) => {
        if (client.exec) await client.exec(sql);
        else await client.query(sql);
      },
      query: async <T = Record<string, unknown>>(sql: string, params: unknown[] = []) =>
        rowsOf(await client.query(sql, params)) as T[],
      transaction: async (fn) => {
        await client.query("BEGIN");
        try {
          const out = await fn();
          await client.query("COMMIT");
          return out;
        } catch (err) {
          await client.query("ROLLBACK");
          throw err;
        }
      },
    };
    super(driver);
  }
}
