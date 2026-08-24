import { SqlAdapter, type SqlDriver } from "./sql";
import { Mutex } from "./util";

/**
 * Minimal surface of `bun:sqlite`'s `Database`. Pass `new Database(path)` in.
 *
 *   import { Database } from "bun:sqlite"
 *   import { SqliteAdapter } from "bunmq"
 *   const adapter = new SqliteAdapter(new Database("bunmq.db"))
 */
export type SqliteDatabase = {
  exec(sql: string): unknown;
  query(sql: string): {
    all: (...params: unknown[]) => unknown[];
    get: (...params: unknown[]) => unknown;
    run: (...params: unknown[]) => unknown;
  };
  transaction<T>(fn: (...args: never[]) => T): (...args: never[]) => T;
};

/**
 * bun:sqlite transactions are synchronous. Claim/update paths are async, so
 * we serialize with a mutex and `BEGIN IMMEDIATE` instead of `db.transaction`.
 */
export class SqliteAdapter extends SqlAdapter {
  constructor(db: SqliteDatabase) {
    const mutex = new Mutex();
    const driver: SqlDriver = {
      dialect: "sqlite",
      exec: async (sql) => {
        db.exec(sql);
      },
      query: async <T = Record<string, unknown>>(sql: string, params: unknown[] = []) => {
        return db.query(sql).all(...params) as T[];
      },
      transaction: async (fn) =>
        mutex.run(async () => {
          db.exec("BEGIN IMMEDIATE");
          try {
            const out = await fn();
            db.exec("COMMIT");
            return out;
          } catch (err) {
            try {
              db.exec("ROLLBACK");
            } catch {
              /* ignore */
            }
            throw err;
          }
        }),
    };
    super(driver);
  }
}
