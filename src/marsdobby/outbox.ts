/**
 * Durable outbox for the MarsDobby session ledger (see `ledger.ts`; wire
 * contract: MarsDobby `docs/botmux-ledger.md`).
 *
 * One SQLite file per bot under botmux's data dir. It holds three things, all
 * written in one transaction per enqueue so "reported" and "queued" never
 * diverge across a crash:
 *   • `outbox`   — events not yet acknowledged by the receiver, ordered by an
 *                  AUTOINCREMENT seq (monotonic for the life of the file).
 *   • `snapshot` — what the ledger last reported per session (for diffing).
 *   • `meta`     — stream id (random, per file) and first-enable time.
 *
 * Bounded: at `maxRows` the oldest `session.updated` rows are dropped to make
 * room (every event carries the full session state, so a dropped update loses
 * nothing a later one does not restate). Started/closed/turn events are never
 * dropped; when only those remain the enqueue is refused and logged loudly —
 * for session transitions the snapshot then stays stale and reconciliation
 * retries later.
 */
import { randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { openDatabaseSyncOrThrow, type DatabaseSyncLike } from '../services/sqlite-compat.js';

export interface SnapshotRow {
  sessionId: string;
  fingerprint: string;
  status: string;
  updateCount: number;
  reportedAt: number;
}

export interface QueuedEvent {
  seq: number;
  id: string;
  type: string;
  body: Record<string, unknown>;
}

export type EnqueueResult = 'queued' | 'duplicate' | 'full';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS outbox (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshot (
  session_id TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  status TEXT NOT NULL,
  update_count INTEGER NOT NULL DEFAULT 0,
  reported_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
`;

export class LedgerOutbox {
  private readonly db: DatabaseSyncLike;
  readonly streamId: string;
  readonly enabledAt: number;

  constructor(path: string, private readonly maxRows = 5000, now: number = Date.now()) {
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    this.db = openDatabaseSyncOrThrow(path);
    this.db.exec('PRAGMA busy_timeout = 2000; PRAGMA journal_mode = WAL;');
    this.db.exec(SCHEMA);
    this.streamId = this.meta('stream') ?? this.setMeta('stream', randomUUID());
    this.enabledAt = Number(this.meta('enabledAt') ?? this.setMeta('enabledAt', String(now)));
  }

  private meta(key: string): string | undefined {
    const row = this.db.prepare('SELECT value FROM meta WHERE key = ?').get(key) as { value: string } | undefined;
    return row?.value;
  }

  private setMeta(key: string, value: string): string {
    this.db.prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)').run(key, value);
    return value;
  }

  getSnapshot(sessionId: string): SnapshotRow | undefined {
    const row = this.db.prepare('SELECT session_id, fingerprint, status, update_count, reported_at FROM snapshot WHERE session_id = ?')
      .get(sessionId) as { session_id: string; fingerprint: string; status: string; update_count: number; reported_at: number } | undefined;
    return row && { sessionId: row.session_id, fingerprint: row.fingerprint, status: row.status,
      updateCount: Number(row.update_count), reportedAt: Number(row.reported_at) };
  }

  listSnapshotIds(): string[] {
    return (this.db.prepare('SELECT session_id FROM snapshot').all() as Array<{ session_id: string }>).map(r => r.session_id);
  }

  /**
   * Queue one event; when `snapshot` is given, record it in the same
   * transaction. `body` gets `seq` and `stream` stamped from the row.
   */
  enqueue(event: { id: string; type: string; body: Record<string, unknown> }, snapshot?: SnapshotRow, now = Date.now()): EnqueueResult {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      if (this.db.prepare('SELECT 1 FROM outbox WHERE id = ?').get(event.id)) {
        if (snapshot) this.writeSnapshot(snapshot);
        this.db.exec('COMMIT');
        return 'duplicate';
      }
      const count = Number((this.db.prepare('SELECT COUNT(*) AS n FROM outbox').get() as { n: number }).n);
      if (count >= this.maxRows) {
        const over = count - this.maxRows + 1;
        const dropped = Number(this.db.prepare(
          "DELETE FROM outbox WHERE seq IN (SELECT seq FROM outbox WHERE type = 'session.updated' ORDER BY seq LIMIT ?)",
        ).run(over).changes);
        if (dropped < over) {
          this.db.exec('ROLLBACK');
          return 'full';
        }
      }
      this.db.prepare('INSERT INTO outbox (id, type, body, created_at) VALUES (?, ?, ?, ?)')
        .run(event.id, event.type, JSON.stringify(event.body), now);
      if (snapshot) this.writeSnapshot(snapshot);
      this.db.exec('COMMIT');
      return 'queued';
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* already rolled back */ }
      throw err;
    }
  }

  private writeSnapshot(s: SnapshotRow): void {
    this.db.prepare('INSERT OR REPLACE INTO snapshot (session_id, fingerprint, status, update_count, reported_at) VALUES (?, ?, ?, ?, ?)')
      .run(s.sessionId, s.fingerprint, s.status, s.updateCount, s.reportedAt);
  }

  /** Forget closed sessions reported long ago (they re-enter as new if reopened). */
  pruneSnapshots(olderThan: number): void {
    this.db.prepare("DELETE FROM snapshot WHERE status = 'closed' AND reported_at < ?").run(olderThan);
  }

  peek(limit: number): QueuedEvent[] {
    const rows = this.db.prepare('SELECT seq, id, type, body FROM outbox ORDER BY seq LIMIT ?').all(limit) as
      Array<{ seq: number; id: string; type: string; body: string }>;
    return rows.map(r => ({
      seq: Number(r.seq), id: r.id, type: r.type,
      body: { ...JSON.parse(r.body), id: r.id, type: r.type, seq: Number(r.seq), stream: this.streamId },
    }));
  }

  ack(seqs: number[]): void {
    if (seqs.length === 0) return;
    const stmt = this.db.prepare('DELETE FROM outbox WHERE seq = ?');
    this.db.exec('BEGIN IMMEDIATE');
    try {
      for (const seq of seqs) stmt.run(seq);
      this.db.exec('COMMIT');
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* already rolled back */ }
      throw err;
    }
  }

  size(): number {
    return Number((this.db.prepare('SELECT COUNT(*) AS n FROM outbox').get() as { n: number }).n);
  }

  close(): void {
    try { this.db.close(); } catch { /* already closed */ }
  }
}
