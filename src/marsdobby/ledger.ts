/**
 * MarsDobby session ledger — reports this bot's sessions and turns to a
 * MarsDobby server so it can list them per project. Wire contract (event
 * names, fields, auth, response): MarsDobby `docs/botmux-ledger.md`.
 *
 * Off unless `BOTMUX_MARSDOBBY_LEDGER_URL` is set: then nothing is subscribed,
 * no file is created and no timer runs. Never injects anything into a prompt.
 *
 * Sources, all in-process and read-only:
 *   • dashboard event bus — `session.*` events only mark a session dirty; the
 *     session store row is the authority and is diffed against the last
 *     reported snapshot. `turn.finished` gives status + asker; a later
 *     `session.update` carrying `patch.lastTurn` gives start time and the first
 *     line of the answer.
 *   • worker-pool `onTurnResult` — full answer text and duration, only for bots
 *     with `turnResults.enabled` in their config.
 *   • a reconciliation pass over the session store (startup + every minute)
 *     catches transitions no event announced (e.g. closing a suspended session,
 *     anything during a daemon restart).
 *
 * Every path is fire-and-forget: the bus callback only records ids, SQLite and
 * HTTP work run on timers, and every error is caught and logged.
 */
import { createHash } from 'node:crypto';
import { lstatSync, readFileSync } from 'node:fs';
import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { join } from 'node:path';
import { config } from '../config.js';
import { dashboardEventBus, type DashboardEvent, type DashboardEventBus } from '../core/dashboard-events.js';
import * as sessionStore from '../services/session-store.js';
import type { TurnResult } from '../services/turn-result.js';
import type { Session } from '../types.js';
import { logger } from '../utils/logger.js';
import { LedgerOutbox, type SnapshotRow } from './outbox.js';

export type LedgerTextMode = 'full' | 'none';

export interface LedgerConfig {
  url: URL;
  token: string;
  text: LedgerTextMode;
}

const ANSWER_MAX_CHARS = 32_000;
const HISTORIC_GRACE_MS = 24 * 60 * 60 * 1000;
const SNAPSHOT_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * Read the ledger configuration from the environment. `undefined` = disabled.
 * Throws (with a reason) when the ledger is requested but unsafe to start.
 */
export function readLedgerConfig(env: NodeJS.ProcessEnv = process.env): LedgerConfig | undefined {
  const rawUrl = env.BOTMUX_MARSDOBBY_LEDGER_URL?.trim();
  if (!rawUrl) return undefined;
  let url: URL;
  try { url = new URL(rawUrl); } catch { throw new Error('ledger URL is not a valid URL'); }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new Error('ledger URL must be http or https');
  if (url.username || url.password) throw new Error('ledger URL must not carry credentials');
  const text = (env.BOTMUX_MARSDOBBY_LEDGER_TEXT?.trim() || 'full') as LedgerTextMode;
  if (text !== 'full' && text !== 'none') throw new Error('BOTMUX_MARSDOBBY_LEDGER_TEXT must be full or none');
  const tokenFile = env.BOTMUX_MARSDOBBY_LEDGER_TOKEN_FILE?.trim();
  if (!tokenFile) throw new Error('BOTMUX_MARSDOBBY_LEDGER_TOKEN_FILE is required');
  return { url, token: readTokenFile(tokenFile), text };
}

/** A bearer token file must be a regular, owner-only file of the daemon user. */
export function readTokenFile(path: string): string {
  const stat = lstatSync(path);
  if (stat.isSymbolicLink() || !stat.isFile()) throw new Error('token file must be a regular file, not a symlink');
  if ((stat.mode & 0o077) !== 0) throw new Error('token file must not be group/world accessible (chmod 600)');
  if (process.getuid && stat.uid !== process.getuid()) throw new Error('token file must be owned by the daemon user');
  if (stat.size > 4096) throw new Error('token file is too large');
  const token = readFileSync(path, 'utf8').trim();
  if (!token || /\s/.test(token)) throw new Error('token file must hold one non-empty token');
  return token;
}

export function ledgerEventId(...parts: Array<string | number | undefined>): string {
  return createHash('sha256').update(parts.map(p => String(p ?? '')).join('\0')).digest('hex').slice(0, 32);
}

export type LedgerSessionStatus = 'active' | 'suspended' | 'closed';

export interface LedgerSessionState {
  chatId: string;
  chatType?: string;
  rootMessageId: string;
  scope?: string;
  cliId?: string;
  cliSessionId?: string;
  workingDir?: string;
  title?: string;
  status: LedgerSessionStatus;
  createdAt?: string;
  closedAt?: string;
  lastMessageAt?: string;
  ownerOpenId?: string;
  creatorOpenId?: string;
}

export function sessionStatus(session: Pick<Session, 'status' | 'suspendedColdResume'>): LedgerSessionStatus {
  if (session.status === 'closed') return 'closed';
  return session.suspendedColdResume ? 'suspended' : 'active';
}

export function sessionState(session: Session, fallbackCliId?: string): LedgerSessionState {
  const out: LedgerSessionState = {
    chatId: session.chatId,
    rootMessageId: session.rootMessageId,
    status: sessionStatus(session),
  };
  const optional: Array<[keyof LedgerSessionState, unknown]> = [
    ['chatType', session.chatType], ['scope', session.scope ?? 'thread'], ['cliId', session.cliId ?? fallbackCliId],
    ['cliSessionId', session.cliSessionId], ['workingDir', session.workingDir], ['title', session.title],
    ['createdAt', session.createdAt], ['closedAt', session.status === 'closed' ? session.closedAt : undefined],
    ['lastMessageAt', session.lastMessageAt], ['ownerOpenId', session.ownerOpenId], ['creatorOpenId', session.creatorOpenId],
  ];
  for (const [key, value] of optional) if (typeof value === 'string' && value) (out as any)[key] = value;
  return out;
}

/** Fields whose change is worth a `session.updated`. Activity timestamps are not. */
export function stateFingerprint(state: LedgerSessionState): string {
  return JSON.stringify([state.status, state.workingDir ?? '', state.cliSessionId ?? '', state.cliId ?? '', state.title ?? '']);
}

export interface LedgerStoreView {
  getSession(sessionId: string): Session | undefined;
  /** Must throw rather than return a partial list when the store is unhealthy. */
  listSessions(): Session[];
}

export type LedgerPost = (body: string) => Promise<number>;

export interface LedgerDeps {
  larkAppId: string;
  cliId?: string;
  text: LedgerTextMode;
  outbox: LedgerOutbox;
  store: LedgerStoreView;
  bus: DashboardEventBus;
  post: LedgerPost;
  now?: () => number;
  flushDelayMs?: number;
  reconcileIntervalMs?: number;
  firstReconcileDelayMs?: number;
  batchSize?: number;
  maxBackoffMs?: number;
}

interface TurnPart {
  sessionId: string;
  turnId: string;
  part: string;
  turn: Record<string, unknown>;
  occurredAt: number;
}

function capText(text: string): { text: string; truncated?: true } {
  return text.length > ANSWER_MAX_CHARS ? { text: text.slice(0, ANSWER_MAX_CHARS), truncated: true } : { text };
}

function finiteMs(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : undefined;
}

export class MarsDobbyLedger {
  private readonly dirty = new Set<string>();
  private readonly turnParts: TurnPart[] = [];
  private readonly timers = new Set<ReturnType<typeof setTimeout>>();
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private reconcileTimer: ReturnType<typeof setInterval> | undefined;
  private unsubscribe: (() => void) | undefined;
  private sending = false;
  private stopped = false;
  private backoffMs = 0;
  private batchSize: number;
  private refusing = false;
  private readonly now: () => number;

  constructor(private readonly deps: LedgerDeps) {
    this.now = deps.now ?? Date.now;
    this.batchSize = deps.batchSize ?? 100;
  }

  start(): this {
    this.unsubscribe = this.deps.bus.subscribe(event => this.onBusEvent(event));
    const first = setTimeout(() => { this.timers.delete(first); this.reconcile(); }, this.deps.firstReconcileDelayMs ?? 5_000);
    first.unref?.();
    this.timers.add(first);
    this.reconcileTimer = setInterval(() => this.reconcile(), this.deps.reconcileIntervalMs ?? 60_000);
    this.reconcileTimer.unref?.();
    this.kick();
    return this;
  }

  stop(): void {
    this.stopped = true;
    this.unsubscribe?.();
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
    if (this.flushTimer) clearTimeout(this.flushTimer);
    if (this.retryTimer) clearTimeout(this.retryTimer);
    if (this.reconcileTimer) clearInterval(this.reconcileTimer);
  }

  /** Bus subscriber: record ids only, never touch disk or network here. */
  onBusEvent(event: DashboardEvent): void {
    if (this.stopped) return;
    try {
      if (event.type === 'session.spawned') {
        const id = event.body.session?.sessionId;
        if (typeof id === 'string') this.markDirty(id);
      } else if (event.type === 'session.update' || event.type === 'session.exited') {
        this.markDirty(event.body.sessionId);
        if (event.type === 'session.update' && event.body.patch?.lastTurn) this.noteLastTurn(event.body.sessionId, event.body.patch.lastTurn);
      } else if (event.type === 'turn.finished') {
        const b = event.body;
        if (b.larkAppId && b.larkAppId !== this.deps.larkAppId) return;
        this.addTurnPart({
          sessionId: b.sessionId, turnId: b.turnId, part: `terminal:${b.dispatchAttempt ?? ''}`, occurredAt: this.now(),
          turn: {
            status: b.status, terminalStatus: b.terminalStatus, finishedAt: new Date(this.now()).toISOString(),
            ...(b.dispatchAttempt !== undefined ? { dispatchAttempt: b.dispatchAttempt } : {}),
            ...(b.senderOpenId ? { askerOpenId: b.senderOpenId } : {}),
          },
        });
      }
    } catch (err) {
      logger.warn(`[marsdobby-ledger] event ignored: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /** worker-pool `onTurnResult` (bots with `turnResults.enabled`). */
  onTurnResult = (result: Readonly<TurnResult>): void => {
    if (this.stopped) return;
    try {
      if (result.ref.botId !== this.deps.larkAppId) return;
      const ex = result.execution;
      const turn: Record<string, unknown> = {
        status: ex.status === 'completed' ? 'success' : 'error',
        terminalStatus: ex.status,
        ...(ex.errorCode ? { errorCode: ex.errorCode } : {}),
        ...(ex.completedAt ? { finishedAt: ex.completedAt } : {}),
        ...(Number.isSafeInteger(ex.durationMs) ? { durationMs: ex.durationMs } : {}),
        ...(result.ref.dispatchAttempt !== undefined ? { dispatchAttempt: result.ref.dispatchAttempt } : {}),
      };
      if (this.deps.text === 'full' && result.output.status === 'available' && typeof result.output.content === 'string') {
        const capped = capText(result.output.content);
        turn.answer = { ...capped, source: result.output.source ?? 'unknown', format: result.output.format ?? 'text' };
      }
      this.addTurnPart({
        sessionId: result.ref.sessionId, turnId: result.ref.turnId, occurredAt: this.now(), turn,
        part: `result:${result.ref.dispatchAttempt ?? ''}:${result.resultRevision ?? 0}`,
      });
    } catch (err) {
      logger.warn(`[marsdobby-ledger] turn result ignored: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  private noteLastTurn(sessionId: string, raw: Record<string, unknown>): void {
    const turnId = typeof raw.turnId === 'string' ? raw.turnId : '';
    const at = finiteMs(raw.at);
    if (!turnId || !at || turnId.startsWith('auto-compact:')) return;
    const startedAt = finiteMs(raw.startedAt);
    const turn: Record<string, unknown> = { outcome: raw.outcome, finishedAt: new Date(at).toISOString() };
    if (startedAt && startedAt <= at) {
      turn.startedAt = new Date(startedAt).toISOString();
      turn.durationMs = at - startedAt;
    }
    if (typeof raw.errorCode === 'string') turn.errorCode = raw.errorCode;
    if (this.deps.text === 'full' && typeof raw.summary === 'string' && raw.summary) turn.answerExcerpt = raw.summary;
    this.addTurnPart({ sessionId, turnId, part: `outcome:${at}`, occurredAt: this.now(), turn });
  }

  private addTurnPart(part: TurnPart): void {
    this.turnParts.push(part);
    this.scheduleFlush();
  }

  private markDirty(sessionId: string): void {
    this.dirty.add(sessionId);
    this.scheduleFlush();
  }

  private scheduleFlush(): void {
    if (this.flushTimer || this.stopped) return;
    this.flushTimer = setTimeout(() => { this.flushTimer = undefined; this.flush(); }, this.deps.flushDelayMs ?? 250);
    this.flushTimer.unref?.();
  }

  /** Drain dirty sessions, then turn parts (so a session is started before its turns). */
  flush(): void {
    const ids = [...this.dirty];
    this.dirty.clear();
    for (const id of ids) {
      try {
        const session = this.deps.store.getSession(id);
        if (session) this.reportSession(session);
      } catch (err) {
        logger.warn(`[marsdobby-ledger] session ${id.slice(0, 8)} not reported: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    const parts = this.turnParts.splice(0);
    for (const p of parts) {
      try { this.reportTurn(p); } catch (err) {
        logger.warn(`[marsdobby-ledger] turn ${p.turnId} not reported: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    this.kick();
  }

  private envelope(sessionId: string, occurredAt: number, extra: Record<string, unknown>): Record<string, unknown> {
    return { occurredAt: new Date(occurredAt).toISOString(), botAppId: this.deps.larkAppId, sessionId, ...extra };
  }

  private enqueue(id: string, type: string, body: Record<string, unknown>, snapshot?: SnapshotRow): boolean {
    const result = this.deps.outbox.enqueue({ id, type, body }, snapshot, this.now());
    if (result === 'full') {
      if (!this.refusing) logger.error(`[marsdobby-ledger] OUTBOX FULL — refusing ${type} events until the receiver drains it`);
      this.refusing = true;
      return false;
    }
    if (this.refusing) logger.warn('[marsdobby-ledger] outbox accepting events again');
    this.refusing = false;
    return true;
  }

  /** Diff one store row against the last reported snapshot and queue what changed. */
  reportSession(session: Session): void {
    const sid = session.sessionId;
    const state = sessionState(session, this.deps.cliId);
    const fp = stateFingerprint(state);
    const snap = this.deps.outbox.getSnapshot(sid);
    const now = this.now();
    const snapshot = (status: string, updateCount: number): SnapshotRow =>
      ({ sessionId: sid, fingerprint: fp, status, updateCount, reportedAt: now });
    if (!snap) {
      if (state.status === 'closed') {
        const closedMs = Date.parse(state.closedAt ?? state.lastMessageAt ?? state.createdAt ?? '');
        // Closed before the ledger existed: history, not news.
        if (!Number.isFinite(closedMs) || closedMs < this.deps.outbox.enabledAt - HISTORIC_GRACE_MS) return;
      }
      const startedBody = this.envelope(sid, now, { session: { ...state } });
      if (state.status !== 'closed') {
        this.enqueue(ledgerEventId(this.deps.larkAppId, sid, 'started'), 'session.started', startedBody, snapshot(state.status, 0));
        return;
      }
      if (!this.enqueue(ledgerEventId(this.deps.larkAppId, sid, 'started'), 'session.started', startedBody)) return;
      this.enqueue(ledgerEventId(this.deps.larkAppId, sid, 'closed', state.closedAt), 'session.closed',
        this.envelope(sid, now, { session: { ...state } }), snapshot('closed', 0));
      return;
    }
    if (snap.fingerprint === fp) return;
    if (state.status === 'closed' && snap.status !== 'closed') {
      this.enqueue(ledgerEventId(this.deps.larkAppId, sid, 'closed', state.closedAt ?? `n${snap.updateCount}`), 'session.closed',
        this.envelope(sid, now, { session: { ...state } }), snapshot('closed', snap.updateCount));
      return;
    }
    const n = snap.updateCount + 1;
    this.enqueue(ledgerEventId(this.deps.larkAppId, sid, 'updated', n), 'session.updated',
      this.envelope(sid, now, { session: { ...state }, previousStatus: snap.status }), snapshot(state.status, n));
  }

  private reportTurn(p: TurnPart): void {
    let session: Session | undefined;
    try { session = this.deps.store.getSession(p.sessionId); } catch { session = undefined; }
    if (session) this.reportSession(session);
    const body = this.envelope(p.sessionId, p.occurredAt, {
      ...(session ? { session: sessionState(session, this.deps.cliId) } : {}),
      turn: { turnId: p.turnId, ...p.turn },
    });
    this.enqueue(ledgerEventId(this.deps.larkAppId, p.sessionId, 'turn', p.turnId, p.part), 'turn.finished', body);
  }

  /** Compare the whole store with the snapshot; reports what events missed. */
  reconcile(): void {
    if (this.stopped) return;
    try {
      const sessions = this.deps.store.listSessions();
      const seen = new Set<string>();
      for (const s of sessions) {
        seen.add(s.sessionId);
        try { this.reportSession(s); } catch (err) {
          logger.warn(`[marsdobby-ledger] reconcile ${s.sessionId.slice(0, 8)}: ${err instanceof Error ? err.message : String(err)}`);
        }
      }
      const now = this.now();
      for (const id of this.deps.outbox.listSnapshotIds()) {
        if (seen.has(id)) continue;
        const snap = this.deps.outbox.getSnapshot(id);
        if (!snap || snap.status === 'closed') continue;
        // Row vanished from the store while we still thought it open.
        this.enqueue(ledgerEventId(this.deps.larkAppId, id, 'closed', `missing${snap.updateCount}`), 'session.closed',
          this.envelope(id, now, { session: { status: 'closed' }, reason: 'missing_from_store' }),
          { ...snap, status: 'closed', reportedAt: now });
      }
      this.deps.outbox.pruneSnapshots(now - SNAPSHOT_RETENTION_MS);
    } catch (err) {
      logger.warn(`[marsdobby-ledger] reconcile skipped: ${err instanceof Error ? err.message : String(err)}`);
    }
    this.kick();
  }

  private kick(): void {
    if (this.sending || this.stopped || this.retryTimer) return;
    const t = setTimeout(() => { this.timers.delete(t); void this.deliver(); }, 0);
    t.unref?.();
    this.timers.add(t);
  }

  /** Send queued events in seq order; resolves when the queue is empty or a retry is scheduled. */
  async deliver(): Promise<void> {
    if (this.sending || this.stopped) return;
    this.sending = true;
    try {
      for (;;) {
        if (this.stopped) return;
        const batch = this.deps.outbox.peek(this.batchSize);
        if (batch.length === 0) return;
        let status: number;
        try {
          status = await this.deps.post(JSON.stringify({ events: batch.map(e => e.body) }));
        } catch (err) {
          this.scheduleRetry(`network: ${err instanceof Error ? err.message : String(err)}`);
          return;
        }
        if (status >= 200 && status < 300) {
          this.deps.outbox.ack(batch.map(e => e.seq));
          if (this.backoffMs > 0) logger.info('[marsdobby-ledger] receiver reachable again');
          this.backoffMs = 0;
          this.batchSize = this.deps.batchSize ?? 100;
          continue;
        }
        if (status === 400 || status === 413) {
          if (batch.length > 1) { this.batchSize = Math.ceil(batch.length / 2); continue; }
          logger.error(`[marsdobby-ledger] receiver rejected event ${batch[0].id} (${batch[0].type}) with ${status}; dropping it`);
          this.deps.outbox.ack([batch[0].seq]);
          continue;
        }
        this.scheduleRetry(`HTTP ${status}`);
        return;
      }
    } catch (err) {
      this.scheduleRetry(`outbox: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      this.sending = false;
    }
  }

  private scheduleRetry(reason: string): void {
    if (this.stopped) return;
    const max = this.deps.maxBackoffMs ?? 300_000;
    if (this.backoffMs === 0) logger.warn(`[marsdobby-ledger] delivery failed (${reason}); retrying with backoff`);
    this.backoffMs = Math.min(max, this.backoffMs === 0 ? 1_000 : this.backoffMs * 2);
    const delay = Math.round(this.backoffMs * (0.75 + Math.random() * 0.5));
    this.retryTimer = setTimeout(() => { this.retryTimer = undefined; void this.deliver(); }, delay);
    this.retryTimer.unref?.();
  }
}

/** POST via node:http(s) — ignores proxy env vars, so a loopback receiver stays loopback. */
export function httpLedgerPost(url: URL, token: string, timeoutMs = 10_000): LedgerPost {
  return body => new Promise<number>((resolve, reject) => {
    const request = url.protocol === 'https:' ? httpsRequest : httpRequest;
    const req = request(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
        authorization: `Bearer ${token}`,
      },
      timeout: timeoutMs,
    }, res => {
      res.resume();
      res.on('end', () => resolve(res.statusCode ?? 0));
      res.on('error', reject);
    });
    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', reject);
    req.end(body);
  });
}

export function ledgerOutboxPath(dataDir: string, larkAppId: string): string {
  return join(dataDir, 'marsdobby-ledger', `${larkAppId.replace(/[^A-Za-z0-9_-]/g, '_')}.sqlite`);
}

/**
 * Daemon entry. Returns undefined (and does nothing at all) when the URL is
 * unset or the configuration is refused; never throws.
 */
export function startMarsDobbyLedger(opts: {
  larkAppId: string;
  cliId?: string;
  env?: NodeJS.ProcessEnv;
  dataDir?: string;
  bus?: DashboardEventBus;
  store?: LedgerStoreView;
}): MarsDobbyLedger | undefined {
  let cfg: LedgerConfig | undefined;
  try {
    cfg = readLedgerConfig(opts.env ?? process.env);
  } catch (err) {
    logger.error(`[marsdobby-ledger] not started: ${err instanceof Error ? err.message : String(err)}`);
    return undefined;
  }
  if (!cfg) return undefined;
  try {
    const outbox = new LedgerOutbox(ledgerOutboxPath(opts.dataDir ?? config.session.dataDir, opts.larkAppId));
    const ledger = new MarsDobbyLedger({
      larkAppId: opts.larkAppId,
      cliId: opts.cliId,
      text: cfg.text,
      outbox,
      store: opts.store ?? { getSession: sessionStore.getSession, listSessions: sessionStore.listSessionsStrict },
      bus: opts.bus ?? dashboardEventBus,
      post: httpLedgerPost(cfg.url, cfg.token),
    }).start();
    logger.info(`[marsdobby-ledger] reporting to ${cfg.url.origin} (text=${cfg.text}, queued=${outbox.size()})`);
    return ledger;
  } catch (err) {
    logger.error(`[marsdobby-ledger] not started: ${err instanceof Error ? err.message : String(err)}`);
    return undefined;
  }
}
