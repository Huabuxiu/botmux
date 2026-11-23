import { chmodSync, mkdtempSync, readdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/utils/logger.js', () => ({
  logger: { error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn() },
}));

import { dashboardEventBus } from '../src/core/dashboard-events.js';
import {
  httpLedgerPost, ledgerEventId, MarsDobbyLedger, readLedgerConfig, startMarsDobbyLedger,
  type LedgerPost,
} from '../src/marsdobby/ledger.js';
import { LedgerOutbox } from '../src/marsdobby/outbox.js';
import type { TurnResult } from '../src/services/turn-result.js';
import type { Session } from '../src/types.js';

const APP = 'cli_test_app';
let dir: string;

beforeEach(() => { dir = mkdtempSync(join(tmpdir(), 'md-ledger-')); });
afterEach(() => { rmSync(dir, { recursive: true, force: true }); });

function row(extra: Partial<Session> = {}): Session {
  return {
    sessionId: 'sess-a', chatId: 'oc_chat', rootMessageId: 'om_root', scope: 'thread', title: 'Fix the build',
    status: 'active', createdAt: new Date().toISOString(), workingDir: '/work/repo-a', cliId: 'claude-code',
    cliSessionId: 'cli-native-1', larkAppId: APP, ownerOpenId: 'ou_owner',
    ...extra,
  } as Session;
}

function fakeStore(rows: Session[]) {
  const map = new Map(rows.map(r => [r.sessionId, r]));
  return {
    map,
    getSession: (id: string) => map.get(id),
    listSessions: () => [...map.values()],
  };
}

function ledgerWith(outbox: LedgerOutbox, store: ReturnType<typeof fakeStore>, post: LedgerPost, text: 'full' | 'none' = 'full') {
  return new MarsDobbyLedger({
    larkAppId: APP, text, outbox, store, bus: dashboardEventBus, post,
    flushDelayMs: 1_000_000, firstReconcileDelayMs: 1_000_000, reconcileIntervalMs: 1_000_000, maxBackoffMs: 5,
  });
}

const queued = (outbox: LedgerOutbox) => outbox.peek(1000).map(e => e.body as Record<string, any>);
const never: LedgerPost = async () => { throw new Error('receiver down'); };

describe('marsdobby ledger configuration', () => {
  it('does nothing when the URL is unset: no ledger, no files', () => {
    expect(readLedgerConfig({})).toBeUndefined();
    expect(startMarsDobbyLedger({ larkAppId: APP, env: {}, dataDir: dir })).toBeUndefined();
    expect(readdirSync(dir)).toEqual([]);
  });

  it('refuses a group-readable or symlinked token file and creates nothing', () => {
    const token = join(dir, 'token');
    writeFileSync(token, 'secret-token\n');
    chmodSync(token, 0o644);
    const env = { BOTMUX_MARSDOBBY_LEDGER_URL: 'http://127.0.0.1:9/api/botmux/events', BOTMUX_MARSDOBBY_LEDGER_TOKEN_FILE: token };
    expect(() => readLedgerConfig(env)).toThrow(/group\/world/);
    const data = join(dir, 'data');
    expect(startMarsDobbyLedger({ larkAppId: APP, env, dataDir: data })).toBeUndefined();
    expect(readdirSync(dir)).toEqual(['token']);

    chmodSync(token, 0o600);
    const link = join(dir, 'link');
    symlinkSync(token, link);
    expect(() => readLedgerConfig({ ...env, BOTMUX_MARSDOBBY_LEDGER_TOKEN_FILE: link })).toThrow(/symlink/);
    expect(readLedgerConfig(env)).toMatchObject({ token: 'secret-token', text: 'full' });
    expect(() => readLedgerConfig({ ...env, BOTMUX_MARSDOBBY_LEDGER_URL: 'ftp://x/' })).toThrow();
  });
});

describe('marsdobby ledger events', () => {
  it('maps bus events and turn results to ledger events with session identifiers', () => {
    const outbox = new LedgerOutbox(join(dir, 'o.sqlite'));
    const store = fakeStore([row()]);
    const ledger = ledgerWith(outbox, store, never).start();
    try {
      dashboardEventBus.publish({ type: 'session.spawned', body: { session: { sessionId: 'sess-a' } } });
      dashboardEventBus.publish({ type: 'turn.finished', body: {
        sessionId: 'sess-a', turnId: 'om_t1', status: 'success', terminalStatus: 'completed', larkAppId: APP, chatId: 'oc_chat', senderOpenId: 'ou_asker' } });
      dashboardEventBus.publish({ type: 'session.update', body: { sessionId: 'sess-a', patch: {
        lastTurn: { turnId: 'om_t1', outcome: 'delivered', at: 2_000_000, startedAt: 1_000_000, summary: 'Build fixed.' } } } });
      ledger.onTurnResult({
        version: 1, ref: { botId: APP, sessionId: 'sess-a', turnId: 'om_t1' }, source: { kind: 'im' },
        execution: { status: 'completed', durationMs: 4200 },
        output: { status: 'available', content: 'Build fixed. Details follow.', source: 'transcript' }, deliveries: [],
      } as TurnResult);
      ledger.flush();
    } finally { ledger.stop(); }
    const events = queued(outbox);
    expect(events.map(e => e.type)).toEqual(['session.started', 'turn.finished', 'turn.finished', 'turn.finished']);
    expect(events[0]).toMatchObject({
      botAppId: APP, sessionId: 'sess-a', stream: outbox.streamId,
      session: { chatId: 'oc_chat', rootMessageId: 'om_root', cliId: 'claude-code', cliSessionId: 'cli-native-1', workingDir: '/work/repo-a', status: 'active' },
    });
    expect(events[1].turn).toMatchObject({ turnId: 'om_t1', status: 'success', askerOpenId: 'ou_asker' });
    expect(events[2].turn).toMatchObject({ turnId: 'om_t1', durationMs: 1_000_000, answerExcerpt: 'Build fixed.' });
    expect(events[3].turn).toMatchObject({ turnId: 'om_t1', durationMs: 4200, answer: { text: 'Build fixed. Details follow.' } });
    const seqs = events.map(e => e.seq);
    expect([...seqs].sort((a, b) => a - b)).toEqual(seqs);
    expect(new Set(events.map(e => e.id)).size).toBe(4);
  });

  it('keeps answer text inside botmux when text mode is none', () => {
    const outbox = new LedgerOutbox(join(dir, 'o.sqlite'));
    const ledger = ledgerWith(outbox, fakeStore([row()]), never, 'none');
    ledger.onBusEvent({ type: 'session.update', body: { sessionId: 'sess-a', patch: {
      lastTurn: { turnId: 'om_t1', outcome: 'delivered', at: 2_000, summary: 'secret answer' } } } });
    ledger.onTurnResult({ version: 1, ref: { botId: APP, sessionId: 'sess-a', turnId: 'om_t1' }, source: { kind: 'im' },
      execution: { status: 'completed' }, output: { status: 'available', content: 'secret answer' }, deliveries: [] } as TurnResult);
    ledger.flush();
    ledger.stop();
    expect(JSON.stringify(queued(outbox))).not.toContain('secret answer');
  });

  it('derives the same event ids for the same facts in a fresh outbox (idempotent redelivery)', () => {
    const run = (path: string) => {
      const outbox = new LedgerOutbox(path);
      const ledger = ledgerWith(outbox, fakeStore([row()]), never);
      ledger.onBusEvent({ type: 'turn.finished', body: {
        sessionId: 'sess-a', turnId: 'om_t9', status: 'error', terminalStatus: 'failed', larkAppId: APP, chatId: 'oc_chat' } });
      ledger.onBusEvent({ type: 'turn.finished', body: {
        sessionId: 'sess-a', turnId: 'om_t9', status: 'error', terminalStatus: 'failed', larkAppId: APP, chatId: 'oc_chat' } });
      ledger.flush();
      ledger.stop();
      return queued(outbox).map(e => e.id);
    };
    const first = run(join(dir, 'a.sqlite'));
    const second = run(join(dir, 'b.sqlite'));
    expect(first).toHaveLength(2); // started + one turn.finished (duplicate collapsed)
    expect(second).toEqual(first);
    expect(first[0]).toBe(ledgerEventId(APP, 'sess-a', 'started'));
  });

  it('reports suspend as an update and finds a close that no event announced', () => {
    const outbox = new LedgerOutbox(join(dir, 'o.sqlite'));
    const store = fakeStore([row()]);
    const ledger = ledgerWith(outbox, store, never);
    ledger.reconcile();
    store.map.set('sess-a', row({ suspendedColdResume: true }));
    ledger.onBusEvent({ type: 'session.update', body: { sessionId: 'sess-a', patch: { status: 'dormant' } } });
    ledger.flush();
    // Closed while suspended: the store changes, the bus stays silent.
    store.map.set('sess-a', row({ suspendedColdResume: true, status: 'closed', closedAt: new Date().toISOString() }));
    ledger.reconcile();
    ledger.reconcile();
    ledger.stop();
    const events = queued(outbox);
    expect(events.map(e => [e.type, e.session.status])).toEqual([
      ['session.started', 'active'], ['session.updated', 'suspended'], ['session.closed', 'closed'],
    ]);
  });

  it('ignores sessions closed long before the ledger was enabled', () => {
    const outbox = new LedgerOutbox(join(dir, 'o.sqlite'));
    const old = new Date(Date.now() - 10 * 24 * 3600_000).toISOString();
    const ledger = ledgerWith(outbox, fakeStore([row({ status: 'closed', closedAt: old, createdAt: old })]), never);
    ledger.reconcile();
    ledger.stop();
    expect(queued(outbox)).toEqual([]);
  });
});

describe('marsdobby ledger outbox', () => {
  it('survives a restart and delivers in order once the receiver is back', async () => {
    const path = join(dir, 'o.sqlite');
    const first = new LedgerOutbox(path);
    const store = fakeStore([row(), row({ sessionId: 'sess-b', rootMessageId: 'om_root_b' })]);
    const down = ledgerWith(first, store, never);
    down.reconcile();
    await down.deliver();
    down.stop();
    const before = queued(first);
    expect(before).toHaveLength(2);
    const stream = first.streamId;
    first.close();

    const received: any[] = [];
    let calls = 0;
    const flaky: LedgerPost = async body => {
      calls++;
      if (calls === 1) return 503;
      received.push(...JSON.parse(body).events);
      return 200;
    };
    const second = new LedgerOutbox(path);
    expect(second.streamId).toBe(stream);
    const up = ledgerWith(second, store, flaky);
    await up.deliver();
    await vi.waitFor(() => expect(second.size()).toBe(0), { timeout: 2000 });
    up.stop();
    expect(received.map(e => e.id)).toEqual(before.map(e => e.id));
    expect(received.map(e => e.seq)).toEqual(before.map(e => e.seq));
  });

  it('drops oldest session.updated first and refuses critical events when full', () => {
    const outbox = new LedgerOutbox(join(dir, 'o.sqlite'), 3);
    expect(outbox.enqueue({ id: 'u1', type: 'session.updated', body: {} })).toBe('queued');
    expect(outbox.enqueue({ id: 's1', type: 'session.started', body: {} })).toBe('queued');
    expect(outbox.enqueue({ id: 'c1', type: 'session.closed', body: {} })).toBe('queued');
    expect(outbox.enqueue({ id: 't1', type: 'turn.finished', body: {} })).toBe('queued');
    expect(outbox.peek(10).map(e => e.id)).toEqual(['s1', 'c1', 't1']);
    expect(outbox.enqueue({ id: 't2', type: 'turn.finished', body: {} })).toBe('full');
    expect(outbox.enqueue({ id: 't1', type: 'turn.finished', body: {} })).toBe('duplicate');
  });

  it('posts with a bearer token to a real HTTP receiver', async () => {
    let auth = '';
    let got: any;
    const server = createServer((req, res) => {
      auth = String(req.headers.authorization);
      let raw = '';
      req.on('data', c => { raw += c; });
      req.on('end', () => { got = JSON.parse(raw); res.end('{"ok":true}'); });
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    try {
      const port = (server.address() as AddressInfo).port;
      const post = httpLedgerPost(new URL(`http://127.0.0.1:${port}/api/botmux/events`), 'tok');
      expect(await post(JSON.stringify({ events: [] }))).toBe(200);
      expect(auth).toBe('Bearer tok');
      expect(got).toEqual({ events: [] });
    } finally {
      server.close();
    }
  });

});
