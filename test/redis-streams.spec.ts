import 'mocha';
import { expect } from 'chai';
import { RedisStreams } from '../src/redis-streams';
import { StreamConsumer } from '../src/stream-consumer';
import { IRedisClient } from '../src/redis-client';
import { ConsumerConfigs, NamedEventHandler, RedisStreamsInputConfig, StreamConfigs } from '../src/types';
import { createFakeRedis, FakeRedis } from './helpers/fake-redis';

// ── Test helpers ──────────────────────────────────────────────────────────────

class TestRedisStreams extends RedisStreams {
  private _producer: IRedisClient;
  private _consumers: IRedisClient[];
  public capturedConsumer?: StreamConsumer;

  constructor(
    producer: IRedisClient,
    consumers: IRedisClient[] = [],
    config?: Partial<RedisStreamsInputConfig>,
  ) {
    super('test-peer', config ?? { redis: { host: 'fake', port: 1 } });
    this._producer = producer;
    this._consumers = [...consumers];
  }

  protected getProducerRedis(): IRedisClient { return this._producer; }
  protected getConsumerRedis(): IRedisClient { return this._consumers.shift() ?? this._producer; }

  protected registerConsumer(
    streamName: string,
    groupName: string,
    handlers: Map<string, NamedEventHandler>,
    config: ConsumerConfigs,
  ): StreamConsumer {
    const consumer = super.registerConsumer(streamName, groupName, handlers, config);
    this.capturedConsumer = consumer;
    return consumer;
  }

  // expose buildConsumerConfigs for testing deadLetters handler
  public buildConsumerConfigsPublic(streamName: string, groupName: string, config?: Partial<StreamConfigs>) {
    return (this as any).buildConsumerConfigs(streamName, groupName, config);
  }
}

function makeEvent(name = 'click', data: any = {}) {
  return { name, v: '1.0.0', data, time: Date.now() };
}

// ── buildConfig ───────────────────────────────────────────────────────────────

describe('RedisStreams - buildConfig', () => {
  it('uses supplied host/port config', () => {
    const rs = new TestRedisStreams(createFakeRedis(), [], { redis: { host: 'h', port: 1234 } });
    expect((rs as any).config.redis).to.deep.equal({ host: 'h', port: 1234 });
  });

  it('parses cluster string from config', () => {
    const rs = new TestRedisStreams(createFakeRedis(), [], { redis: { cluster: 'n1:7001,n2:7002' } });
    expect((rs as any).config.redis).to.deep.equal([
      { host: 'n1', port: 7001 },
      { host: 'n2', port: 7002 },
    ]);
  });

  it('falls back to env vars when no redis config is supplied', () => {
    const orig = { host: process.env.REDIS_HOST, port: process.env.REDIS_PORT };
    process.env.REDIS_HOST = 'env-host';
    process.env.REDIS_PORT = '6379';
    try {
      const rs = new RedisStreams('test-peer'); // no config → reads from env vars
      expect((rs as any).config.redis.host).to.equal('env-host');
    } finally {
      if (orig.host !== undefined) process.env.REDIS_HOST = orig.host; else delete process.env.REDIS_HOST;
      if (orig.port !== undefined) process.env.REDIS_PORT = orig.port; else delete process.env.REDIS_PORT;
    }
  });

  it('stores deadLetters config when provided', () => {
    const rs = new TestRedisStreams(createFakeRedis(), [], {
      redis: { host: 'h', port: 1 },
      deadLetters: { stream: 'dl', maxRetries: 3, maxSize: 1000 },
    });
    expect((rs as any).config.deadLetters).to.exist;
    expect((rs as any).config.deadLetters.maxRetries).to.equal(3);
  });

  it('uses the provided logger instead of creating a default one', () => {
    const myLogger = { trace: () => {}, info: () => {}, error: () => {}, warn: () => {} };
    const rs = new TestRedisStreams(createFakeRedis(), [], {
      redis: { host: 'h', port: 1 },
      logger: myLogger as any,
    });
    expect((rs as any).config.logger).to.equal(myLogger);
  });
});

// ── group() / stream caching ──────────────────────────────────────────────────

describe('RedisStreams - group() and stream caching', () => {
  it('returns a ConsumerGroup with a stream() factory', () => {
    const rs = new TestRedisStreams(createFakeRedis());
    const group = rs.group('g');
    expect(group.stream).to.be.a('function');
  });

  it('returns the same StreamGroupConsumer on repeated stream() calls', () => {
    const rs = new TestRedisStreams(createFakeRedis());
    const group = rs.group('g');
    const s1 = group.stream('s');
    const s2 = group.stream('s');
    expect(s1).to.equal(s2);
  });

  it('returns different consumers for different stream names', () => {
    const rs = new TestRedisStreams(createFakeRedis());
    const group = rs.group('g');
    const s1 = group.stream('stream-a');
    const s2 = group.stream('stream-b');
    expect(s1).to.not.equal(s2);
  });
});

// ── getProducerRedis lazy caching ─────────────────────────────────────────────

describe('RedisStreams - producer client lazy caching', () => {
  it('returns the same producer instance on repeated calls', () => {
    const fakeProducer = createFakeRedis();
    // After overriding _producer via subclass we can verify caching:
    const rs2 = new TestRedisStreams(fakeProducer);
    const p1 = rs2['getProducerRedis']();
    const p2 = rs2['getProducerRedis']();
    expect(p1).to.equal(p2);
    expect(p1).to.equal(fakeProducer);
  });

  it('real getProducerRedis() creates and caches a new client on first call', () => {
    const rs = new RedisStreams('peer', { redis: { host: 'localhost', port: 6399 } });
    expect((rs as any)._producerRedis).to.be.undefined;
    const c1 = (rs as any).getProducerRedis();
    const c2 = (rs as any).getProducerRedis();
    expect(c1).to.exist;
    expect(c1).to.equal(c2);
    c1.disconnect();
  });

  it('real getConsumerRedis() creates a fresh client on each call', () => {
    const rs = new RedisStreams('peer', { redis: { host: 'localhost', port: 6399 } });
    const c1 = (rs as any).getConsumerRedis();
    const c2 = (rs as any).getConsumerRedis();
    expect(c1).to.exist;
    c1.disconnect();
    c2.disconnect();
  });
});

// ── produce() ────────────────────────────────────────────────────────────────

describe('RedisStreams - produce()', () => {
  it('executes a pipeline for a plain event', async () => {
    const fakeProducer = createFakeRedis();
    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.resetHistory();
    fakeProducer.pipeline.callsFake(() => fakePipe);

    const rs = new TestRedisStreams(fakeProducer);
    const stream = rs.group('g').stream('s');
    await stream.produce(makeEvent());
    expect((fakePipe as any).exec.calledOnce).to.be.true;
  });

  it('returns a wait handle for an IWaitEvent and resolves with the reply', async () => {
    const fakeProducer = createFakeRedis();
    const fakeConsumer = createFakeRedis();

    let capturedMsgHandler: ((...a: any[]) => void) | undefined;
    // Use withArgs('message') because sinon 9's returnsThis() takes precedence over plain callsFake()
    fakeConsumer.on.withArgs('message').callsFake((_event: string, handler: any) => {
      capturedMsgHandler = handler;
      return fakeConsumer;
    });

    // After pipeline.exec on the producer, emit the reply on the consumer
    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.resetHistory();
    fakeProducer.pipeline.callsFake(() => fakePipe);
    (fakePipe as any).exec.callsFake(() => {
      // capturedMsgHandler is set synchronously by redis.on('message', ...) before exec is called
      if (capturedMsgHandler) capturedMsgHandler('ch', JSON.stringify({ reply: 'pong', wait: {} }));
      return Promise.resolve([]);
    });

    // First consumer goes to registerConsumer (StreamConsumer internal), second goes to wait()
    const rs = new TestRedisStreams(fakeProducer, [createFakeRedis(), fakeConsumer]);
    const stream = rs.group('g').stream('s');

    const waitEvent = { name: 'ping', v: '1', data: {}, time: 1, wait: { source: 'svc' } };
    const result = await (stream.produce(waitEvent) as any).wait(5000);
    expect(result).to.equal('pong');
    expect(fakeConsumer.disconnect.calledOnce).to.be.true;
  });

  it('cleanup skips redis.off when subscribe rejects before the message handler is attached', async () => {
    const fakeProducer = createFakeRedis();
    const fakeConsumer = createFakeRedis();
    // subscribe rejects inside the async promise executor — the error is swallowed by the async
    // function, so the outer promise times out. msgHandler is never set, so cleanup runs without it.
    fakeConsumer.subscribe.rejects(new Error('subscribe failed'));

    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.callsFake(() => fakePipe);

    const rs = new TestRedisStreams(fakeProducer, [createFakeRedis(), fakeConsumer]);
    const stream = rs.group('g').stream('s');
    const waitEvent = { name: 'ping', v: '1', data: {}, time: 1, wait: { source: 'svc' } };
    // use a 1ms timeout — subscribe error is swallowed; timer fires instead
    await expect((stream.produce(waitEvent) as any).wait(1)).to.be.rejectedWith('Timeout');
    // cleanup ran: msgHandler was undefined, so off() was NOT called
    expect(fakeConsumer.off.called).to.be.false;
  });

  it('rejects with Timeout error when the reply does not arrive in time', async () => {
    const fakeProducer = createFakeRedis();
    const fakeConsumer = createFakeRedis();
    // pipeline.exec never emits a message
    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.resetHistory();
    fakeProducer.pipeline.callsFake(() => fakePipe);
    (fakePipe as any).exec.callsFake(async () => new Promise(() => {})); // never resolves

    // First consumer goes to registerConsumer, second goes to wait()
    const rs = new TestRedisStreams(fakeProducer, [createFakeRedis(), fakeConsumer]);
    const stream = rs.group('g').stream('s');
    const waitEvent = { name: 'ping', v: '1', data: {}, time: 1, wait: { source: 'svc' } };
    await expect((stream.produce(waitEvent) as any).wait(1)).to.be.rejectedWith('Timeout');
  });
});

// ── produceMany() / flush() ───────────────────────────────────────────────────

describe('RedisStreams - produceMany() + flush()', () => {
  it('batches multiple events into one pipeline and executes on flush', async () => {
    const fakeProducer = createFakeRedis();
    const fakeMulti = fakeProducer.multi();
    fakeProducer.multi.resetHistory();
    fakeProducer.multi.callsFake(() => fakeMulti);

    const rs = new TestRedisStreams(fakeProducer);
    const stream = rs.group('g').stream('s');

    await stream.produceMany(makeEvent('a'), makeEvent('b')).flush();
    expect((fakeMulti as any).exec.calledOnce).to.be.true;
    expect((fakeMulti as any).xadd.callCount).to.equal(2);
  });

  it('creates a fresh pipeline after each flush', async () => {
    const fakeProducer = createFakeRedis();
    const pipes: any[] = [];
    fakeProducer.multi.callsFake(() => {
      const p = fakeProducer.pipeline();
      pipes.push(p);
      return p;
    });

    const rs = new TestRedisStreams(fakeProducer);
    const stream = rs.group('g').stream('s');

    await stream.produceMany(makeEvent()).flush();
    await stream.produceMany(makeEvent()).flush();
    expect(pipes.length).to.be.at.least(2);
    expect(pipes[0]).to.not.equal(pipes[1]);
  });

  it('reuses the existing pipeline when produceMany is called again before flush', () => {
    const fakeProducer = createFakeRedis();
    const fakeMulti = fakeProducer.multi();
    fakeProducer.multi.resetHistory();
    fakeProducer.multi.callsFake(() => fakeMulti);

    const rs = new TestRedisStreams(fakeProducer);
    const stream = rs.group('g').stream('s');

    // Call produceMany twice without flushing — _currentPipeline already set on second call
    stream.produceMany(makeEvent('a'));
    stream.produceMany(makeEvent('b'));

    // multi() should only have been created once
    expect(fakeProducer.multi.callCount).to.equal(1);
    expect((fakeMulti as any).xadd.callCount).to.equal(2);
  });

  it('each stream uses its own isolated pipeline', async () => {
    const fakeProducer = createFakeRedis();
    const pipes: any[] = [];
    fakeProducer.multi.callsFake(() => {
      const p = fakeProducer.pipeline();
      pipes.push(p);
      return p;
    });

    const rs = new TestRedisStreams(fakeProducer);
    const streamA = rs.group('g').stream('stream-a');
    const streamB = rs.group('g').stream('stream-b');

    streamA.produceMany(makeEvent('a'));
    streamB.produceMany(makeEvent('b'));

    expect(pipes.length).to.equal(2);
    expect(pipes[0]).to.not.equal(pipes[1]);
  });
});

// ── handle() / event dispatch ─────────────────────────────────────────────────

describe('RedisStreams - handle() and event dispatch', () => {
  it('dispatches to the specific named handler', async () => {
    const rs = new TestRedisStreams(createFakeRedis());
    const stream = rs.group('g').stream('s');

    let called = false;
    stream.handle('myEvent', async () => { called = true; });

    const ev = { name: 'myEvent', v: '1', data: {}, time: 1 };
    await (rs.capturedConsumer as any).processMessage('id-1', ['myEvent', JSON.stringify(ev)]);
    expect(called).to.be.true;
  });

  it('falls back to the "*" wildcard handler when no specific handler matches', async () => {
    const rs = new TestRedisStreams(createFakeRedis());
    const stream = rs.group('g').stream('s');

    let wildcardCalled = false;
    stream.handle('*', async () => { wildcardCalled = true; });

    const ev = { name: 'unknownEvent', v: '1', data: {}, time: 1 };
    await (rs.capturedConsumer as any).processMessage('id-1', ['unknownEvent', JSON.stringify(ev)]);
    expect(wildcardCalled).to.be.true;
  });

  it('returns undefined silently when no handler matches', async () => {
    const rs = new TestRedisStreams(createFakeRedis());
    rs.group('g').stream('s'); // register no handlers

    const ev = { name: 'unhandled', v: '1', data: {}, time: 1 };
    const result = await (rs.capturedConsumer as any).processMessage('id-1', ['unhandled', JSON.stringify(ev)]);
    expect(result).to.be.undefined;
  });
});

// ── consume() / continue() ────────────────────────────────────────────────────

describe('RedisStreams - consume() and continue()', () => {
  it('initializes the consumer group and returns stop/continue handles', async () => {
    const fakeConsumer = createFakeRedis();
    const rs = new TestRedisStreams(createFakeRedis(), [fakeConsumer]);
    const stream = rs.group('g').stream('s');

    let unblock!: () => void;
    fakeConsumer.xpending.resolves([]);
    fakeConsumer.xreadgroup.callsFake(async () => {
      await new Promise<void>(r => { unblock = r; });
      return null;
    });

    const { stop, continue: cont } = await stream.consume();
    expect(fakeConsumer.xgroup.calledOnce).to.be.true;
    expect(stop).to.be.a('function');
    expect(cont).to.be.a('function');

    // let the (fire-and-forget) start() reach the blocked xreadgroup call and assign `unblock`
    await new Promise(r => setTimeout(r, 0));

    // stop() now waits for the in-flight (blocked) read to finish before resolving
    const stopping = stop();
    unblock();
    await stopping;

    expect(fakeConsumer.xgroup.calledWith('DELCONSUMER')).to.be.true;
  });

  it('continue() calls start() again on the consumer', async () => {
    const fakeConsumer = createFakeRedis();
    const rs = new TestRedisStreams(createFakeRedis(), [fakeConsumer]);
    const stream = rs.group('g').stream('s');

    let unblock!: () => void;
    fakeConsumer.xpending.resolves([]);
    fakeConsumer.xreadgroup.callsFake(async () => {
      await new Promise<void>(r => { unblock = r; });
      return null;
    });

    const { stop, continue: cont } = await stream.consume();
    // let the (fire-and-forget) start() reach the blocked xreadgroup call and assign `unblock`
    await new Promise(r => setTimeout(r, 0));
    const stopping = stop();
    unblock();
    await stopping;
    // after stop, calling continue re-invokes start (exits immediately since disposing=true)
    cont();
    const createCalls = fakeConsumer.xgroup.getCalls().filter((c: any) => c.args[0] === 'CREATE');
    expect(createCalls.length).to.equal(1); // only one init
  });
});

// ── with() ────────────────────────────────────────────────────────────────────

describe('RedisStreams - with()', () => {
  it('wraps event factories to call stream.produce', async () => {
    const fakeProducer = createFakeRedis();
    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.callsFake(() => fakePipe);

    const rs = new TestRedisStreams(fakeProducer);
    const stream = rs.group('g').stream('s').with({
      ping: (msg: string) => makeEvent('ping', { msg }),
    });

    await (stream as any).ping('hello');
    expect((fakePipe as any).exec.calledOnce).to.be.true;
  });
});

// ── doProduce() edge cases ────────────────────────────────────────────────────

describe('RedisStreams - doProduce()', () => {
  it('returns the pipeline unchanged when events array is empty', async () => {
    const fakeProducer = createFakeRedis();
    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.callsFake(() => fakePipe);

    const rs = new TestRedisStreams(fakeProducer);
    const stream = rs.group('g').stream('s');
    // produceMany with no events — calls doProduce with empty list
    await stream.produceMany().flush();
    expect((fakePipe as any).xadd.called).to.be.false;
  });

  it('calls xadd without MAXLEN when maxLen is undefined', () => {
    const fakeProducer = createFakeRedis();
    const rs = new TestRedisStreams(fakeProducer);
    const stream = rs.group('g').stream('s', { maxLen: undefined as any });
    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.callsFake(() => fakePipe);
    (rs as any).doProduce(fakePipe, 'my-stream', undefined, makeEvent('e'));
    const xaddArgs = (fakePipe as any).xadd.firstCall.args;
    expect(xaddArgs[0]).to.equal('my-stream');
    expect(xaddArgs[1]).to.equal('*');
  });

  it('calls xadd with MAXLEN ~ when maxLen is a number', () => {
    const fakeProducer = createFakeRedis();
    const rs = new TestRedisStreams(fakeProducer);
    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.callsFake(() => fakePipe);
    (rs as any).doProduce(fakePipe, 'my-stream', 5000, makeEvent('e'));
    const xaddArgs = (fakePipe as any).xadd.firstCall.args;
    expect(xaddArgs[1]).to.equal('MAXLEN');
    expect(xaddArgs[2]).to.equal('~');
    expect(xaddArgs[3]).to.equal('5000');
  });
});

// ── deadLetters handler ───────────────────────────────────────────────────────

describe('RedisStreams - buildConsumerConfigs deadLetters.handler', () => {
  it('publishes dead-letter events to the configured stream via the producer pipeline', async () => {
    const fakeProducer = createFakeRedis();
    const fakePipe = fakeProducer.pipeline();
    fakeProducer.pipeline.callsFake(() => fakePipe);

    const rs = new TestRedisStreams(fakeProducer, [], {
      redis: { host: 'h', port: 1 },
      deadLetters: { stream: 'dead-letters', maxRetries: 3, maxSize: 1000 },
    });

    const configs = rs.buildConsumerConfigsPublic('s', 'g');
    await configs.deadLetters!.handler({ name: 'deadMessage', v: '1', data: {} as any, time: 1 });

    expect((fakePipe as any).exec.calledOnce).to.be.true;
    expect((fakePipe as any).xadd.calledOnce).to.be.true;
    // xadd was called for the dead-letters stream
    const xaddArgs = (fakePipe as any).xadd.firstCall.args;
    expect(xaddArgs[0]).to.equal('dead-letters');
  });
});
