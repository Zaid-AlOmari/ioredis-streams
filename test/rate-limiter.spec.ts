import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { RateLimiter } from '../src/rate-limiter';
import { StreamConsumer } from '../src/stream-consumer';
import { ConsumerConfigs, IEvent } from '../src/types';
import { createFakeRedis } from './helpers/fake-redis';

class T extends StreamConsumer {
  set testDisposing(v: boolean) { this.disposing = v; }
  async testDoRead(limit?: number) { return this.doRead(limit); }
  async testDoClaim(limit?: number) { return this.doClaim(limit); }
  async testTryReading() { return this.tryReading(); }
  get testLimiter() { return this.rateLimiter; }
}

function makeConfig(overrides: Partial<ConsumerConfigs> = {}): ConsumerConfigs {
  return {
    streamName: 'my-stream',
    groupName: 'my-group',
    peerName: 'peer-1',
    batchSize: 5,
    claimIdleTime: 15000,
    readBlockTime: 5000,
    mode: 'parallel',
    maxLen: 100000,
    ...overrides,
  };
}

function entry(id: string): [string, string[]] {
  const event: IEvent<any> = { name: 'click', v: '1.0.0', data: {}, time: 1 };
  return [id, [event.name, JSON.stringify(event)]];
}

function readReply(...ids: string[]): any {
  return [['my-stream', ids.map(entry)]];
}

describe('RateLimiter', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => { clock = sinon.useFakeTimers({ now: 1_000_000 }); });
  afterEach(() => { clock.restore(); });

  it('rejects non-positive configuration', () => {
    expect(() => new RateLimiter({ maxMessages: 0, duration: 1000 })).to.throw(/maxMessages/);
    expect(() => new RateLimiter({ maxMessages: NaN as any, duration: 1000 })).to.throw(/maxMessages/);
    expect(() => new RateLimiter({ maxMessages: 5, duration: 0 })).to.throw(/rateLimit.duration/);
    expect(() => new RateLimiter({ maxMessages: 5, duration: '1 week' as any })).to.throw(/rateLimit.duration/);
  });

  it('accepts string durations such as 1m', () => {
    const limiter = new RateLimiter({ maxMessages: 60, duration: '1m' }); // 1 per second
    limiter.take(60);
    clock.tick(999);
    expect(limiter.available).to.equal(0);
    clock.tick(1);
    expect(limiter.available).to.equal(1);
  });

  it('starts full, allowing a burst of maxMessages', async () => {
    const limiter = new RateLimiter({ maxMessages: 10, duration: 1000 });
    expect(limiter.available).to.equal(10);
    expect(await limiter.waitForTokens(4)).to.equal(4);
    expect(await limiter.waitForTokens(50)).to.equal(10);
  });

  it('spends tokens with take() and refills over time, capped at maxMessages', () => {
    const limiter = new RateLimiter({ maxMessages: 10, duration: 1000 });
    limiter.take(10);
    expect(limiter.available).to.equal(0);
    clock.tick(500);
    expect(limiter.available).to.equal(5);
    clock.tick(10_000);
    expect(limiter.available).to.equal(10);
  });

  it('ignores take() with zero or negative counts', () => {
    const limiter = new RateLimiter({ maxMessages: 3, duration: 1000 });
    limiter.take(0);
    limiter.take(-2);
    expect(limiter.available).to.equal(3);
  });

  it('waits for a full batch, not a single message', async () => {
    const limiter = new RateLimiter({ maxMessages: 100, duration: '1s' }); // 1 token / 10ms
    limiter.take(100);

    let result: number | undefined;
    const waiting = limiter.waitForTokens(50).then(n => { result = n; });

    clock.tick(10); // 1 token: enough for one message, not for the batch
    await Promise.resolve();
    expect(result).to.be.undefined;
    clock.tick(489);
    await Promise.resolve();
    expect(result).to.be.undefined;
    clock.tick(1); // 50 tokens
    await waiting;
    expect(result).to.equal(50);
  });

  it('waits for maxMessages when the limit is smaller than the batch', async () => {
    const limiter = new RateLimiter({ maxMessages: 2, duration: 1000 }); // 1 token / 500ms
    limiter.take(2);

    let result: number | undefined;
    const waiting = limiter.waitForTokens(5).then(n => { result = n; });

    clock.tick(500);
    await Promise.resolve();
    expect(result).to.be.undefined;
    clock.tick(500);
    await waiting;
    expect(result).to.equal(2);
  });

  it('refund() gives tokens back, capped at maxMessages', () => {
    const limiter = new RateLimiter({ maxMessages: 10, duration: '1m' });
    limiter.take(8);
    limiter.refund(3);
    expect(limiter.available).to.equal(5);
    limiter.refund(0);
    limiter.refund(100);
    expect(limiter.available).to.equal(10);
  });

  it('countOn defaults to read and rejects unknown values', () => {
    expect(new RateLimiter({ maxMessages: 1, duration: '1s' }).countOn).to.equal('read');
    expect(new RateLimiter({ maxMessages: 1, duration: '1s', countOn: 'ack' }).countOn).to.equal('ack');
    expect(() => new RateLimiter({ maxMessages: 1, duration: '1s', countOn: 'success' as any })).to.throw(/countOn/);
  });

  it('cancel() wakes a pending wait and returns 0', async () => {
    const limiter = new RateLimiter({ maxMessages: 1, duration: 60_000 });
    limiter.take(1);
    const waiting = limiter.waitForTokens(5);
    limiter.cancel();
    expect(await waiting).to.equal(0);
  });

  it('cancel() is a no-op when nothing is waiting', () => {
    const limiter = new RateLimiter({ maxMessages: 1, duration: 1000 });
    expect(() => limiter.cancel()).to.not.throw();
  });
});

describe('StreamConsumer rate limiting', () => {

  it('does not create a limiter when rateLimit is not configured', () => {
    const consumer = new T(createFakeRedis(), async () => undefined, makeConfig());
    expect(consumer.testLimiter).to.be.undefined;
  });

  it('reads only as many messages as the limiter allows', async () => {
    const fake = createFakeRedis();
    const consumer = new T(fake, async () => undefined, makeConfig({
      batchSize: 10,
      rateLimit: { maxMessages: 3, duration: 60_000 },
    }));
    consumer.lastTimePendingCheck = Date.now(); // skip claim

    fake.xreadgroup.callsFake(async () => {
      consumer.testDisposing = true;
      return readReply('1-0', '2-0', '3-0');
    });

    await consumer.testTryReading();

    const args = fake.xreadgroup.firstCall.args;
    expect(args[args.indexOf('COUNT') + 1]).to.equal(3);
    expect(consumer.testLimiter!.available).to.equal(0);
    expect(fake.xack.calledOnce).to.be.true;
  });

  it('only spends tokens for messages actually received', async () => {
    const fake = createFakeRedis();
    const consumer = new T(fake, async () => undefined, makeConfig({
      rateLimit: { maxMessages: 5, duration: 60_000 },
    }));
    fake.xreadgroup.resolves(readReply('1-0', '2-0'));

    await consumer.testDoRead(5);

    expect(consumer.testLimiter!.available).to.equal(3);
  });

  it('limits claims to the allowed count and does not charge dead letters', async () => {
    const fake = createFakeRedis();
    const handler = sinon.stub().resolves();
    const consumer = new T(fake, async () => undefined, makeConfig({
      rateLimit: { maxMessages: 5, duration: 60_000 },
      deadLetters: { handler, maxRetries: 3 },
    }));
    fake.xpending.resolves([
      ['1-0', 'peer-x', 20000, 1],
      ['2-0', 'peer-x', 20000, 5], // over maxRetries -> dead letter
    ]);
    fake.xclaim.resolves([entry('1-0'), entry('2-0')]);

    expect(await consumer.testDoClaim(2)).to.be.true;

    expect(fake.xpending.firstCall.args[4]).to.equal(2);
    expect(handler.calledOnce).to.be.true;
    expect(consumer.testLimiter!.available).to.equal(4);
  });

  for (const mode of ['parallel', 'serial'] as const) {
    it(`countOn ack gives a failed message's spot back (${mode})`, async () => {
      const fake = createFakeRedis();
      const consumer = new T(fake, async (id: string) => { if (id === '2-0') throw new Error('boom'); }, makeConfig({
        mode,
        logger: { trace() {}, info() {}, warn() {}, error() {} } as any,
        rateLimit: { maxMessages: 10, duration: '1h', countOn: 'ack' },
      }));
      fake.xreadgroup.resolves(readReply('1-0', '2-0', '3-0'));

      await consumer.testDoRead(3);

      expect(consumer.testLimiter!.available).to.equal(8);
    });
  }

  it('countOn read keeps a failed message counted', async () => {
    const fake = createFakeRedis();
    const consumer = new T(fake, async (id: string) => { if (id === '2-0') throw new Error('boom'); }, makeConfig({
      logger: { trace() {}, info() {}, warn() {}, error() {} } as any,
      rateLimit: { maxMessages: 10, duration: '1h' },
    }));
    fake.xreadgroup.resolves(readReply('1-0', '2-0', '3-0'));

    await consumer.testDoRead(3);

    expect(consumer.testLimiter!.available).to.equal(7);
  });

  it('dispose() stops a consumer that is waiting for rate-limit capacity', async () => {
    const fake = createFakeRedis();
    const consumer = new T(fake, async () => undefined, makeConfig({
      rateLimit: { maxMessages: 1, duration: 60 * 60 * 1000 },
    }));
    consumer.testLimiter!.take(1); // empty: next token in an hour

    const started = consumer.start();
    await new Promise(r => setTimeout(r, 10));
    await consumer.dispose();
    await started;

    expect(fake.xreadgroup.called).to.be.false;
    expect(fake.xgroup.calledWith('DELCONSUMER', 'my-stream', 'my-group', 'peer-1')).to.be.true;
  });
});
