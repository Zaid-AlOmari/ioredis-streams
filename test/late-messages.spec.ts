import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { parseDuration } from '../src/duration';
import { StreamConsumer } from '../src/stream-consumer';
import { ConsumerConfigs, DeadLetterEvent, IEvent } from '../src/types';
import { createFakeRedis } from './helpers/fake-redis';

class T extends StreamConsumer {
  async testDoRead(limit?: number) { return this.doRead(limit); }
  async testDoClaim(limit?: number) { return this.doClaim(limit); }
  get testLimiter() { return this.rateLimiter; }
}

const NOW = 10_000_000;

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

/** Entry whose stream id says Redis added it `ageMs` before NOW. */
function entryAged(ageMs: number, seq = 0): [string, string[]] {
  const event: IEvent<any> = { name: 'click', v: '1.0.0', data: {}, time: NOW - ageMs };
  return [`${NOW - ageMs}-${seq}`, [event.name, JSON.stringify(event)]];
}

describe('parseDuration', () => {
  it('parses numbers as milliseconds', () => {
    expect(parseDuration(1500)).to.equal(1500);
  });

  it('parses ms, s, m, h and d units, including decimals and spaces', () => {
    expect(parseDuration('500ms')).to.equal(500);
    expect(parseDuration('10s')).to.equal(10_000);
    expect(parseDuration('1m')).to.equal(60_000);
    expect(parseDuration('2h')).to.equal(7_200_000);
    expect(parseDuration('1d')).to.equal(86_400_000);
    expect(parseDuration('1.5s')).to.equal(1500);
    expect(parseDuration(' 3 m ' as any)).to.equal(180_000);
  });

  it('rejects unknown formats, zero, negatives and non-finite values', () => {
    expect(() => parseDuration('10 minutes' as any)).to.throw(/Invalid duration/);
    expect(() => parseDuration('abc' as any)).to.throw(/Invalid duration/);
    expect(() => parseDuration({} as any, 'x')).to.throw(/Invalid x/);
    expect(() => parseDuration('0s')).to.throw(/greater than 0/);
    expect(() => parseDuration(-5)).to.throw(/greater than 0/);
    expect(() => parseDuration(Infinity)).to.throw(/greater than 0/);
  });
});

describe('StreamConsumer late messages (maxMessageAge)', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => { clock = sinon.useFakeTimers({ now: NOW }); });
  afterEach(() => { clock.restore(); });

  function setup(overrides: Partial<ConsumerConfigs> = {}) {
    const fake = createFakeRedis();
    const dead: DeadLetterEvent[] = [];
    const processed: string[] = [];
    const consumer = new T(fake, async (id: string) => { processed.push(id); }, makeConfig({
      maxMessageAge: '1m',
      deadLetters: { maxRetries: 3, handler: async (...events) => { dead.push(...events); } },
      ...overrides,
    }));
    return { fake, dead, processed, consumer };
  }

  it('is off when maxMessageAge is not set: even very old messages are processed', async () => {
    const { fake, dead, processed, consumer } = setup({ maxMessageAge: undefined });
    const ancient = entryAged(365 * 24 * 60 * 60 * 1000);
    fake.xreadgroup.resolves([['my-stream', [ancient]]]);

    await consumer.testDoRead();

    expect(processed).to.deep.equal([ancient[0]]);
    expect(dead).to.be.empty;
  });

  it('is off by default in stream configs built by RedisStreams', () => {
    const { RedisStreams } = require('../src/redis-streams');
    const configs = (new RedisStreams('peer', { redis: { host: 'localhost', port: 6379 } }) as any).buildConsumerConfigs('s', 'g');
    expect(configs).to.not.have.property('maxMessageAge');
    expect(configs).to.not.have.property('rateLimit');
  });

  it('requires dead letters to be configured', () => {
    expect(() => new T(createFakeRedis(), async () => undefined, makeConfig({ maxMessageAge: '1m' })))
      .to.throw(/requires deadLetters/);
  });

  it('rejects an invalid maxMessageAge', () => {
    expect(() => setup({ maxMessageAge: 'soon' as any })).to.throw(/Invalid maxMessageAge/);
  });

  it('sends messages older than maxMessageAge to dead letters, acks them, and processes the rest', async () => {
    const { fake, dead, processed, consumer } = setup();
    const late = entryAged(61_000);
    const fresh = entryAged(59_000, 1);
    fake.xreadgroup.resolves([['my-stream', [late, fresh]]]);

    expect(await consumer.testDoRead()).to.be.true;

    expect(processed).to.deep.equal([fresh[0]]);
    expect(dead).to.have.length(1);
    expect(dead[0].data.id).to.equal(late[0]);
    expect(dead[0].data.reason).to.equal('too-old');
    expect(dead[0].data.message).to.deep.equal(late[1]);
    expect(fake.xack.calledWith('my-stream', 'my-group', late[0])).to.be.true;
    expect(fake.xack.calledWith('my-stream', 'my-group', fresh[0])).to.be.true;
  });

  it('does nothing extra when every message is fresh', async () => {
    const { fake, dead, processed, consumer } = setup();
    fake.xreadgroup.resolves([['my-stream', [entryAged(1000), entryAged(2000, 1)]]]);

    await consumer.testDoRead();

    expect(processed).to.have.length(2);
    expect(dead).to.be.empty;
    expect(fake.xack.calledOnce).to.be.true;
  });

  it('skips the buffer entirely when every message is late', async () => {
    const { fake, dead, processed, consumer } = setup();
    fake.xreadgroup.resolves([['my-stream', [entryAged(120_000), entryAged(90_000, 1)]]]);

    expect(await consumer.testDoRead()).to.be.true;

    expect(processed).to.be.empty;
    expect(dead.map(d => d.data.reason)).to.deep.equal(['too-old', 'too-old']);
  });

  it('checks claimed messages too', async () => {
    const { fake, dead, processed, consumer } = setup();
    const late = entryAged(300_000);
    fake.xpending.resolves([[late[0], 'peer-x', 20000, 1]]);
    fake.xclaim.resolves([late]);

    expect(await consumer.testDoClaim()).to.be.true;

    expect(processed).to.be.empty;
    expect(dead[0].data.reason).to.equal('too-old');
  });

  it('keeps retry dead letters in their original shape (no reason field)', async () => {
    const { fake, dead, consumer } = setup();
    const retried = entryAged(1000);
    fake.xpending.resolves([[retried[0], 'peer-x', 20000, 5]]);
    fake.xclaim.resolves([retried]);

    await consumer.testDoClaim();

    expect(dead).to.have.length(1);
    expect(dead[0].data).to.not.have.property('reason');
    expect(Object.keys(dead[0].data)).to.deep.equal(['id', 'message', 'stream', 'group']);
  });

  it('does not charge late messages against the rate limit', async () => {
    const { fake, consumer } = setup({ rateLimit: { maxMessages: 10, duration: '1m' } });
    fake.xreadgroup.resolves([['my-stream', [entryAged(120_000), entryAged(1000, 1)]]]);

    await consumer.testDoRead();

    expect(consumer.testLimiter!.available).to.equal(9);
  });

  it('leaves ids without a timestamp alone', async () => {
    const { fake, dead, processed, consumer } = setup();
    const event: IEvent<any> = { name: 'click', v: '1.0.0', data: {}, time: 0 };
    fake.xreadgroup.resolves([['my-stream', [['custom-id', [event.name, JSON.stringify(event)]]]]]);

    await consumer.testDoRead();

    expect(processed).to.deep.equal(['custom-id']);
    expect(dead).to.be.empty;
  });
});
