import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';
import { event, eventWithReply, augmentEvents } from '../src/events';
import { StreamGroupConsumer, IEvent } from '../src/types';

function makeStream(produce: sinon.SinonStub = sinon.stub().resolves()): StreamGroupConsumer {
  const s: StreamGroupConsumer = {
    consume: async () => ({ stop: () => {}, continue: () => {} }),
    handle: (() => ({ handle: s.handle, consume: s.consume })) as any,
    produce,
    produceMany: () => ({ produceMany: s.produceMany, flush: async () => {} }),
    with: (events) => augmentEvents(events, s),
  };
  return s;
}

describe('event()', () => {
  it('returns a factory keyed by the event name', () => {
    const factories = event('userSignedUp').of<{ email: string }>();
    expect(factories).to.have.property('userSignedUp');
    expect(factories.userSignedUp).to.be.a('function');
  });

  it('creates an event with the correct name, version, and data', () => {
    const factories = event('clicked', '2.0.0').of<{ x: number }>();
    const ev = factories.clicked({ x: 5 });
    expect(ev.name).to.equal('clicked');
    expect(ev.v).to.equal('2.0.0');
    expect(ev.data).to.deep.equal({ x: 5 });
  });

  it('uses the current timestamp when time is not supplied', () => {
    const before = Date.now();
    const factories = event('ping').of<void>();
    const ev = factories.ping(undefined as any);
    const after = Date.now();
    expect(ev.time).to.be.within(before, after);
  });

  it('uses the provided timestamp when supplied', () => {
    const factories = event('ping').of<void>();
    const ev = factories.ping(undefined as any, 12345);
    expect(ev.time).to.equal(12345);
  });

  it('defaults the version to "1.0.0" when not specified', () => {
    const factories = event('ping').of<void>();
    const ev = factories.ping(undefined as any);
    expect(ev.v).to.equal('1.0.0');
  });
});

describe('eventWithReply()', () => {
  it('returns a factory that creates a wait event with the correct source', () => {
    const factories = eventWithReply('fetchUser', 'api-gateway').of<{ id: string }, { name: string }>();
    const ev = factories.fetchUser({ id: '1' });
    expect(ev.name).to.equal('fetchUser');
    expect((ev as any).wait).to.deep.equal({ source: 'api-gateway' });
  });

  it('creates the wait event with correct name, version, data, and time', () => {
    const factories = eventWithReply('doThing', 'svc', '3.0.0').of<{ n: number }>();
    const ev = factories.doThing({ n: 7 }, 999);
    expect(ev.name).to.equal('doThing');
    expect(ev.v).to.equal('3.0.0');
    expect(ev.data).to.deep.equal({ n: 7 });
    expect(ev.time).to.equal(999);
    expect(ev.wait.source).to.equal('svc');
  });

  it('defaults version to "1.0.0"', () => {
    const factories = eventWithReply('ping', 'src').of<void>();
    const ev = factories.ping(undefined as any);
    expect(ev.v).to.equal('1.0.0');
  });
});

describe('augmentEvents()', () => {
  it('wraps each factory so calling it produces via the stream', () => {
    const produce = sinon.stub().resolves();
    const stream = makeStream(produce);
    const factories = {
      login: (userId: string) => ({ name: 'login', v: '1', data: { userId }, time: 1 }),
    };
    const augmented = augmentEvents(factories, stream);
    augmented.login('u1');
    expect(produce.calledOnce).to.be.true;
    const producedEvent = produce.firstCall.args[0] as IEvent<any>;
    expect(producedEvent.name).to.equal('login');
    expect(producedEvent.data.userId).to.equal('u1');
  });

  it('merges the original stream methods into the returned object', () => {
    const stream = makeStream();
    const factories = {
      ping: () => ({ name: 'ping', v: '1', data: {}, time: 0 }),
    };
    const augmented = augmentEvents(factories, stream);
    expect(augmented.consume).to.equal(stream.consume);
    expect(augmented.produce).to.equal(stream.produce);
    expect(augmented.handle).to.equal(stream.handle);
  });
});
