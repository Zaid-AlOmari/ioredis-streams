import sinon from 'sinon';
import { IRedisClient } from '../../src/redis-client';

export type FakeRedis = IRedisClient & {
  xgroup: sinon.SinonStub;
  xreadgroup: sinon.SinonStub;
  xpending: sinon.SinonStub;
  xclaim: sinon.SinonStub;
  xadd: sinon.SinonStub;
  xack: sinon.SinonStub;
  publish: sinon.SinonStub;
  subscribe: sinon.SinonStub;
  unsubscribe: sinon.SinonStub;
  on: sinon.SinonStub;
  off: sinon.SinonStub;
  disconnect: sinon.SinonStub;
  pipeline: sinon.SinonStub;
  multi: sinon.SinonStub;
  exec: sinon.SinonStub;
};

export function createFakeRedis(): FakeRedis {
  const createPipeline = (): any => {
    const pipe: any = {};
    pipe.xadd = sinon.stub().callsFake(() => pipe);
    pipe.exec = sinon.stub().resolves([]);
    pipe.xgroup = sinon.stub().resolves('OK');
    pipe.xpending = sinon.stub().resolves([]);
    pipe.xclaim = sinon.stub().resolves([]);
    pipe.xack = sinon.stub().resolves(1);
    pipe.publish = sinon.stub().resolves(1);
    pipe.subscribe = sinon.stub().resolves();
    pipe.unsubscribe = sinon.stub().resolves();
    pipe.xreadgroup = sinon.stub().resolves(null);
    pipe.on = sinon.stub().returnsThis();
    pipe.off = sinon.stub().returnsThis();
    pipe.disconnect = sinon.stub();
    pipe.pipeline = sinon.stub().callsFake(createPipeline);
    pipe.multi = sinon.stub().callsFake(createPipeline);
    return pipe;
  };

  const client: any = {};
  client.xgroup = sinon.stub().resolves('OK');
  client.xreadgroup = sinon.stub().resolves(null);
  client.xpending = sinon.stub().resolves([]);
  client.xclaim = sinon.stub().resolves([]);
  client.xadd = sinon.stub().callsFake(() => client);
  client.xack = sinon.stub().resolves(1);
  client.publish = sinon.stub().resolves(1);
  client.subscribe = sinon.stub().resolves();
  client.unsubscribe = sinon.stub().resolves();
  client.on = sinon.stub().returnsThis();
  client.off = sinon.stub().returnsThis();
  client.disconnect = sinon.stub();
  client.pipeline = sinon.stub().callsFake(createPipeline);
  client.multi = sinon.stub().callsFake(createPipeline);
  client.exec = sinon.stub().resolves([]);
  return client as FakeRedis;
}
