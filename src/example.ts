import { RedisStreams } from '.';

const stream = new RedisStreams('')
  .group('group')
  .stream('stream')
  .with({
    someEvent: (hi: 1) => ({
      name: 'someEvent',
      data: {
        time: 1
      },
      time: Date.now(),
      v: 'v1',
    }),
    someEvent2: (hellow: 1) => ({
      name: 'someEvent2',
      data: {
        w: 1
      },
      time: Date.now(),
      v: 'v1',
    })
  });
stream.someEvent(1);
stream.someEvent2(1)
stream
  .handle('*', async (id, event) => {
    event.data.wow
  })
  .handle('someEvent', async (id, event) => {
    event.data.time
  })
  .handle('someEvent2', async (id, event) => {
    event.data.w
  })
  .consume().then(x => x.stop());
