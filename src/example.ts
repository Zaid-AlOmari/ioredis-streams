import { RedisStreams, event } from '.';

const events = {
  someEvent3: (hi: 1) => ({
    name: 'someEvent3',
    data: {
      time: hi
    },
    time: Date.now(),
    v: 'v1',
  }),
}
const stream = new RedisStreams('')
  .group('group')
  .stream('stream')
  .with({
    ...event('s').of<{ data: number }>(),
    someEvent: (hi: 1) => ({
      name: 'someEvent',
      data: {
        time: 1
      },
      time: Date.now(),
      v: 'v1',
    }),
    someEvent2: (hellow: 1) => ({
      name: 'someE3vent2',
      data: {
        w: 1
      },
      time: Date.now(),
      v: 'v1',
    }),
    ...events
  });
stream.someEvent(1);
stream.someEvent2(1);
stream.someEvent3(1);
stream
  .handle('*', async (id, event) => {
    event.data.wow
  })
  .handle('someEvent', async (id, event) => {
    event.data.time
  })
  .handle('someEvent3', async (id, event) => {
    event.data.time
  })
  .handle('s', async (id, event) => {
    event.data;
  })
  .consume().then(x => x.stop());
