# channelkit

[![All Contributors](https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square)](#contributors-)

> Zero-dependency TypeScript Go-style async channels: buffered/unbuffered `Channel<T>`, `select()`, `merge()`, `pipeline()`, `for-await` iteration, `AbortSignal` cancellation. Port of Go channels / Python `asyncio.Queue` / C# `Channel<T>`.

[![npm](https://img.shields.io/npm/v/channelkit)](https://www.npmjs.com/package/channelkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Install

```bash
npm install channelkit
```

## Quick start

```typescript
import { channel, select, merge, fromIterable, toArray } from "channelkit";

// Buffered channel
const ch = channel<number>(10);
await ch.send(1);
await ch.send(2);
await ch.recv();  // 1

// Unbuffered (rendezvous) — send blocks until recv is ready
const sync = channel<string>();
ch.recv().then(v => console.log(v));
await sync.send("hello");  // "hello" delivered immediately

// for-await iteration — terminates when channel is closed
const nums = channel<number>(5);
for (let i = 0; i < 5; i++) await nums.send(i);
nums.close();
for await (const n of nums) console.log(n);  // 0, 1, 2, 3, 4

// select — first ready channel wins
const a = channel<string>(1);
const b = channel<string>(1);
await b.send("from b");
const { value, index } = await select([a, b]);
// value = "from b", index = 1

// merge — fan-in from multiple channels
const merged = merge(ch1, ch2, ch3);
```

## Why channelkit?

Go channels are a proven pattern for typed, concurrency-safe message passing. JavaScript has `MessageChannel` (DOM, no generics, postMessage overhead) and `@repeaterjs/repeater` (async iterators, different model). Neither offers the Go-style rendezvous + buffered + `select` API that TypeScript developers coming from Go, Kotlin, or C# expect.

| Package | Downloads/week | Model | TypeScript |
|---|---|---|---|
| `@repeaterjs/repeater` | ~50k | Async generator push | ✅ |
| Node.js `stream.Readable` | built-in | Stream/callback | Partial |
| **`channelkit`** | — | Go-style channels | ✅ native |

## Features

- **Buffered channels** — `channel<T>(N)` buffers up to N values, send resolves immediately while buffer has space
- **Unbuffered channels** — `channel<T>()` (capacity 0) provides rendezvous — both sender and receiver unblock simultaneously
- **Graceful close** — `ch.close()` drains buffered values to waiting receivers; pending senders receive `ChannelClosedError`
- **`for await...of`** — channels are async iterables that terminate when closed
- **`select(channels)`** — wait for first available channel (like Go's `select`)
- **`tryRecv()` / `trySend()`** — non-blocking variants
- **`AbortSignal`** — cancel blocked `send()`/`recv()`/`select()` operations
- **Utilities** — `merge`, `pipe`, `fanOut`, `fromIterable`, `toArray`, `pipeline`, `tick`, `after`

## API

### `channel<T>(capacity = 0)`

Create a channel. `capacity = 0` (default) creates an unbuffered channel.

```typescript
const ch = channel<number>(5);   // buffered, capacity 5
const sync = channel<string>();  // unbuffered (rendezvous)
```

### `Channel<T>` instance

```typescript
ch.send(value, signal?)   // Promise<void> — blocks if full; rejects if closed
ch.recv(signal?)          // Promise<T | undefined> — undefined when closed + drained
ch.trySend(value)         // boolean — non-blocking; false if full or closed
ch.tryRecv()              // { ok: true, value } | { ok: false, done }
ch.close()                // void — close the channel
ch.closed                 // boolean
ch.length                 // current number of buffered values
ch.capacity               // maximum buffer size

for await (const v of ch) { ... }  // async iteration
```

### `select(channels, signal?)`

Wait for the first channel that has a value ready:

```typescript
const { value, index, done } = await select([ch1, ch2, ch3]);
// value: the received value (undefined if done)
// index: which channel fired
// done:  true if the channel was closed
```

### `trySelect(channels)`

Non-blocking select — returns `SelectResult | null`:

```typescript
const r = trySelect([ch1, ch2]);
if (r) console.log(r.value, r.index);
```

### Utilities

```typescript
// Pipe: forward all values from src to dst
await pipe(src, dst);

// Merge: fan-in multiple channels into one (closes when all close)
const merged = merge(ch1, ch2, ch3);

// Fan-out: broadcast each value to all destinations
await fanOut(src, d1, d2, d3);

// fromIterable: wrap any iterable into a channel
const ch = fromIterable([1, 2, 3, 4, 5]);
const ch2 = fromIterable(asyncGenerator());

// toArray: collect all values from a channel
const values = await toArray(ch);

// pipeline: transform values (like Array.map but streaming)
const doubled = pipeline(src, x => x * 2);

// tick: emit values at intervals
const timer = tick(100, 5);  // 5 ticks, 100ms each

// after: single value after delay
const done = after(1000, "timeout");
```

## Examples

### Worker pool

```typescript
import { channel, fromIterable } from "channelkit";

const jobs = fromIterable([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
const results = channel<number>(10);

// 3 concurrent workers
await Promise.all(
  Array.from({ length: 3 }, () =>
    (async () => {
      for await (const job of jobs) {
        const result = await heavyCompute(job);
        await results.send(result);
      }
    })()
  )
);
results.close();

const all = await toArray(results);
```

### Select with timeout

```typescript
import { channel, select, after } from "channelkit";

const data = channel<Response>(1);
const timeout = after(5000, null);

// Start fetching
fetch(url).then(r => data.send(r)).catch(() => data.close());

const { value, index } = await select([data, timeout]);
if (index === 0 && value) {
  // got data
} else {
  // timed out
}
```

### Pipeline stage

```typescript
import { fromIterable, pipeline, toArray } from "channelkit";

const urls = fromIterable(["https://...", "https://..."]);
const responses = pipeline(urls, url => fetch(url));
const jsons = pipeline(responses, r => r.json());

const results = await toArray(jsons);
```

### Pub/Sub fan-out

```typescript
import { channel, fanOut, fromIterable } from "channelkit";

const events = channel<string>(16);
const logger = channel<string>(16);
const metrics = channel<string>(16);
const alerts = channel<string>(16);

// Fan out to 3 consumers (all get every event)
const fanOutDone = fanOut(events, logger, metrics, alerts);

// Produce events
for (const event of eventStream) await events.send(event);
events.close();
await fanOutDone;
```

## Comparison with Go

```go
// Go
ch := make(chan int, 10)
ch <- 42
v := <-ch
close(ch)
for v := range ch { ... }
select {
  case v := <-ch1: ...
  case v := <-ch2: ...
}
```

```typescript
// channelkit
const ch = channel<number>(10);
await ch.send(42);
const v = await ch.recv();
ch.close();
for await (const v of ch) { ... }
const { value, index } = await select([ch1, ch2]);
```

## Contributors ✨

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind are welcome — code, docs, bug reports, ideas, reviews! See the [emoji key](https://allcontributors.org/docs/en/emoji-key) for how each contribution is recognized, and open a PR or issue to get involved.

Thanks goes to these wonderful people:

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/trananhtung"><img src="https://avatars.githubusercontent.com/u/30992229?v=4?s=100" width="100px;" alt="Tung Tran"/><br /><sub><b>Tung Tran</b></sub></a><br /><a href="https://github.com/trananhtung/channelkit/commits?author=trananhtung" title="Code">💻</a> <a href="#maintenance-trananhtung" title="Maintenance">🚧</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

## License

MIT © [trananhtung](https://github.com/trananhtung)
