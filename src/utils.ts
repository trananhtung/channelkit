import { Channel, channel } from "./channel.js";

/**
 * Pipe: forward all values from `src` to `dst` until `src` is closed.
 * Does not close `dst` — allows multiple producers.
 */
export async function pipe<T>(src: Channel<T>, dst: Channel<T>): Promise<void> {
  for await (const v of src) await dst.send(v);
}

/**
 * Merge: fan-in N source channels into one output channel.
 * The output channel is closed when all sources are closed.
 */
export function merge<T>(...sources: Channel<T>[]): Channel<T> {
  const out = channel<T>(16);
  let remaining = sources.length;
  if (remaining === 0) { out.close(); return out; }
  for (const src of sources) {
    (async () => {
      for await (const v of src) await out.send(v);
      if (--remaining === 0) out.close();
    })().catch(() => { if (--remaining === 0) out.close(); });
  }
  return out;
}

/**
 * Fan-out: broadcast each value from `src` to all `destinations`.
 * Closes all destinations when `src` is closed.
 */
export async function fanOut<T>(src: Channel<T>, ...destinations: Channel<T>[]): Promise<void> {
  for await (const v of src) {
    await Promise.all(destinations.map((d) => d.send(v)));
  }
  for (const d of destinations) d.close();
}

/**
 * fromIterable: wrap a sync or async iterable into a channel.
 * The channel is closed after all values have been sent.
 */
export function fromIterable<T>(iter: Iterable<T> | AsyncIterable<T>, capacity = 0): Channel<T> {
  const ch = channel<T>(capacity);
  (async () => {
    for await (const v of iter as AsyncIterable<T>) await ch.send(v);
    ch.close();
  })().catch(() => ch.close());
  return ch;
}

/**
 * toArray: collect all values from a channel until it closes.
 */
export async function toArray<T>(ch: Channel<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const v of ch) result.push(v);
  return result;
}

/**
 * tick: produce a value every `intervalMs` milliseconds until signal aborts or count reached.
 * Returns a channel that emits numbers (tick count, starting from 0).
 */
export function tick(intervalMs: number, count = Infinity, signal?: AbortSignal): Channel<number> {
  const ch = channel<number>(1);
  let n = 0;
  const timer = setInterval(() => {
    if (signal?.aborted || n >= count) {
      clearInterval(timer);
      ch.close();
      return;
    }
    ch.trySend(n++);
  }, intervalMs);
  if (signal) {
    signal.addEventListener("abort", () => { clearInterval(timer); ch.close(); }, { once: true });
  }
  return ch;
}

/**
 * after: send a single value after `delayMs` ms, then close.
 */
export function after<T>(delayMs: number, value: T): Channel<T> {
  const ch = channel<T>(1);
  setTimeout(() => { ch.trySend(value); ch.close(); }, delayMs);
  return ch;
}

/**
 * pipeline: chain transforms. Each stage receives values from the previous channel
 * and sends transformed values to the next. Returns the final output channel.
 */
export function pipeline<T, U>(
  src: Channel<T>,
  transform: (value: T) => Promise<U> | U,
  capacity = 0
): Channel<U> {
  const out = channel<U>(capacity);
  (async () => {
    for await (const v of src) await out.send(await transform(v));
    out.close();
  })().catch(() => out.close());
  return out;
}
