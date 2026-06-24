import { Channel, ChannelCancelledError } from "./channel.js";

export interface SelectCase<T> {
  channel: Channel<T>;
}

export interface SelectResult<T> {
  value: T | undefined;
  index: number;
  done: boolean;
}

/**
 * Select waits for the first channel that has a value ready and returns it.
 * Similar to Go's `select { case v := <-ch1: ... case v := <-ch2: ... }`.
 *
 * Returns the received value, the index of the channel that fired, and whether
 * the channel was closed (done=true when value is undefined).
 *
 * If multiple channels are immediately ready, one is chosen deterministically
 * (the first one with a pending value).
 */
export async function select<T>(
  channels: Channel<T>[],
  signal?: AbortSignal
): Promise<SelectResult<T>> {
  if (channels.length === 0) throw new RangeError("select() requires at least one channel");
  if (signal?.aborted) throw new ChannelCancelledError();

  // Try non-blocking first
  for (let i = 0; i < channels.length; i++) {
    const r = channels[i].tryRecv();
    if (r.ok) return { value: r.value, index: i, done: false };
    if (!r.ok && r.done) return { value: undefined, index: i, done: true };
  }

  // Block on all channels simultaneously — first to resolve wins
  const ac = new AbortController();
  const cleanup = () => ac.abort();

  if (signal) signal.addEventListener("abort", cleanup, { once: true });

  return new Promise<SelectResult<T>>((resolve, reject) => {
    let settled = false;

    if (signal) {
      signal.addEventListener("abort", () => {
        if (!settled) {
          settled = true;
          reject(new ChannelCancelledError());
        }
      }, { once: true });
    }

    for (let i = 0; i < channels.length; i++) {
      const idx = i;
      channels[idx].recv(ac.signal).then(
        (value) => {
          if (!settled) {
            settled = true;
            ac.abort(); // cancel remaining
            if (signal) signal.removeEventListener("abort", cleanup);
            resolve({ value, index: idx, done: value === undefined });
          }
        },
        (err) => {
          // ChannelCancelledError from our own abort — ignore; it means another ch won
          if (err instanceof ChannelCancelledError && !signal?.aborted) return;
          if (!settled) {
            settled = true;
            ac.abort();
            if (signal) signal.removeEventListener("abort", cleanup);
            reject(err);
          }
        }
      );
    }
  });
}

/**
 * Non-blocking select — tries each channel once and returns the first ready value.
 * Returns null if no channel has a value immediately available.
 */
export function trySelect<T>(channels: Channel<T>[]): SelectResult<T> | null {
  for (let i = 0; i < channels.length; i++) {
    const r = channels[i].tryRecv();
    if (r.ok) return { value: r.value, index: i, done: false };
    if (!r.ok && r.done) return { value: undefined, index: i, done: true };
  }
  return null;
}
