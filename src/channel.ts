export class ChannelClosedError extends Error {
  constructor() {
    super("send on closed channel");
    this.name = "ChannelClosedError";
  }
}

export class ChannelCancelledError extends Error {
  constructor() {
    super("channel operation cancelled");
    this.name = "ChannelCancelledError";
  }
}

interface PendingSend<T> {
  value: T;
  resolve: () => void;
  reject: (e: Error) => void;
}

interface PendingRecv<T> {
  resolve: (v: T | undefined) => void;
  reject: (e: Error) => void;
}

/**
 * A typed, optionally-buffered async channel inspired by Go channels.
 *
 * - capacity 0 (default): unbuffered — send() blocks until a recv() is ready (rendezvous).
 * - capacity N: buffered — send() resolves immediately while buffer has space; blocks when full.
 *
 * Closing a channel:
 * - All pending senders receive ChannelClosedError.
 * - All pending receivers resolve with `undefined` (drain semantics).
 * - Buffered values are still drained before receiving `undefined`.
 * - for-await loops terminate cleanly on close.
 */
export class Channel<T> {
  private readonly _cap: number;
  private readonly _buf: T[] = [];
  private readonly _senders: PendingSend<T>[] = [];
  private readonly _receivers: PendingRecv<T>[] = [];
  private _closed = false;

  constructor(capacity = 0) {
    if (!Number.isInteger(capacity) || capacity < 0) {
      throw new RangeError("Channel capacity must be a non-negative integer");
    }
    this._cap = capacity;
  }

  get closed(): boolean { return this._closed; }
  get length(): number  { return this._buf.length + this._senders.length; }
  get capacity(): number { return this._cap; }

  /**
   * Send a value. Rejects with ChannelClosedError if the channel is closed.
   * Rejects with ChannelCancelledError if the AbortSignal fires.
   */
  send(value: T, signal?: AbortSignal): Promise<void> {
    if (this._closed) return Promise.reject(new ChannelClosedError());
    if (signal?.aborted) return Promise.reject(new ChannelCancelledError());

    // Fast path: hand off directly to a waiting receiver
    if (this._receivers.length > 0) {
      const r = this._receivers.shift()!;
      r.resolve(value);
      return Promise.resolve();
    }

    // Fast path: buffer has space
    if (this._buf.length < this._cap) {
      this._buf.push(value);
      return Promise.resolve();
    }

    // Block: wait for space
    return new Promise<void>((resolve, reject) => {
      const pending: PendingSend<T> = { value, resolve, reject };
      this._senders.push(pending);

      if (signal) {
        const onAbort = () => {
          const idx = this._senders.indexOf(pending);
          if (idx !== -1) this._senders.splice(idx, 1);
          reject(new ChannelCancelledError());
        };
        signal.addEventListener("abort", onAbort, { once: true });
        // Clean up listener if send completes naturally
        const origResolve = resolve;
        pending.resolve = () => {
          signal.removeEventListener("abort", onAbort);
          origResolve();
        };
        const origReject = reject;
        pending.reject = (e: Error) => {
          signal.removeEventListener("abort", onAbort);
          origReject(e);
        };
      }
    });
  }

  /**
   * Receive a value. Resolves with the value, or `undefined` when the channel
   * is closed and drained. Rejects with ChannelCancelledError if the AbortSignal fires.
   */
  recv(signal?: AbortSignal): Promise<T | undefined> {
    if (signal?.aborted) return Promise.reject(new ChannelCancelledError());

    // Fast path: value in buffer
    if (this._buf.length > 0) {
      const value = this._buf.shift()!;
      // Unblock a waiting sender (if any) — they can now push to the buffer
      if (this._senders.length > 0) {
        const s = this._senders.shift()!;
        this._buf.push(s.value);
        s.resolve();
      }
      return Promise.resolve(value);
    }

    // Fast path: a sender is waiting (unbuffered or buffer full)
    if (this._senders.length > 0) {
      const s = this._senders.shift()!;
      s.resolve();
      return Promise.resolve(s.value);
    }

    // Channel is closed and empty
    if (this._closed) return Promise.resolve(undefined);

    // Block: wait for a sender
    return new Promise<T | undefined>((resolve, reject) => {
      const pending: PendingRecv<T> = { resolve, reject };
      this._receivers.push(pending);

      if (signal) {
        const onAbort = () => {
          const idx = this._receivers.indexOf(pending);
          if (idx !== -1) this._receivers.splice(idx, 1);
          reject(new ChannelCancelledError());
        };
        signal.addEventListener("abort", onAbort, { once: true });
        const origResolve = pending.resolve;
        pending.resolve = (v: T | undefined) => {
          signal.removeEventListener("abort", onAbort);
          origResolve(v);
        };
        const origReject = pending.reject;
        pending.reject = (e: Error) => {
          signal.removeEventListener("abort", onAbort);
          origReject(e);
        };
      }
    });
  }

  /**
   * Non-blocking receive. Returns { ok: true, value } if a value is available,
   * or { ok: false } if the channel is empty.
   */
  tryRecv(): { ok: true; value: T } | { ok: false; done: boolean } {
    if (this._buf.length > 0) {
      const value = this._buf.shift()!;
      if (this._senders.length > 0) {
        const s = this._senders.shift()!;
        this._buf.push(s.value);
        s.resolve();
      }
      return { ok: true, value };
    }
    if (this._senders.length > 0) {
      const s = this._senders.shift()!;
      s.resolve();
      return { ok: true, value: s.value };
    }
    return { ok: false, done: this._closed };
  }

  /**
   * Non-blocking send. Returns true if sent, false if the channel is full or closed.
   */
  trySend(value: T): boolean {
    if (this._closed) return false;
    if (this._receivers.length > 0) {
      const r = this._receivers.shift()!;
      r.resolve(value);
      return true;
    }
    if (this._buf.length < this._cap) {
      this._buf.push(value);
      return true;
    }
    return false;
  }

  /**
   * Close the channel. All pending senders receive ChannelClosedError.
   * Pending receivers drain remaining buffered values, then receive undefined.
   */
  close(): void {
    if (this._closed) return;
    this._closed = true;

    // Drain buffered values to waiting receivers
    while (this._receivers.length > 0 && this._buf.length > 0) {
      this._receivers.shift()!.resolve(this._buf.shift()!);
    }

    // Remaining receivers: resolve with undefined (closed)
    for (const r of this._receivers.splice(0)) r.resolve(undefined);

    // Pending senders: reject with ChannelClosedError
    for (const s of this._senders.splice(0)) s.reject(new ChannelClosedError());
  }

  /** Async iteration — yields all values until the channel is closed. */
  async *[Symbol.asyncIterator](): AsyncGenerator<T, void, undefined> {
    while (true) {
      const v = await this.recv();
      if (v === undefined) return;
      yield v;
    }
  }
}

/** Create a buffered or unbuffered channel. */
export function channel<T>(capacity = 0): Channel<T> {
  return new Channel<T>(capacity);
}
