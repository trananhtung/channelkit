import {
  channel, Channel, ChannelClosedError, ChannelCancelledError,
  select, trySelect,
  pipe, merge, fanOut, fromIterable, toArray, after, pipeline,
} from "../src/index.js";

// ── 1. buffered channel basics ────────────────────────────────────────────────

describe("buffered channel", () => {
  test("send then recv within capacity", async () => {
    const ch = channel<number>(3);
    await ch.send(1);
    await ch.send(2);
    await ch.send(3);
    expect(await ch.recv()).toBe(1);
    expect(await ch.recv()).toBe(2);
    expect(await ch.recv()).toBe(3);
  });

  test("length tracks occupancy", async () => {
    const ch = channel<number>(5);
    expect(ch.length).toBe(0);
    await ch.send(10);
    expect(ch.length).toBe(1);
    await ch.recv();
    expect(ch.length).toBe(0);
  });

  test("capacity is correct", () => {
    expect(channel<number>(4).capacity).toBe(4);
  });

  test("send blocks when full, unblocks when recv drains", async () => {
    const ch = channel<number>(1);
    await ch.send(1);

    let unblocked = false;
    const sendPromise = ch.send(2).then(() => { unblocked = true; });

    // Not yet unblocked — no consumer yet
    expect(unblocked).toBe(false);

    // Consume one value
    await ch.recv();
    await sendPromise;
    expect(unblocked).toBe(true);
  });

  test("FIFO order preserved", async () => {
    const ch = channel<number>(10);
    for (let i = 0; i < 5; i++) await ch.send(i);
    const result: number[] = [];
    for (let i = 0; i < 5; i++) result.push(await ch.recv() as number);
    expect(result).toEqual([0, 1, 2, 3, 4]);
  });
});

// ── 2. unbuffered channel (rendezvous) ────────────────────────────────────────

describe("unbuffered channel (capacity 0)", () => {
  test("send blocks until recv is ready", async () => {
    const ch = channel<number>();
    let sent = false;
    const sendP = ch.send(42).then(() => { sent = true; });

    // Not yet resolved — no receiver
    await Promise.resolve(); // microtask
    expect(sent).toBe(false);

    const v = await ch.recv();
    await sendP;
    expect(v).toBe(42);
    expect(sent).toBe(true);
  });

  test("recv blocks until send is ready", async () => {
    const ch = channel<string>();
    const recvP = ch.recv();
    await ch.send("hello");
    expect(await recvP).toBe("hello");
  });

  test("multiple rendezvous in sequence", async () => {
    const ch = channel<number>();
    const results: number[] = [];
    for (let i = 0; i < 3; i++) {
      const recvP = ch.recv();
      await ch.send(i);
      results.push(await recvP as number);
    }
    expect(results).toEqual([0, 1, 2]);
  });
});

// ── 3. close semantics ────────────────────────────────────────────────────────

describe("close()", () => {
  test("recv returns undefined after close+drain", async () => {
    const ch = channel<number>(3);
    await ch.send(1);
    await ch.send(2);
    ch.close();
    expect(await ch.recv()).toBe(1);
    expect(await ch.recv()).toBe(2);
    expect(await ch.recv()).toBeUndefined();  // drained
  });

  test("send after close throws ChannelClosedError", async () => {
    const ch = channel<number>(1);
    ch.close();
    await expect(ch.send(1)).rejects.toBeInstanceOf(ChannelClosedError);
  });

  test("pending sender gets ChannelClosedError on close", async () => {
    const ch = channel<number>(0);
    const sendP = ch.send(99);
    ch.close();
    await expect(sendP).rejects.toBeInstanceOf(ChannelClosedError);
  });

  test("pending receiver gets undefined on close", async () => {
    const ch = channel<number>(0);
    const recvP = ch.recv();
    ch.close();
    expect(await recvP).toBeUndefined();
  });

  test("closed property", () => {
    const ch = channel<number>();
    expect(ch.closed).toBe(false);
    ch.close();
    expect(ch.closed).toBe(true);
  });

  test("double close is a no-op", () => {
    const ch = channel<number>();
    ch.close();
    expect(() => ch.close()).not.toThrow();
  });
});

// ── 4. for-await iteration ────────────────────────────────────────────────────

describe("for-await iteration", () => {
  test("iterates all values then terminates", async () => {
    const ch = channel<number>(5);
    for (let i = 0; i < 5; i++) await ch.send(i);
    ch.close();
    const result: number[] = [];
    for await (const v of ch) result.push(v);
    expect(result).toEqual([0, 1, 2, 3, 4]);
  });

  test("concurrent producer/consumer via for-await", async () => {
    const ch = channel<number>(2);
    const results: number[] = [];

    const producer = (async () => {
      for (let i = 0; i < 5; i++) await ch.send(i);
      ch.close();
    })();

    const consumer = (async () => {
      for await (const v of ch) results.push(v);
    })();

    await Promise.all([producer, consumer]);
    expect(results).toEqual([0, 1, 2, 3, 4]);
  });
});

// ── 5. tryRecv / trySend ──────────────────────────────────────────────────────

describe("tryRecv / trySend", () => {
  test("tryRecv on empty channel returns ok:false", () => {
    const ch = channel<number>(1);
    const r = ch.tryRecv();
    expect(r.ok).toBe(false);
  });

  test("tryRecv on channel with value returns ok:true", async () => {
    const ch = channel<number>(1);
    await ch.send(99);
    const r = ch.tryRecv();
    expect(r.ok).toBe(true);
    if (r.ok) expect(r.value).toBe(99);
  });

  test("trySend on full channel returns false", async () => {
    const ch = channel<number>(1);
    await ch.send(1);
    expect(ch.trySend(2)).toBe(false);
  });

  test("trySend on channel with space returns true", () => {
    const ch = channel<number>(2);
    expect(ch.trySend(1)).toBe(true);
    expect(ch.trySend(2)).toBe(true);
  });

  test("trySend on closed returns false", () => {
    const ch = channel<number>(5);
    ch.close();
    expect(ch.trySend(1)).toBe(false);
  });
});

// ── 6. AbortSignal cancellation ───────────────────────────────────────────────

describe("AbortSignal cancellation", () => {
  test("recv cancels via AbortSignal", async () => {
    const ch = channel<number>();
    const ac = new AbortController();
    const recvP = ch.recv(ac.signal);
    ac.abort();
    await expect(recvP).rejects.toBeInstanceOf(ChannelCancelledError);
  });

  test("send cancels via AbortSignal when blocked", async () => {
    const ch = channel<number>(0);
    const ac = new AbortController();
    const sendP = ch.send(1, ac.signal);
    ac.abort();
    await expect(sendP).rejects.toBeInstanceOf(ChannelCancelledError);
  });

  test("pre-aborted signal rejects immediately", async () => {
    const ac = new AbortController();
    ac.abort();
    const ch = channel<number>();
    await expect(ch.recv(ac.signal)).rejects.toBeInstanceOf(ChannelCancelledError);
    await expect(ch.send(1, ac.signal)).rejects.toBeInstanceOf(ChannelCancelledError);
  });
});

// ── 7. select ────────────────────────────────────────────────────────────────

describe("select()", () => {
  test("returns first ready channel", async () => {
    const ch1 = channel<number>(1);
    const ch2 = channel<number>(1);
    await ch1.send(10);
    const r = await select([ch1, ch2]);
    expect(r.value).toBe(10);
    expect(r.index).toBe(0);
  });

  test("select waits for first available", async () => {
    const ch1 = channel<number>();
    const ch2 = channel<number>();

    const selP = select([ch1, ch2]);
    await ch2.send(42);
    const r = await selP;
    expect(r.value).toBe(42);
    expect(r.index).toBe(1);
  });

  test("select returns done:true when channel closes", async () => {
    const ch1 = channel<number>();
    const ch2 = channel<number>();
    const selP = select([ch1, ch2]);
    ch1.close();
    const r = await selP;
    expect(r.done).toBe(true);
    expect(r.value).toBeUndefined();
  });

  test("select cancels via AbortSignal", async () => {
    const ch1 = channel<number>();
    const ch2 = channel<number>();
    const ac = new AbortController();
    const selP = select([ch1, ch2], ac.signal);
    ac.abort();
    await expect(selP).rejects.toBeInstanceOf(ChannelCancelledError);
  });
});

// ── 8. trySelect ──────────────────────────────────────────────────────────────

describe("trySelect()", () => {
  test("returns first ready value", async () => {
    const ch1 = channel<number>(1);
    const ch2 = channel<number>(1);
    await ch2.send(99);
    const r = trySelect([ch1, ch2]);
    expect(r).not.toBeNull();
    expect(r!.value).toBe(99);
    expect(r!.index).toBe(1);
  });

  test("returns null when all channels empty", () => {
    const ch1 = channel<number>(1);
    const ch2 = channel<number>(1);
    expect(trySelect([ch1, ch2])).toBeNull();
  });
});

// ── 9. utility functions ─────────────────────────────────────────────────────

describe("pipe()", () => {
  test("forwards values from src to dst", async () => {
    const src = channel<number>(5);
    const dst = channel<number>(5);
    for (let i = 0; i < 3; i++) await src.send(i);
    src.close();
    await pipe(src, dst);
    dst.close();
    expect(await toArray(dst)).toEqual([0, 1, 2]);
  });
});

describe("merge()", () => {
  test("merges multiple channels", async () => {
    const ch1 = channel<number>(3);
    const ch2 = channel<number>(3);
    await ch1.send(1); await ch1.send(3);
    await ch2.send(2); await ch2.send(4);
    ch1.close(); ch2.close();
    const merged = merge(ch1, ch2);
    const result = (await toArray(merged)).sort((a, b) => a - b);
    expect(result).toEqual([1, 2, 3, 4]);
  });

  test("empty merge closes immediately", async () => {
    const ch = merge<number>();
    const result = await toArray(ch);
    expect(result).toEqual([]);
  });
});

describe("fromIterable()", () => {
  test("wraps array into channel", async () => {
    const ch = fromIterable([1, 2, 3, 4, 5]);
    expect(await toArray(ch)).toEqual([1, 2, 3, 4, 5]);
  });

  test("wraps async generator", async () => {
    async function* gen() { yield 10; yield 20; yield 30; }
    const ch = fromIterable(gen());
    expect(await toArray(ch)).toEqual([10, 20, 30]);
  });
});

describe("after()", () => {
  test("sends value after delay and closes", async () => {
    const ch = after(10, "done");
    expect(await ch.recv()).toBe("done");
    expect(await ch.recv()).toBeUndefined();
  });
});

describe("pipeline()", () => {
  test("transforms values from src to output", async () => {
    const src = fromIterable([1, 2, 3, 4]);
    const out = pipeline(src, (x) => x * x);
    expect(await toArray(out)).toEqual([1, 4, 9, 16]);
  });

  test("async transform", async () => {
    const src = fromIterable([1, 2, 3]);
    const out = pipeline(src, async (x) => x + 10);
    expect(await toArray(out)).toEqual([11, 12, 13]);
  });
});

describe("fanOut()", () => {
  test("broadcasts to multiple destinations", async () => {
    const src = channel<number>(5);
    const d1 = channel<number>(5);
    const d2 = channel<number>(5);
    for (let i = 0; i < 3; i++) await src.send(i);
    src.close();
    await fanOut(src, d1, d2);
    expect(await toArray(d1)).toEqual([0, 1, 2]);
    expect(await toArray(d2)).toEqual([0, 1, 2]);
  });
});

// ── 10. real-world: worker pool ───────────────────────────────────────────────

describe("worker pool pattern", () => {
  test("process N jobs with M workers", async () => {
    const jobs = channel<number>(10);
    const results = channel<number>(10);

    // Produce 10 jobs
    for (let i = 1; i <= 10; i++) await jobs.send(i);
    jobs.close();

    // 3 workers
    const workers = Array.from({ length: 3 }, () =>
      (async () => {
        for await (const job of jobs) {
          await results.send(job * job);
        }
      })()
    );

    await Promise.all(workers);
    results.close();

    const out = await toArray(results);
    const expected = [1, 4, 9, 16, 25, 36, 49, 64, 81, 100];
    expect(out.sort((a, b) => a - b)).toEqual(expected);
  });
});

// ── 11. real-world: rate-limited pipeline ─────────────────────────────────────

describe("producer-consumer", () => {
  test("producer/consumer with buffered channel", async () => {
    const ch = channel<number>(4);
    const consumed: number[] = [];

    const producer = (async () => {
      for (let i = 0; i < 8; i++) await ch.send(i);
      ch.close();
    })();

    const consumer = (async () => {
      for await (const v of ch) consumed.push(v);
    })();

    await Promise.all([producer, consumer]);
    expect(consumed).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  });
});

// ── 12. error cases ───────────────────────────────────────────────────────────

describe("error cases", () => {
  test("negative capacity throws", () => {
    expect(() => channel(-1)).toThrow(RangeError);
  });

  test("float capacity throws", () => {
    expect(() => channel(1.5)).toThrow(RangeError);
  });

  test("select on empty array throws", async () => {
    await expect(select([])).rejects.toThrow(RangeError);
  });
});
