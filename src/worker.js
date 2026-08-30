import os from "node:os";
import { StringCodec } from "nats";
import {
  connectNats,
  ensureStream,
  ensureConsumer,
  ensureKv,
  STREAM,
  CONSUMER,
} from "./nats.js";

const sc = StringCodec();
const WORKER_ID = process.env.WORKER_ID || os.hostname();
const MAX_INFLIGHT = Number(process.env.MAX_INFLIGHT || 5);
const FAIL_RATE = Number(process.env.FAIL_RATE ?? 0.1);
const PROCESS_MS = Number(process.env.PROCESS_MS || 500);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (event, id) =>
  console.log(`[worker ${WORKER_ID}] ${event} id=${id}`);

async function handle(m, kv) {
  const { id } = JSON.parse(sc.decode(m.data));
  log("picked", id);
  try {
    // Idempotency guard: a task already recorded done was fully processed by
    // an earlier delivery whose ack never reached the server. Skip re-running it.
    const already = await kv.get(id);
    if (already) {
      log("duplicate-skip", id);
      m.ack();
      return;
    }

    await sleep(PROCESS_MS); // simulate the work

    if (Math.random() < FAIL_RATE) {
      log("failed", id);
      m.nak(200); // retry soon
      return;
    }

    try {
      await kv.create(
        id,
        sc.encode(
          JSON.stringify({ workerId: WORKER_ID, ts: new Date().toISOString() }),
        ),
      );
    } catch {
      // Lost a race with another delivery of the same id -- it is already
      // recorded as done; still ack so this delivery isn't redelivered again.
    }
    log("done", id);
    m.ack();
  } catch (err) {
    console.error(`[worker ${WORKER_ID}] error id=${id}`, err);
    m.nak(500);
  }
}

async function main() {
  const { nc, js, jsm } = await connectNats();
  await ensureStream(jsm);
  await ensureConsumer(jsm);
  const kv = await ensureKv(js);
  const consumer = await js.consumers.get(STREAM, CONSUMER);

  let inFlight = 0;
  let stopping = false;
  process.once("SIGINT", () => {
    stopping = true;
  });
  process.once("SIGTERM", () => {
    stopping = true;
  });

  while (!stopping) {
    const capacity = MAX_INFLIGHT - inFlight;
    if (capacity <= 0) {
      await sleep(50);
      continue;
    }
    let batch;
    try {
      batch = await consumer.fetch({ max_messages: capacity, expires: 1000 });
    } catch (err) {
      console.error(`[worker ${WORKER_ID}] fetch error`, err);
      await sleep(500);
      continue;
    }
    for await (const m of batch) {
      inFlight++;
      handle(m, kv).finally(() => {
        inFlight--;
      });
    }
  }

  while (inFlight > 0) await sleep(50);
  await nc.drain();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
