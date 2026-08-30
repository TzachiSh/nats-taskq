import { connect } from "nats";

export const STREAM = "TASKS";
export const SUBJECT = "tasks.jobs";
export const CONSUMER = "workers";
export const KV_BUCKET = "completed";

const NATS_URL = process.env.NATS_URL || "nats://127.0.0.1:4222";

export async function connectNats() {
  const nc = await connect({ servers: NATS_URL });
  const jsm = await nc.jetstreamManager();
  const js = nc.jetstream();
  return { nc, js, jsm };
}

export async function ensureStream(jsm) {
  try {
    await jsm.streams.info(STREAM);
  } catch {
    await jsm.streams.add({
      name: STREAM,
      subjects: [SUBJECT],
      retention: "workqueue",
    });
  }
}

export async function ensureConsumer(jsm) {
  try {
    await jsm.consumers.info(STREAM, CONSUMER);
  } catch {
    await jsm.consumers.add(STREAM, {
      durable_name: CONSUMER,
      ack_policy: "explicit",
      max_deliver: -1,
      ack_wait: 10_000_000_000, // 10s, in nanoseconds
    });
  }
}

export async function ensureKv(js) {
  return js.views.kv(KV_BUCKET, { history: 1 });
}
