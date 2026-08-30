import assert from "node:assert/strict";
import { StringCodec } from "nats";
import {
  connectNats,
  ensureStream,
  ensureConsumer,
  ensureKv,
  STREAM,
  CONSUMER,
  SUBJECT,
} from "../src/nats.js";

const sc = StringCodec();

async function main() {
  const { nc, js, jsm } = await connectNats();
  await ensureStream(jsm);
  await ensureConsumer(jsm);
  const kv = await ensureKv(js);

  // publish 7, fetch in a capped batch of 5 -- exercises the exact fetch() call worker.js uses
  for (let i = 0; i < 7; i++) {
    await js.publish(SUBJECT, sc.encode(JSON.stringify({ id: `chk-${i}` })), {
      msgID: `chk-${i}`,
    });
  }
  const consumer = await js.consumers.get(STREAM, CONSUMER);
  const batch = await consumer.fetch({ max_messages: 5, expires: 2000 });
  let n = 0;
  const msgs = [];
  for await (const m of batch) {
    n++;
    msgs.push(m);
  }
  assert.equal(n, 5, "fetch respected max_messages cap");

  // ack 4, nak 1 with delay -- exercise both paths
  for (let i = 0; i < 4; i++) msgs[i].ack();
  msgs[4].nak(200);

  // kv atomic create-if-absent
  await kv.create("dedupe-check", sc.encode("first"));
  await assert.rejects(
    () => kv.create("dedupe-check", sc.encode("second")),
    "kv.create rejects on existing key",
  );
  const got = await kv.get("dedupe-check");
  assert.equal(sc.decode(got.value), "first");

  const batch2 = await consumer.fetch({ max_messages: 5, expires: 2500 });
  let n2 = 0;
  for await (const m of batch2) {
    n2++;
    m.ack();
  }
  assert.equal(
    n2,
    3,
    "remaining 3 messages arrive on next pull: the naked redelivery plus the 2 never fetched",
  );

  console.log("api-check OK");
  await nc.drain();
  process.exit(0);
}

main().catch((err) => {
  console.error("api-check FAILED", err);
  process.exit(1);
});
