import { StringCodec } from "nats";
import { connectNats, ensureStream, SUBJECT } from "./nats.js";

const sc = StringCodec();
const TASK_COUNT = Number(process.env.TASK_COUNT || 1000);

async function main() {
  const { nc, js, jsm } = await connectNats();
  await ensureStream(jsm);

  for (let i = 0; i < TASK_COUNT; i++) {
    const id = `task-${String(i).padStart(4, "0")}`;
    const body = JSON.stringify({ id, payload: `payload-${i}` });
    // msgID enables JetStream's own publish-side de-dup if the producer is ever re-run.
    await js.publish(SUBJECT, sc.encode(body), { msgID: id });
  }

  console.log(`seeded ${TASK_COUNT}/${TASK_COUNT}`);
  await nc.drain();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
