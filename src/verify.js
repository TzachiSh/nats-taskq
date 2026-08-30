import { connectNats, ensureKv } from "./nats.js";

const TARGET = Number(process.env.TASK_COUNT || 1000);
const TIMEOUT_MS = Number(process.env.VERIFY_TIMEOUT_MS || 120000);
const POLL_MS = 1000;

async function countKeys(kv) {
  const keys = await kv.keys();
  let n = 0;
  for await (const _ of keys) n++;
  return n;
}

async function main() {
  const { nc, js } = await connectNats();
  const kv = await ensureKv(js);

  const start = Date.now();
  let count = 0;
  while (Date.now() - start < TIMEOUT_MS) {
    count = await countKeys(kv);
    console.log(`progress ${count}/${TARGET}`);
    if (count >= TARGET) break;
    await new Promise((r) => setTimeout(r, POLL_MS));
  }

  await nc.drain();
  if (count >= TARGET) {
    console.log(`DONE ${count}/${TARGET}`);
    process.exit(0);
  } else {
    console.log(`INCOMPLETE ${count}/${TARGET}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
