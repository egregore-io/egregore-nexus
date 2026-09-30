#!/usr/bin/env node
import { mkdir, open, readFile, writeFile } from "node:fs/promises";

const [scenario = "happy", envCapture, startCount, stateRoot] = process.argv.slice(2);
if (stateRoot) await mkdir(stateRoot, { recursive: true, mode: 0o700 });
if (envCapture) await writeFile(envCapture, `${JSON.stringify(process.env)}\n`, { mode: 0o600 });
if (startCount) {
  const current = await readFile(startCount, "utf8").then(Number).catch(() => 0);
  await writeFile(startCount, String(current + 1), { mode: 0o600 });
}

send({
  t: "transport/hello",
  protocolVersion: scenario === "unsupported" ? 99 : 1,
});

let buffer = "";
const held = [];
let capacityReached = false;
process.stdin.setEncoding("utf8");
process.stdin.on("data", async (chunk) => {
  buffer += chunk;
  while (true) {
    const newline = buffer.indexOf("\n");
    if (newline < 0) break;
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (!line) continue;
    const frame = JSON.parse(line);
    if (frame.t === "transport/init" && scenario === "happy") {
      send({
        t: "transport/bind",
        external: { userId: "user-1", displayName: "External One" },
      });
      const ingress = {
        t: "transport/ingress",
        ingressId: "ingress-1",
        external: { userId: "user-1", displayName: "External One" },
        chatId: "group-1",
        text: "hello from bridge",
      };
      send(ingress);
      send(ingress);
      send({
        t: "transport/log",
        level: "info",
        message: `bridge token=${process.env.FAKE_TOKEN ?? "missing"}`,
      });
    } else if (frame.t === "transport/init" && scenario === "dm-first-contact") {
      send({
        t: "transport/ingress",
        ingressId: "private-ingress-1",
        external: { userId: "private-user", displayName: "Private User" },
        chatId: "private-chat-1",
        text: "hello privately",
      });
    } else if (frame.t === "transport/init" && scenario === "bind-restart") {
      const starts = Number(await awaitText(startCount, "0"));
      if (starts === 1) {
        send({
          t: "transport/bindLane",
          external: { chatId: "restart-chat" },
          lane: { kind: "thread", name: "restart-thread" },
        });
        setTimeout(() => process.exit(17), 20);
      } else {
        send({
          t: "transport/ingress",
          ingressId: "restart-ingress",
          external: { userId: "restart-user", displayName: "Restart User" },
          chatId: "restart-chat",
          text: "after restart",
        });
      }
    } else if (frame.t === "transport/init" && scenario === "crash-loop") {
      process.exit(21);
    } else if (frame.t === "transport/init" && scenario === "overflow") {
      process.stdout.write(`${"x".repeat(65 * 1024)}\n`);
    } else if (frame.t === "transport/deliver" && scenario === "journal-crash") {
      void journalDelivery(frame);
    } else if (frame.t === "transport/deliver" && scenario === "slow") {
      held.push(frame);
      void appendLine(`${stateRoot}/deliveries.jsonl`, JSON.stringify(frame));
      if (held.length === 256) {
        capacityReached = true;
        void writeFile(`${stateRoot}/first-batch.txt`, "256", { mode: 0o600 });
        setTimeout(() => {
          for (const delivery of held.splice(0)) {
            const receipt = {
              t: "transport/receipt",
              obligationId: delivery.obligationId,
              externalMessageId: `external-${delivery.obligationId}`,
            };
            send(receipt);
            send(receipt);
          }
        }, 25);
      } else if (capacityReached && held.length > 0 && held.length < 256) {
        setTimeout(() => {
          for (const delivery of held.splice(0)) {
            send({
              t: "transport/receipt",
              obligationId: delivery.obligationId,
              externalMessageId: `external-${delivery.obligationId}`,
            });
          }
        }, 25);
      }
    } else if (frame.t === "transport/deliver") {
      send({
        t: "transport/receipt",
        obligationId: frame.obligationId,
        externalMessageId: `external-${frame.obligationId}`,
      });
    } else if (frame.t === "transport/shutdown") {
      process.exit(0);
    }
  }
});

function send(frame) {
  process.stdout.write(`${JSON.stringify(frame)}\n`);
}

async function journalDelivery(frame) {
  const journalPath = `${stateRoot}/journal.jsonl`;
  const callsPath = `${stateRoot}/provider-calls.txt`;
  const crashPath = `${stateRoot}/crashed-once`;
  const journal = await awaitText(journalPath, "");
  const cached = journal.split("\n").filter(Boolean).map(JSON.parse)
    .find((row) => row.obligationId === frame.obligationId);
  if (cached) {
    send({
      t: "transport/receipt",
      obligationId: cached.obligationId,
      externalMessageId: cached.externalMessageId,
    });
    return;
  }
  const calls = Number(await awaitText(callsPath, "0")) + 1;
  await writeFile(callsPath, String(calls), { mode: 0o600 });
  const row = {
    obligationId: frame.obligationId,
    externalMessageId: `provider-${calls}`,
  };
  await appendLine(journalPath, JSON.stringify(row));
  const crashed = await readFile(crashPath).then(() => true).catch(() => false);
  if (!crashed) {
    await writeFile(crashPath, "1", { mode: 0o600 });
    process.exit(23);
  }
  send({ t: "transport/receipt", ...row });
}

async function appendLine(path, line) {
  const handle = await open(path, "a", 0o600);
  try {
    await handle.writeFile(`${line}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function awaitText(path, fallback) {
  return readFile(path, "utf8").catch(() => fallback);
}
