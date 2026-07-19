#!/usr/bin/env node

let input = "";
for await (const chunk of process.stdin) input += chunk;
const invocation = JSON.parse(input);
const messageId = invocation?.receipt?.messageId;
if (typeof messageId !== "string" || messageId.length === 0) {
  throw new Error("after_receipt fixture requires receipt.messageId");
}

process.stdout.write(JSON.stringify({
  metadata: {
    hookGate: {
      afterReceipt: true,
      receiptMessageId: messageId,
    },
  },
}));
