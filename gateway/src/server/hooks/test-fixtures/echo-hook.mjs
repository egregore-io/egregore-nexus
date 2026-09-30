let input = "";
for await (const chunk of process.stdin) input += chunk;
const invocation = JSON.parse(input);
process.stdout.write(JSON.stringify({
  action: "continue",
  metadata: {
    runtime: "javascript",
    invocationId: invocation.invocationId,
    body: invocation.message.body,
    environment: Object.keys(process.env).sort(),
    allowedValue: process.env.HOOK_ALLOWED ?? null,
  },
}));
