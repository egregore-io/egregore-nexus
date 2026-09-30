import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";

type WsModule = typeof import("./ws.mjs");

class FakeSocket extends EventEmitter {
  sent: string[] = [];
  closed: { code?: number; reason?: string } | null = null;
  bufferedAmount = 0;

  send(data: string | Buffer) {
    this.sent.push(String(data));
  }

  close(code?: number, reason?: string) {
    this.closed = { code, reason };
    this.emit("close");
  }
}

function textStream(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}

function openStream(): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start() {
      // Keep the stream open until the test closes the socket.
    },
  });
}

async function loadWs(): Promise<WsModule> {
  return import("./ws.mjs") as Promise<WsModule>;
}

describe("AG-UI WebSocket verification gates", () => {
  it("preserves the Message Post compound reconnect cursor and emits raw AG-UI JSON", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const observedUrls: string[] = [];

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?thread=design&after=1783300001000&afterRowid=44"),
      {
        observe: async (request: Request) => {
          observedUrls.push(request.url);
          return new Response(
            textStream([
              ": comment ignored\n\n",
              "data: {\"type\":\"RUN_STARTED\",\"threadId\":\"post:design\",\"runId\":\"r1\"}\n\n",
              "data: {\"type\":\"TEXT_MESSAGE_START\",\"messageId\":\"m1\",\"cursor\":{\"createdAt\":1783300001000,\"rowid\":45}}\n\n",
            ]),
            { status: 200, headers: { "content-type": "text/event-stream" } },
          );
        },
      },
    );
    await control.closed;

    expect(observedUrls).toEqual([
      "http://localhost/api/agui/observe?thread=design&after=1783300001000&afterRowid=44",
    ]);
    expect(socket.sent).toEqual([
      "{\"type\":\"RUN_STARTED\",\"threadId\":\"post:design\",\"runId\":\"r1\"}",
      "{\"type\":\"TEXT_MESSAGE_START\",\"messageId\":\"m1\",\"cursor\":{\"createdAt\":1783300001000,\"rowid\":45}}",
    ]);
  });

  it("preserves the Agent Session afterId reconnect cursor", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const observedUrls: string[] = [];

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=iris&afterId=2141088"),
      {
        observe: async (request: Request) => {
          observedUrls.push(request.url);
          return new Response(
            textStream([
              "data: {\"type\":\"RUN_STARTED\",\"threadId\":\"session:iris\",\"runId\":\"r-materialized\"}\n\n",
              "data: {\"type\":\"TEXT_MESSAGE_CONTENT\",\"messageId\":\"m1\",\"delta\":\"replayed\",\"streamEventId\":2141089}\n\n",
            ]),
            { status: 200, headers: { "content-type": "text/event-stream" } },
          );
        },
      },
    );
    await control.closed;

    expect(observedUrls).toEqual([
      "http://localhost/api/agui/observe?session=iris&afterId=2141088",
    ]);
    expect(socket.sent).toEqual([
      "{\"type\":\"RUN_STARTED\",\"threadId\":\"session:iris\",\"runId\":\"r-materialized\"}",
      "{\"type\":\"TEXT_MESSAGE_CONTENT\",\"messageId\":\"m1\",\"delta\":\"replayed\",\"streamEventId\":2141089}",
    ]);
  });

  it("does not acknowledge socket input until existing ingress resolves", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    let resolveInput!: (response: Response) => void;
    const inputResult = new Promise<Response>((resolve) => {
      resolveInput = resolve;
    });
    const sessionInput = vi.fn(() => inputResult);

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=iris"),
      {
        observe: async () => new Response(openStream(), { status: 200 }),
        sessionInput,
      },
    );

    socket.emit(
      "message",
      JSON.stringify({
        t: "input",
        mode: "session",
        target: { name: "iris", agentId: "a_iris" },
        text: "continue",
        clientMessageId: "cm_socket",
      }),
    );

    await Promise.resolve();
    expect(sessionInput).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "session",
        target: { name: "iris", agentId: "a_iris" },
        text: "continue",
        clientMessageId: "cm_socket",
      }),
      expect.any(Request),
    );
    expect(socket.sent).not.toContain(JSON.stringify({
      t: "input.ack",
      commandId: "cmd_socket",
    }));

    resolveInput(new Response(JSON.stringify({
      ok: true,
      receipt: {
        commandId: "cmd_socket",
        clientMessageId: "cm_socket",
        sessionId: "s_iris",
        state: "queued",
        revision: 1,
        seq: 11,
      },
    }), {
      status: 201,
      headers: { "content-type": "application/json" },
    }));

    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "input.ack",
        commandId: "cmd_socket",
        clientMessageId: "cm_socket",
        sessionId: "s_iris",
        state: "queued",
        revision: 1,
        seq: 11,
      }));
    });
    control.close();
    await control.closed;
  });

  it("routes bus input through the Message Post ingress with the client idempotency key", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const routed: Array<{ url: string; body: unknown; idempotencyKey: string | null }> = [];

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?thread=nexus-project"),
      {
        observe: async () => new Response(openStream(), { status: 200 }),
        fetchHandler: async (request: Request) => {
          routed.push({
            url: request.url,
            body: await request.json(),
            idempotencyKey: request.headers.get("idempotency-key"),
          });
          return new Response(JSON.stringify({ ok: true, result: { messageId: "m1" } }), {
            status: 201,
            headers: { "content-type": "application/json" },
          });
        },
      },
    );

    socket.emit(
      "message",
      JSON.stringify({
        t: "input",
        mode: "bus",
        target: { verb: "post", thread: "nexus-project" },
        text: "status",
        clientMessageId: "cm_bus",
      }),
    );

    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "input.ack",
        clientMessageId: "cm_bus",
        delivered: true,
      }));
    });
    expect(routed).toEqual([
      {
        url: "http://localhost/api/v1/messages",
        body: {
          to: { verb: "post", thread: "nexus-project" },
          body: "status",
          idempotencyKey: "cm_bus",
        },
        idempotencyKey: "cm_bus",
      },
    ]);
    control.close();
    await control.closed;
  });
});
