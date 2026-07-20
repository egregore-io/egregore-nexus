export const TRANSPORT_PROTOCOL_VERSION = 1 as const;
export const TRANSPORT_MAX_FRAME_BYTES = 64 * 1024;
export const TRANSPORT_MAX_UNSETTLED = 256;

export type TransportLaneKind = "thread" | "dm";

export interface TransportLane {
  kind: TransportLaneKind;
  name: string;
}

export type HostTransportFrame =
  | {
      t: "transport/init";
      protocolVersions: [1];
      config: Readonly<Record<string, unknown>>;
      generation: number;
    }
  | {
      t: "transport/deliver";
      obligationId: string;
      externalChatId: string;
      lane: TransportLane;
      text: string;
    }
  | { t: "transport/ping" }
  | { t: "transport/shutdown" };

export type BridgeTransportFrame =
  | { t: "transport/hello"; protocolVersion: number }
  | {
      t: "transport/ingress";
      ingressId: string;
      external: { userId: string; displayName?: string };
      chatId: string;
      text: string;
    }
  | {
      t: "transport/receipt";
      obligationId: string;
      externalMessageId: string;
    }
  | {
      t: "transport/bind";
      external: { userId: string; displayName?: string };
    }
  | {
      t: "transport/bindLane";
      external: { chatId: string };
      lane: TransportLane;
    }
  | { t: "transport/log"; level: string; message: string };
