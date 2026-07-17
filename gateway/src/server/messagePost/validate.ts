import { GatewayError } from "@server/api/http";
import type { SendRequest } from "@shared/types";

export const EMPTY_MESSAGE_BODY = "message body must not be empty";

export function hasMessageBody(body: string): boolean {
  return body.trim().length > 0;
}

export function assertMessageBody(body: string): void {
  if (!hasMessageBody(body)) {
    throw new GatewayError(400, EMPTY_MESSAGE_BODY);
  }
}

export function assertSendRequestBody(req: SendRequest): void {
  assertMessageBody(req.body);
}
