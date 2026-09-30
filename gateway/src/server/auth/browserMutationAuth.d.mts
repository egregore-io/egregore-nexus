export function browserMutationCsrfFailure(
  request: Request,
  options?: { enforce?: boolean },
): Response | undefined;
export function bindWebSocketCsrf(request: Request): Request;
export function csrfWebSocketProtocol(token: string): string;
