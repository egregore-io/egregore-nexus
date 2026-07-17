import { createFileRoute } from "@tanstack/react-router";

import { IndexView } from "./-IndexView";

export const Route = createFileRoute("/")({ component: IndexView });
