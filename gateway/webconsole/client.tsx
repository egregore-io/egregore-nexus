import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";

import { createWebconsoleRouter } from "./router";

const root = document.getElementById("root");
if (!root) throw new Error("Nexus WebUI root element is missing");

const router = createWebconsoleRouter();
createRoot(root).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
);
