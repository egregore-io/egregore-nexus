// TDD for the /login page.
import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";

// We test the LoginPage component in isolation (no full router needed).
import { LoginPage } from "./login";

// ── helpers ──────────────────────────────────────────────────────────────────

type FetchMock = ReturnType<typeof vi.fn>;

function mockFetch(status: number, body: unknown): FetchMock {
  return vi.fn(async () =>
    new Response(JSON.stringify(body), { status }),
  );
}

// ── tests ─────────────────────────────────────────────────────────────────────

describe("LoginPage component", () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it("renders a name input and a submit button", () => {
    render(<LoginPage onSuccess={() => void 0} />);
    expect(screen.getByRole("textbox", { name: /name/i })).toBeInTheDocument();
    expect(screen.getByLabelText(/password/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /enter/i })).toBeInTheDocument();
  });

  it("submits POST /api/login with the entered name and calls onSuccess", async () => {
    const fetchMock = mockFetch(200, { ok: true });
    vi.stubGlobal("fetch", fetchMock);

    const onSuccess = vi.fn();
    render(<LoginPage onSuccess={onSuccess} />);

    fireEvent.change(screen.getByRole("textbox", { name: /name/i }), {
      target: { value: "alice" },
    });
    fireEvent.change(screen.getByLabelText(/password/i), {
      target: { value: "pw" },
    });
    fireEvent.click(screen.getByRole("button", { name: /enter/i }));

    await waitFor(() => expect(onSuccess).toHaveBeenCalledOnce());

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/login",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ name: "alice", password: "pw" }),
      }),
    );
  });

  it("shows an error message when the server returns non-ok", async () => {
    vi.stubGlobal("fetch", mockFetch(400, { error: "name is required" }));

    render(<LoginPage onSuccess={() => void 0} />);

    // Use a valid-looking name so it passes client-side guard and hits the mocked server.
    fireEvent.change(screen.getByRole("textbox", { name: /name/i }), {
      target: { value: "bad-name" },
    });
    fireEvent.change(screen.getByLabelText(/password/i), {
      target: { value: "pw" },
    });
    fireEvent.click(screen.getByRole("button", { name: /enter/i }));

    await waitFor(() =>
      expect(screen.getByRole("alert")).toBeInTheDocument(),
    );
  });

  it("does not submit when name field is empty", () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    render(<LoginPage onSuccess={() => void 0} />);
    fireEvent.click(screen.getByRole("button", { name: /enter/i }));

    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("does not submit when password field is empty", () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    render(<LoginPage onSuccess={() => void 0} />);
    fireEvent.change(screen.getByRole("textbox", { name: /name/i }), {
      target: { value: "alice" },
    });
    fireEvent.click(screen.getByRole("button", { name: /enter/i }));

    expect(fetchMock).not.toHaveBeenCalled();
  });
});
