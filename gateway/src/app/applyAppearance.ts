// useApplyAppearance — single DOM-apply path for accent + backdrop dim.
//
// Reads the persisted appearance values from the uiPrefs store and writes them
// to `document.documentElement` as a side-effect. Call once from AppShell (or
// any root component) so that DOM mutations are centralised and tested in one
// place; handlers in SettingsModal call ONLY store setters, and this hook
// reacts to the store change automatically.
import { useEffect } from "react";

import { useAppearance } from "@app/uiPrefs";

export function useApplyAppearance() {
  const { accent, backdropDim } = useAppearance();
  useEffect(() => {
    document.documentElement.dataset.accent = accent;
    document.documentElement.style.setProperty(
      "--lens-backdrop-dim",
      String(backdropDim),
    );
  }, [accent, backdropDim]);
}
