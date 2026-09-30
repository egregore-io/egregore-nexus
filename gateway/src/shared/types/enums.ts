// Contract enums — re-exported from the generated mirror so the rest of the app
// imports them from @shared/types, never from contracts.gen directly. These are
// runtime enums (string-valued), hence a value re-export (not `export type`).
//
// NEVER redefine or duplicate a contract enum here — re-export only.
export {
  Kind,
  Scope,
  Tier,
  Presence,
  DeliveryState,
  SearchMode,
  ChannelOp,
  StatusState,
} from "./contracts.gen";
