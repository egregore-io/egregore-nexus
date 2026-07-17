// Barrel for the contract types layer. The whole app imports types from
// `@shared/types` — never from `contracts.gen` directly. The generated mirror is
// re-exported first; the hand-authored modules (entities/events/dto/enums) then
// re-export the subset they own + add view-models/helpers, and WIN on any shared
// name (e.g. the `MessageVM` view-model, the `isEvent` narrowing helper).
//
// The Drizzle read models are a SEPARATE introspected layer in @drizzle — they
// describe DB rows for display and are NEVER re-exported here.
export * from "./contracts.gen";
export * from "./enums";
export * from "./entities";
export * from "./events";
export * from "./dto";
