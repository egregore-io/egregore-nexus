import { describe, expect, it } from "vitest";

import { Kind, Locality } from "@shared/types";
import {
  dottedEntityKind,
  EntityKindError,
  parseEntityKind,
} from "@server/identity/entityKind";

describe("Gateway entity-kind parsing", () => {
  it("preserves dotted locality and treats bare legacy kinds as local", () => {
    expect(parseEntityKind("external.human")).toEqual({
      locality: Locality.External,
      kind: Kind.Human,
    });
    expect(parseEntityKind("human")).toEqual({
      locality: Locality.Local,
      kind: Kind.Human,
    });
    expect(parseEntityKind("human", "trusted")).toEqual({
      locality: Locality.Trusted,
      kind: Kind.Human,
    });
    expect(dottedEntityKind(Locality.External, Kind.Human)).toBe("external.human");
  });

  it.each(["remote.human", "external.bot", "", "local.human.extra"])(
    "refuses unknown entity kind %j",
    (value) => expect(() => parseEntityKind(value)).toThrow(EntityKindError),
  );

  it("refuses contradictory dotted and parallel locality facets", () => {
    expect(() => parseEntityKind("external.human", "local")).toThrow(EntityKindError);
  });
});
