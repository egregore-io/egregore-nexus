#!/usr/bin/env node
// Compatibility entrypoint. The real logic lives in gateway-serve-impl.mjs.
//
// Keep this file parseable by pre-Node-20 runtimes so users receive the explicit
// version error below instead of a syntax error from the modern implementation.
var major = parseInt(process.versions.node.split(".")[0], 10);
if (major < 20) {
  console.error(
    "nexus gateway: node " +
      process.versions.node +
      " is too old (need >= 20). This is usually a stale system node shadowing your " +
      "real one — check `which node`, or point NEXUS_GATEWAY_NODE at a modern binary."
  );
  process.exit(1);
}
import("./gateway-serve-impl.mjs").catch(function (error) {
  console.error(error);
  process.exit(1);
});
