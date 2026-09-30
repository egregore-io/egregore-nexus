/* ============================================================================
   nexus-mark.js — the Nexus mark, randomized on every render.
   Zero dependencies. ~2KB. Framework-free.

   USAGE
   -----
   1) Drop a placeholder wherever you want the mark, set its color via CSS:

        <span class="nexus-mark" style="color:#fff" data-size="40"></span>
        <script src="nexus-mark.js"></script>

      Every .nexus-mark on the page is filled with a freshly randomized mark
      on load — so each visit looks a little different.

   2) Or call the API yourself:

        NexusMark.render(el, { density: 0.45, size: 48 });
        const svg = NexusMark.svg({ density: 0.6 });   // returns an SVG string

   OPTIONS (all optional)
   ----------------------
     size        px (number) — sets width & height. Omit to fill via CSS.
     density     0..1 — chance each of the 12 corners lights up. Default 0.45.
     dotSize     node radius in the 64-unit viewBox. Default 2.
     prism       show the faint inner prism. Default true.
     strokeWidth default 1.5.
     seed        pass a number/string for a REPRODUCIBLE mark (same every load).
                 Omit for true randomness on each render.
     rerollOnClick  true → clicking the mark re-randomizes it. Default false.

   Data-attribute equivalents on the placeholder: data-size, data-density,
   data-dot-size, data-prism="false", data-seed, data-reroll-on-click.
   ========================================================================== */
(function (global) {
  "use strict";

  // The 12 candidate corners: 6 on the outer hexagon, 6 on the inner prism.
  var VERTS = [
    [32, 8], [53, 20], [53, 44], [32, 56], [11, 44], [11, 20],          // outer
    [32, 17.6], [44.6, 24.8], [44.6, 39.2], [32, 46.4], [19.4, 39.2], [19.4, 24.8] // inner
  ];

  // Static geometry (viewBox 0 0 64 64).
  var HEX = "M32 8 L53 20 L53 44 L32 56 L11 44 L11 20 Z";
  var PRISM = "M32 17.6 L44.6 24.8 L44.6 39.2 L32 46.4 L19.4 39.2 L19.4 24.8 Z";
  var EDGES = "M32 32 L32 8 M32 32 L11 44 M32 32 L53 44";
  var WELD = "M34.8 32 L32 24.6 L29.2 32 Z M30.6 29.6 L25.6 35.6 L33.4 34.4 Z M30.6 34.4 L38.4 35.6 L33.4 29.6 Z";

  // Seeded RNG (mulberry32) so an optional seed gives a stable mark.
  function makeRng(seed) {
    if (seed == null) return Math.random;
    var h = 1779033703 ^ String(seed).length;
    for (var i = 0; i < String(seed).length; i++) {
      h = Math.imul(h ^ String(seed).charCodeAt(i), 3432918353);
      h = (h << 13) | (h >>> 19);
    }
    var a = h >>> 0;
    return function () {
      a |= 0; a = (a + 0x6D2B79F5) | 0;
      var t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  // Pick which corners light up.
  function pickNodes(density, rng) {
    var on = [];
    for (var i = 0; i < VERTS.length; i++) if (rng() < density) on.push(VERTS[i]);
    if (!on.length) on.push(VERTS[Math.floor(rng() * VERTS.length)]); // never fully empty
    return on;
  }

  function svg(opts) {
    opts = opts || {};
    var density = opts.density == null ? 0.45 : +opts.density;
    var dotSize = opts.dotSize == null ? 2 : +opts.dotSize;
    var sw = opts.strokeWidth == null ? 1.5 : +opts.strokeWidth;
    var showPrism = opts.prism !== false && opts.prism !== "false";
    var rng = makeRng(opts.seed);
    var sizeAttr = opts.size ? ' width="' + opts.size + '" height="' + opts.size + '"' : ' width="100%" height="100%"';

    var dots = pickNodes(density, rng).map(function (p) {
      return '<circle cx="' + p[0] + '" cy="' + p[1] + '" r="' + dotSize + '" fill="currentColor"/>';
    }).join("");

    return '<svg xmlns="http://www.w3.org/2000/svg"' + sizeAttr +
      ' viewBox="0 0 64 64" fill="none" stroke="currentColor" stroke-width="' + sw +
      '" stroke-linejoin="round" stroke-linecap="round">' +
      '<path d="' + HEX + '"/>' +
      (showPrism ? '<path d="' + PRISM + '" stroke-opacity="0.42"/>' : "") +
      '<path d="' + EDGES + '" stroke-opacity="0.5"/>' +
      '<path d="' + WELD + '" fill="currentColor" stroke="none"/>' +
      dots + "</svg>";
  }

  /* ---- Loading affordance -------------------------------------------------
     Two modes:
       'travel' (default) — dots glide straight across the mark between corners
                            through the interior, never along the drawn lines.
       'hop'              — dots pop in and out in place, a shifting constellation.
     Either way each dot fades in as it appears and out as it goes. */
  var NS = "http://www.w3.org/2000/svg";
  function svgEl(name, attrs) {
    var e = document.createElementNS(NS, name);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    return e;
  }

  function loader(el, opts) {
    opts = opts || {};
    var sw = opts.strokeWidth == null ? 1.5 : +opts.strokeWidth;
    var dotSize = opts.dotSize == null ? 2.2 : +opts.dotSize;
    // count: a number for a fixed pool, or [min,max] / "3-7" to randomize the pool size each run
    var count;
    (function () {
      var c = opts.count == null ? 5 : opts.count, lo, hi;
      if (Array.isArray(c)) { lo = +c[0]; hi = +c[1]; }
      else if (typeof c === "string" && c.indexOf("-") > 0) { var p = c.split("-"); lo = +p[0]; hi = +p[1]; }
      else { count = +c; return; }
      lo = Math.max(1, lo | 0); hi = Math.max(lo, hi | 0);
      count = lo + ((Math.random() * (hi - lo + 1)) | 0);
    })();
    var speed = opts.speed == null ? 1 : +opts.speed;   // higher = faster
    var mode = opts.mode === "hop" ? "hop" : "travel";
    var showPrism = opts.prism !== false && opts.prism !== "false";

    if (el.__nexusStop) el.__nexusStop();
    el.innerHTML = "";

    var root = svgEl("svg", { viewBox: "0 0 64 64", fill: "none", stroke: "currentColor",
      "stroke-width": sw, "stroke-linejoin": "round", "stroke-linecap": "round" });
    if (opts.size) { root.setAttribute("width", opts.size); root.setAttribute("height", opts.size); }
    else { root.setAttribute("width", "100%"); root.setAttribute("height", "100%"); }

    root.appendChild(svgEl("path", { d: HEX }));
    if (showPrism) root.appendChild(svgEl("path", { d: PRISM, "stroke-opacity": 0.42 }));
    root.appendChild(svgEl("path", { d: EDGES, "stroke-opacity": 0.5 }));
    root.appendChild(svgEl("path", { d: WELD, fill: "currentColor", stroke: "none" }));
    var layer = svgEl("g", {});
    root.appendChild(layer);
    el.appendChild(root);

    function ease(t) { return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2; }
    // travel: visible most of the trip; hop: fades in/out with a hold in place
    function envelope(t) { var edge = mode === "hop" ? 0.3 : 0.16; if (t < edge) return t / edge; if (t > 1 - edge) return (1 - t) / edge; return 1; }
    // never route along a drawn line: skip same-ring self / neighbour / opposite; cross-ring is always a clean chord
    function disallow(a, b) {
      if ((a < 6) === (b < 6)) { var base = a < 6 ? 0 : 6, i = a - base, j = b - base, d = Math.min((i - j + 6) % 6, (j - i + 6) % 6); return d === 0 || d === 1 || d === 3; }
      return false;
    }
    // Time-scheduled occupancy. A collision is any of:
    //   - two dots STARTING from the same vertex at overlapping times,
    //   - two dots on the SAME segment at overlapping times,
    //   - a dot ARRIVING at a vertex another dot still holds.
    // A vertex also stays "live" for a cooldown AFTER a dot leaves it (~0.5s at
    // speed 1, scaled with speed), so a hand-off needs real breathing room.
    var HEAD = 0.18, TAIL = 0.82;               // trip fractions spent on source / arriving at dest
    function cool() { return 500 / speed; }      // trailing "still live" window (ms); 0.5s at speed 1
    function edgeKey(a, b) { return a < b ? a + "-" + b : b + "-" + a; }
    function ovl(a, b) { return a[0] < b[1] && b[0] < a[1]; }
    function occOf(s) {                          // vertices/segment a trip holds, and when (incl. cooldown)
      var c = cool();
      if (mode === "hop") { var w = [s.tStart, s.tStart + s.dur + c]; return { fi: s.fi, ti: s.fi, src: w, dst: w, edge: null, seg: null }; }
      return { fi: s.fi, ti: s.ti,
        src: [s.tStart, s.tStart + HEAD * s.dur + c],
        dst: [s.tStart + TAIL * s.dur, s.tStart + s.dur + c],
        seg: [s.tStart, s.tStart + s.dur + c], edge: edgeKey(s.fi, s.ti) };
    }
    function vtxBusy(o, v, iv) { return (v === o.fi && ovl(iv, o.src)) || (v === o.ti && ovl(iv, o.dst)); }
    function collides(cand, idx) {
      for (var k = 0; k < slots.length; k++) {
        if (k === idx) continue; var s = slots[k]; if (s.tStart == null) continue;
        var o = occOf(s);
        if (cand.edge && o.edge === cand.edge && ovl(cand.seg, o.seg)) return true; // same segment, overlapping time
        if (vtxBusy(o, cand.fi, cand.src)) return true;                             // our start vertex still held
        if (vtxBusy(o, cand.ti, cand.dst)) return true;                             // arriving where one still sits
      }
      return false;
    }
    function dur() { return (mode === "hop" ? (850 + Math.random() * 900) : (1000 + Math.random() * 700)) / speed; }
    function gap() { return Math.random() < 0.5 ? (200 + Math.random() * 950) / speed : 0; }

    // schedule a slot's next trip so it collides with nobody; nudge later / retry if boxed in
    function schedule(s, idx, now) {
      var base = now + (s.gap || 0), lf = 0, lt = 0, ld = dur(), ls = base;
      for (var a = 0; a < 30; a++) {
        var fi = (Math.random() * VERTS.length) | 0, ti = fi, tries = 0;
        if (mode !== "hop") { do { ti = (Math.random() * VERTS.length) | 0; } while ((ti === fi || disallow(fi, ti)) && ++tries < 40); }
        var d = dur(), tStart = base + (a > 5 ? (a - 5) * 130 / speed : 0);
        var cand = occOf({ fi: fi, ti: ti, tStart: tStart, dur: d });
        lf = fi; lt = ti; ld = d; ls = tStart;
        if (!collides(cand, idx)) { s.fi = fi; s.ti = ti; s.dur = d; s.tStart = tStart; return; }
      }
      s.fi = lf; s.ti = lt; s.dur = ld; s.tStart = ls + cool(); // fallback: last try, pushed a cooldown later
    }

    var slots = [];
    for (var i = 0; i < count; i++) {
      var c = svgEl("circle", { r: dotSize, fill: "currentColor", stroke: "none", opacity: 0 });
      layer.appendChild(c);
      slots.push({ c: c, fi: 0, ti: 0, dur: dur(), tStart: null, gap: (Math.random() * 1300) / speed });
    }

    var raf;
    function frame(now) {
      if (!document.contains(el)) { el.__nexusStop = null; return; } // auto-cleanup
      for (var i = 0; i < slots.length; i++) {
        var s = slots[i];
        if (s.tStart == null) schedule(s, i, now);
        var t = (now - s.tStart) / s.dur;
        if (t < 0) { s.c.setAttribute("opacity", "0"); continue; }  // waiting out its gap
        if (t >= 1) { s.gap = gap(); schedule(s, i, now); t = (now - s.tStart) / s.dur; if (t < 0) { s.c.setAttribute("opacity", "0"); continue; } }
        if (mode === "travel") {
          var A = VERTS[s.fi], B = VERTS[s.ti], e = ease(t);
          s.c.setAttribute("cx", (A[0] + (B[0] - A[0]) * e).toFixed(2));
          s.c.setAttribute("cy", (A[1] + (B[1] - A[1]) * e).toFixed(2));
        } else { s.c.setAttribute("cx", VERTS[s.fi][0]); s.c.setAttribute("cy", VERTS[s.fi][1]); }
        s.c.setAttribute("opacity", envelope(t).toFixed(3));
      }
      raf = requestAnimationFrame(frame);
    }
    raf = requestAnimationFrame(frame);
    var stop = function () { if (raf) cancelAnimationFrame(raf); raf = null; el.__nexusStop = null; };
    el.__nexusStop = stop;
    return { stop: stop, el: el };
  }

  function readOpts(el) {
    var d = el.dataset || {};
    var o = {};
    if (d.size) o.size = +d.size;
    if (d.density) o.density = +d.density;
    if (d.dotSize) o.dotSize = +d.dotSize;
    if (d.prism) o.prism = d.prism;
    if (d.strokeWidth) o.strokeWidth = +d.strokeWidth;
    if (d.seed) o.seed = d.seed;
    if (d.rerollOnClick != null) o.rerollOnClick = d.rerollOnClick !== "false";
    if (d.loading != null) o.loading = d.loading !== "false";
    if (d.count) o.count = d.count.indexOf("-") > 0 ? d.count : +d.count;
    if (d.countMin || d.countMax) o.count = [d.countMin || 1, d.countMax || d.countMin || 6];
    if (d.speed) o.speed = +d.speed;
    if (d.mode) o.mode = d.mode;
    return o;
  }

  function render(el, opts) {
    if (!el) return;
    opts = opts || readOpts(el);
    if (opts.loading) { loader(el, opts); return; }
    if (el.__nexusStop) el.__nexusStop();
    el.innerHTML = svg(opts);
    if (opts.rerollOnClick) {
      el.style.cursor = "pointer";
      el.onclick = function () { el.innerHTML = svg(Object.assign({}, opts, { seed: undefined })); };
    }
  }

  function mountAll(root) {
    (root || document).querySelectorAll(".nexus-mark").forEach(function (el) { render(el); });
  }

  var NexusMark = { svg: svg, render: render, loader: loader, mountAll: mountAll, VERTS: VERTS };
  if (typeof module !== "undefined" && module.exports) module.exports = NexusMark;
  global.NexusMark = NexusMark;

  if (typeof document !== "undefined") {
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", function () { mountAll(); });
    else mountAll();
  }
})(typeof window !== "undefined" ? window : this);
