<script setup lang="ts">
// kayak's landing page. The markup and the styles are the page; the script is
// the pinned tour (which tab the card shows is a function of scroll position)
// and the two things that make the card look alive — a chart that rolls once
// a second and a log that ticks — both of which the product actually does.
import { ref, onMounted, onBeforeUnmount } from 'vue'

const rootRef = ref<HTMLElement | null>(null)
const timers: ReturnType<typeof setInterval>[] = []
const cleanup: (() => void)[] = []

onMounted(() => {
  const root = rootRef.value
  if (!root) return
  var reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  /* ------------------------------------------------ the charts on the cards
     Thirty slots, two thin bars each, drawn as two paths in a 100×100 box at
     preserveAspectRatio none — the way the product draws them. */
  var SLOTS = 30, SLOT = 100 / SLOTS, BAR = SLOT * 0.36;
  function series(shape) {
    var inS = [], outS = [];
    for (var i = 0; i < SLOTS; i++) {
      var base = 0.55 + 0.35 * Math.sin(i / 3.1) * Math.sin(i / 7.3) + ((i * 37) % 11) / 60;
      var v = Math.min(1, Math.max(0.15, base));
      var o;
      if (shape === 'rollup') o = 0.04;
      else if (shape === 'same') o = v;
      else if (shape === 'sparse') o = v * (0.2 + 0.25 * Math.abs(Math.sin(i / 2.2)));
      else o = v * 0.92;
      inS.push(v); outS.push(o);
    }
    return { in: inS, out: outS };
  }
  function bars(vals, offset) {
    var d = '';
    for (var i = 0; i < vals.length; i++) {
      var h = vals[i] * 100, x = i * SLOT + offset;
      d += 'M' + x.toFixed(2) + ' ' + (100 - h).toFixed(2) + 'h' + BAR.toFixed(2) + 'v' + h.toFixed(2) + 'h-' + BAR.toFixed(2) + 'z';
    }
    return d;
  }
  var charts = [];
  root.querySelectorAll('.chart-plot .chart-svg').forEach(function (svg) {
    var pIn = svg.querySelector('[data-series=in]'), pOut = svg.querySelector('[data-series=out]');
    var s = series(pIn.getAttribute('data-shape') || 'main');
    charts.push({ pIn: pIn, pOut: pOut, s: s });
    draw({ pIn: pIn, pOut: pOut, s: s });
  });
  function draw(c) {
    c.pIn.setAttribute('d', bars(c.s.in, SLOT * 0.1));
    c.pOut.setAttribute('d', bars(c.s.out, SLOT * 0.1 + BAR + SLOT * 0.08));
  }
  // the error strip on the one card that has something to say
  var err = root.querySelector('[data-series=err]');
  if (err) {
    var e = [];
    for (var i = 0; i < SLOTS; i++) e.push([4, 5, 11, 12, 13, 22, 28].indexOf(i) >= 0 ? 1 : 0);
    err.setAttribute('d', bars(e, SLOT * 0.1).replace(/h[\d.]+v/g, function (m) { return 'h' + (SLOT * 0.8).toFixed(2) + 'v'; }));
  }
  // charts roll once a second, like the product's
  if (!reduced) timers.push(setInterval(function () {
    charts.forEach(function (c) {
      c.s.in.push(c.s.in.shift()); c.s.out.push(c.s.out.shift()); draw(c);
    });
  }, 1000));

  /* -------------------------------------------------------- the live log */
  var log = root.querySelector('#log'), rate = root.querySelector('#rate');
  var sensors = ['line1/temp', 'line1/flow', 'line2/temp', 'line2/pressure', 'boiler/temp'];
  var n = 0, t0 = Date.now();
  function stamp(ms) {
    var d = new Date(ms), p = function (x, w) { return String(x).padStart(w || 2, '0'); };
    return p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds()) + '.' + p(d.getMilliseconds(), 3);
  }
  function line(ms, stage) {
    var s = sensors[n % sensors.length], v = (18 + 6 * Math.sin(n / 5) + (n % 7) / 10).toFixed(1);
    var msg = stage === 'IN'
      ? '{"_meta":{"subject":"sensors.' + s.split('/')[0] + '","connection":"plant-nats"},"sensor":"' + s + '","value":' + v + ',"ts":"…'
      : '{"sensor":"' + s + '","value":' + v + ',"recorded_at":"2026-08-28T' + stamp(ms).slice(0, 8) + 'Z"}';
    var row = document.createElement('div');
    row.className = 'log-row';
    row.innerHTML = '<span class="t">' + stamp(ms) + '</span><span class="s">' + stage + '</span><span class="m"></span>';
    row.lastChild.textContent = msg;
    return row;
  }
  function push() {
    var ms = Date.now();
    log.appendChild(line(ms, 'IN'));
    log.appendChild(line(ms + 3, 'OUT'));
    n++;
    while (log.children.length > 5) log.removeChild(log.firstChild);
  }
  for (var k = 0; k < 3; k++) push();
  if (!reduced) timers.push(setInterval(push, 900));
  if (!reduced) timers.push(setInterval(function () { rate.textContent = (86 + Math.round(12 * Math.sin(Date.now() / 4000))) + '/s'; }, 1000));

  /* ------------------------------------------------- the scroll-driven tour
     The track is 5.6 viewports tall and the stage is stuck for all of it.
     The step is which fifth of the track is under the viewport; the graph's
     transform is the one thing that eases with scroll rather than stepping. */
  var tour = root.querySelector('.tour'), track = root.querySelector('.tour-track'), stage = root.querySelector('.tour-stage');
  // the stage sticks under vitepress' nav where that nav is fixed, and at the top where it isn't
  var navTop = function () { return parseFloat(getComputedStyle(stage).top) || 0; };
  var canvas = root.querySelector('.tour-canvas'), graph = root.querySelector('#graph');
  var texts = root.querySelectorAll('.tour-text'), steps = root.querySelectorAll('.steps li:not(.head)');
  var tabs = root.querySelectorAll('#card .tabs .tab'), panes = root.querySelectorAll('#card .pane-body');
  var STEPS = 5, current = -1;
  var CARD_W = 360, GRAPH_W = 1140, CARD_H = 470, GRAPH_H = 1100, GAP = 250;
  var card = root.querySelector('#card'), children = root.querySelectorAll('.card.child');
  var edgePaths = graph.querySelectorAll('.edges path');
  function layoutGraph() {
    CARD_H = card.offsetHeight;
    var y0 = CARD_H, y1 = CARD_H + GAP, mid = y0 + Math.round(GAP * 0.45);
    children.forEach(function (c) { c.style.top = y1 + 'px'; });
    GRAPH_H = y1 + Math.max.apply(null, Array.prototype.map.call(children, function (c) { return c.offsetHeight; }));
    var ds = [
      'M552 ' + y0 + ' V' + (mid - 6) + ' Q552 ' + mid + ' 546 ' + mid + ' H186 Q180 ' + mid + ' 180 ' + (mid + 6) + ' V' + y1,
      'M570 ' + y0 + ' V' + y1,
      'M588 ' + y0 + ' V' + (mid + 14) + ' Q588 ' + (mid + 20) + ' 594 ' + (mid + 20) + ' H954 Q960 ' + (mid + 20) + ' 960 ' + (mid + 26) + ' V' + y1
    ];
    edgePaths.forEach(function (p, i) { p.setAttribute('d', ds[i % 3]); });
  }

  function place(step) {
    layoutGraph();
    var cw = canvas.clientWidth, ch = canvas.clientHeight;
    var s, tx, ty;
    if (step < 4) {
      // the parent card alone, centred, at whatever scale fits the canvas
      s = Math.min(1, (cw - 32) / CARD_W, (ch - 32) / CARD_H);
      tx = -cw / 2 - (390 + CARD_W / 2) * s + cw / 2;
      ty = -ch / 2 - (CARD_H / 2) * s + ch / 2;
    } else {
      // zoomed out: the whole graph
      s = Math.min((cw - 32) / GRAPH_W, (ch - 32) / GRAPH_H);
      tx = -cw / 2 - (GRAPH_W / 2) * s + cw / 2;
      ty = -ch / 2 - (GRAPH_H / 2) * s + ch / 2;
    }
    graph.style.transform = 'translate(' + tx.toFixed(1) + 'px,' + ty.toFixed(1) + 'px) scale(' + s.toFixed(4) + ')';
  }
  function setStep(step) {
    if (step === current) return;
    current = step;
    tour.className = 'tour step-' + step;
    texts.forEach(function (t, i) { t.classList.toggle('active', i === step); });
    steps.forEach(function (li, i) { li.classList.toggle('active', i === step); });
    var tab = step === 0 ? 0 : Math.min(step - 1, 2);
    if (step === 4) tab = 0;
    tabs.forEach(function (t, i) { t.classList.toggle('active', i === tab); });
    panes.forEach(function (p, i) { p.classList.toggle('active', i === tab); });
    place(step);
  }
  function onScroll() {
    var r = track.getBoundingClientRect();
    var vh = stage.offsetHeight;
    var travelled = navTop() - r.top;
    var per = (r.height - vh) / STEPS;
    var step = Math.max(0, Math.min(STEPS - 1, Math.floor(travelled / per + 0.15)));
    setStep(step);
  }
  var onResize = function () { place(current < 0 ? 0 : current); };
  window.addEventListener('scroll', onScroll, { passive: true });
  window.addEventListener('resize', onResize);
  cleanup.push(function () { window.removeEventListener('scroll', onScroll); window.removeEventListener('resize', onResize); });
  // the step list scrolls the page to that step's fifth of the track
  root.querySelectorAll('.steps a[data-step]').forEach(function (a) {
    a.addEventListener('click', function (ev) {
      ev.preventDefault();
      var i = +a.getAttribute('data-step');
      var top = track.getBoundingClientRect().top + window.scrollY;
      var per = (track.offsetHeight - stage.offsetHeight) / STEPS;
      window.scrollTo({ top: top - navTop() + per * i + 2, behavior: reduced ? 'auto' : 'smooth' });
    });
  });
  onScroll();
  if (current < 0) setStep(0);
})

onBeforeUnmount(() => {
  timers.forEach((t) => clearInterval(t))
  cleanup.forEach((f) => f())
})
</script>

<template>
<div class="landing" ref="rootRef">
<!-- ================================================================ hero -->
<header class="hero grid-bg" id="top">
  <div class="hero-inner">
    <div>
      <h1>stream processing <span class="thin">you can watch</span></h1>
      <p class="lede">kayak runs <code>inputs → transforms → outputs</code> pipelines from a config file and draws the whole graph while it's running — a card per pipeline, an edge for every hand-off, and on each card the config, a throughput chart and the actual messages going through.</p>
      <pre class="install"><span class="p">$ </span>git clone https://github.com/niclasgrahm/kayak &amp;&amp; cd kayak
<span class="p">$ </span>docker compose up -d        <span class="c"># optional: the brokers and stores</span>
<span class="p">$ </span>just dev                    <span class="c"># → localhost:6767</span></pre>
      <div class="hero-links">
        <span>one rust binary · no database of its own · no signup</span>
      </div>
    </div>

    <svg class="hero-graph" viewBox="0 0 520 330" role="img" aria-label="one pipeline fanning out to four downstream pipelines, with batches crossing the edges">
      <!-- root -->
      <g transform="translate(180 20)">
        <rect class="node" width="160" height="76" rx="3"/>
        <rect class="bar" x="1" y="1" width="158" height="18"/>
        <text x="8" y="14">sensors</text>
        <text class="small" x="8" y="32">NATS · sensors.&gt;</text>
        <g class="bars" transform="translate(8 40)">
          <rect class="in" x="0" y="8" width="3" height="18"/><rect class="out" x="4" y="9" width="3" height="17"/>
          <rect class="in" x="10" y="4" width="3" height="22"/><rect class="out" x="14" y="5" width="3" height="21"/>
          <rect class="in" x="20" y="10" width="3" height="16"/><rect class="out" x="24" y="11" width="3" height="15"/>
          <rect class="in" x="30" y="2" width="3" height="24"/><rect class="out" x="34" y="3" width="3" height="23"/>
          <rect class="in" x="40" y="7" width="3" height="19"/><rect class="out" x="44" y="8" width="3" height="18"/>
          <rect class="in" x="50" y="5" width="3" height="21"/><rect class="out" x="54" y="6" width="3" height="20"/>
          <rect class="in" x="60" y="9" width="3" height="17"/><rect class="out" x="64" y="10" width="3" height="16"/>
          <rect class="in" x="70" y="3" width="3" height="23"/><rect class="out" x="74" y="4" width="3" height="22"/>
          <rect class="in" x="80" y="6" width="3" height="20"/><rect class="out" x="84" y="7" width="3" height="19"/>
          <rect class="in" x="90" y="8" width="3" height="18"/><rect class="out" x="94" y="9" width="3" height="17"/>
          <rect class="in" x="100" y="4" width="3" height="22"/><rect class="out" x="104" y="5" width="3" height="21"/>
          <rect class="in" x="110" y="10" width="3" height="16"/><rect class="out" x="114" y="11" width="3" height="15"/>
          <rect class="in" x="120" y="6" width="3" height="20"/><rect class="out" x="124" y="7" width="3" height="19"/>
          <rect class="in" x="130" y="2" width="3" height="24"/><rect class="out" x="134" y="3" width="3" height="23"/>
        </g>
      </g>
      <!-- edges: orthogonal, down, a channel between the rows -->
      <g>
        <path class="edge" d="M240 96 V150 Q240 156 234 156 H60 Q54 156 54 162 V220"/>
        <path class="edge" d="M256 96 V170 Q256 176 250 176 H197 Q191 176 191 182 V220"/>
        <path class="edge" d="M272 96 V170 Q272 176 278 176 H322 Q328 176 328 182 V220"/>
        <path class="edge" d="M288 96 V150 Q288 156 294 156 H459 Q465 156 465 162 V220"/>
      </g>
      <g>
        <path class="pulse" d="M240 96 V150 Q240 156 234 156 H60 Q54 156 54 162 V220"/>
        <path class="pulse" d="M256 96 V170 Q256 176 250 176 H197 Q191 176 191 182 V220"/>
        <path class="pulse" d="M272 96 V170 Q272 176 278 176 H322 Q328 176 328 182 V220"/>
        <path class="pulse" d="M288 96 V150 Q288 156 294 156 H459 Q465 156 465 162 V220"/>
      </g>
      <!-- children -->
      <g transform="translate(0 220)">
        <rect class="node" width="109" height="70" rx="3"/><rect class="bar" x="1" y="1" width="107" height="18"/>
        <text x="8" y="14">sensors_10s_avg</text><text class="small" x="8" y="32">REDUCER → STDOUT</text>
        <g class="bars" transform="translate(8 40)"><rect class="in" x="0" y="10" width="3" height="16"/><rect class="out" x="4" y="22" width="3" height="4"/><rect class="in" x="10" y="8" width="3" height="18"/><rect class="out" x="14" y="22" width="3" height="4"/><rect class="in" x="20" y="12" width="3" height="14"/><rect class="out" x="24" y="22" width="3" height="4"/><rect class="in" x="30" y="6" width="3" height="20"/><rect class="out" x="34" y="22" width="3" height="4"/><rect class="in" x="40" y="9" width="3" height="17"/><rect class="out" x="44" y="22" width="3" height="4"/><rect class="in" x="50" y="11" width="3" height="15"/><rect class="out" x="54" y="22" width="3" height="4"/><rect class="in" x="60" y="7" width="3" height="19"/><rect class="out" x="64" y="22" width="3" height="4"/><rect class="in" x="70" y="10" width="3" height="16"/><rect class="out" x="74" y="22" width="3" height="4"/><rect class="in" x="80" y="8" width="3" height="18"/><rect class="out" x="84" y="22" width="3" height="4"/><rect class="in" x="90" y="12" width="3" height="14"/><rect class="out" x="94" y="22" width="3" height="4"/></g>
      </g>
      <g transform="translate(137 220)">
        <rect class="node" width="109" height="70" rx="3"/><rect class="bar" x="1" y="1" width="107" height="18"/>
        <text x="8" y="14">sensors_archive</text><text class="small" x="8" y="32">POSTGRES</text>
        <g class="bars" transform="translate(8 40)"><rect class="in" x="0" y="8" width="3" height="18"/><rect class="out" x="4" y="8" width="3" height="18"/><rect class="in" x="10" y="4" width="3" height="22"/><rect class="out" x="14" y="4" width="3" height="22"/><rect class="in" x="20" y="10" width="3" height="16"/><rect class="out" x="24" y="10" width="3" height="16"/><rect class="in" x="30" y="2" width="3" height="24"/><rect class="out" x="34" y="2" width="3" height="24"/><rect class="in" x="40" y="7" width="3" height="19"/><rect class="out" x="44" y="7" width="3" height="19"/><rect class="in" x="50" y="5" width="3" height="21"/><rect class="out" x="54" y="5" width="3" height="21"/><rect class="in" x="60" y="9" width="3" height="17"/><rect class="out" x="64" y="9" width="3" height="17"/><rect class="in" x="70" y="3" width="3" height="23"/><rect class="out" x="74" y="3" width="3" height="23"/><rect class="in" x="80" y="6" width="3" height="20"/><rect class="out" x="84" y="6" width="3" height="20"/><rect class="in" x="90" y="8" width="3" height="18"/><rect class="out" x="94" y="8" width="3" height="18"/></g>
      </g>
      <g transform="translate(274 220)">
        <rect class="node" width="109" height="70" rx="3"/><rect class="bar" x="1" y="1" width="107" height="18"/>
        <text x="8" y="14">hot_readings</text><text class="small" x="8" y="32">FILTER → KAFKA</text>
        <g class="bars" transform="translate(8 40)"><rect class="in" x="0" y="8" width="3" height="18"/><rect class="out" x="4" y="18" width="3" height="8"/><rect class="in" x="10" y="4" width="3" height="22"/><rect class="out" x="14" y="14" width="3" height="12"/><rect class="in" x="20" y="10" width="3" height="16"/><rect class="out" x="24" y="20" width="3" height="6"/><rect class="in" x="30" y="2" width="3" height="24"/><rect class="out" x="34" y="12" width="3" height="14"/><rect class="in" x="40" y="7" width="3" height="19"/><rect class="out" x="44" y="17" width="3" height="9"/><rect class="in" x="50" y="5" width="3" height="21"/><rect class="out" x="54" y="15" width="3" height="11"/><rect class="in" x="60" y="9" width="3" height="17"/><rect class="out" x="64" y="19" width="3" height="7"/><rect class="in" x="70" y="3" width="3" height="23"/><rect class="out" x="74" y="13" width="3" height="13"/><rect class="in" x="80" y="6" width="3" height="20"/><rect class="out" x="84" y="16" width="3" height="10"/><rect class="in" x="90" y="8" width="3" height="18"/><rect class="out" x="94" y="18" width="3" height="8"/></g>
      </g>
      <g transform="translate(411 220)">
        <rect class="node" width="109" height="70" rx="3"/><rect class="bar" x="1" y="1" width="107" height="18"/>
        <text x="8" y="14">sensors_to_s3</text><text class="small" x="8" y="32">S3 · ndjson</text>
        <g class="bars" transform="translate(8 40)"><rect class="in" x="0" y="8" width="3" height="18"/><rect class="out" x="4" y="25" width="3" height="1"/><rect class="in" x="10" y="4" width="3" height="22"/><rect class="out" x="14" y="25" width="3" height="1"/><rect class="in" x="20" y="10" width="3" height="16"/><rect class="out" x="24" y="25" width="3" height="1"/><rect class="in" x="30" y="2" width="3" height="24"/><rect class="out" x="34" y="4" width="3" height="22"/><rect class="in" x="40" y="7" width="3" height="19"/><rect class="out" x="44" y="25" width="3" height="1"/><rect class="in" x="50" y="5" width="3" height="21"/><rect class="out" x="54" y="25" width="3" height="1"/><rect class="in" x="60" y="9" width="3" height="17"/><rect class="out" x="64" y="25" width="3" height="1"/><rect class="in" x="70" y="3" width="3" height="23"/><rect class="out" x="74" y="25" width="3" height="1"/><rect class="in" x="80" y="6" width="3" height="20"/><rect class="out" x="84" y="4" width="3" height="22"/><rect class="in" x="90" y="8" width="3" height="18"/><rect class="out" x="94" y="25" width="3" height="1"/></g>
      </g>
    </svg>
  </div>
</header>

<!-- ============================================================ the tour -->
<section class="tour" id="how" aria-label="how a pipeline works">
  <div class="tour-track">
    <div class="tour-stage">

      <div class="tour-copy">
        <ol class="steps" aria-label="steps">
          <li class="head label">how it works</li>
          <li class="active"><a href="#s-pipeline" data-step="0">a pipeline <small>card</small></a></li>
          <li><a href="#s-inputs" data-step="1">inputs <small>tab 1</small></a></li>
          <li><a href="#s-transforms" data-step="2">transforms <small>tab 2</small></a></li>
          <li><a href="#s-outputs" data-step="3">outputs <small>tab 3</small></a></li>
          <li><a href="#s-graph" data-step="4">the graph <small>zoom out</small></a></li>
        </ol>

        <div class="tour-texts">
          <div class="tour-text active" id="s-pipeline">
            <h2>one pipeline is one card</h2>
            <p>a pipeline is <code>inputs → transforms → outputs</code>. all three are arrays: several inputs are merged into one stream, every output receives every batch, and the transforms run in the order you wrote them.</p>
            <p class="dim">the card on the right is what kayak draws for it — its config in three tabs, a throughput chart, and a live log of the batches that actually went through. that is the whole interface: there is no dashboard beside the graph, the graph is it.</p>
          </div>
          <div class="tour-text" id="s-inputs">
            <h2>inputs: where the messages come from</h2>
            <p>an input reads a subject, a topic, a channel, a set of opc ua nodes — or is posted to, if it's <code>http</code>. it hands the pipeline batches of plain JSON. no schema to declare, no registry, no code generation.</p>
            <p class="dim">every input can <strong>buffer</strong> (by count, by window, or whichever first), attach an <strong>envelope</strong> of what it knows about a message — the subject, the partition, the offset — as ordinary fields, and acknowledge on receipt or on delivery.</p>
            <div class="inventory"><code>nats</code><code>kafka</code><code>mqtt</code><code>redis</code><code>opcua</code><code>http</code><code>pipeline</code><code>dummy</code></div>
          </div>
          <div class="tour-text" id="s-transforms">
            <h2>transforms: what happens on the way</h2>
            <p>a chain of small, named steps. <code>filter</code> keeps or drops. <code>map</code> copies, casts, concatenates, does one arithmetic operation per mapping. <code>reducer</code> aggregates with a <code>group_by</code> and answers several questions in one message. <code>script</code> is where you write actual code, when the shape of the problem stops being configuration.</p>
            <p class="dim">every transform addresses fields by path — <code>value</code>, <code>sensor.id</code>, <code>_meta.subject</code> — and a field either exists or it doesn't. what to do about a message that lacks one is always spelled out, never guessed.</p>
            <div class="inventory"><code>filter</code><code>map</code><code>reducer</code><code>splitter</code><code>buffer</code><code>remember</code><code>recall</code><code>script</code><code>http</code></div>
          </div>
          <div class="tour-text" id="s-outputs">
            <h2>outputs: where they end up</h2>
            <p>every output gets every batch, so "archive to postgres <em>and</em> forward to kafka" is one pipeline with two outputs, not two pipelines. the database outputs map fields onto real columns with real types; the object-store and file outputs rotate parts by rows or by time.</p>
            <p class="dim">an output that isn't reachable yet is retried on backoff rather than failing the run — a postgres that comes up thirty seconds after kayak did is an outage, not a config error, and the card says which.</p>
            <div class="inventory"><code>postgres</code><code>clickhouse</code><code>s3</code><code>file</code><code>kafka</code><code>nats</code><code>mqtt</code><code>redis</code><code>http</code><code>stdout</code></div>
          </div>
          <div class="tour-text" id="s-graph">
            <h2>pipelines feed pipelines</h2>
            <p>the <code>pipeline</code> input subscribes to another pipeline's output. so one that reads the broker once can fan out to a rollup, an archive and an alert — each a card of its own, each with its own transforms, chart and log — and what you have configured is a graph, not a list of jobs.</p>
            <p class="dim">the canvas lays the cards out top to bottom by depth until you drag one somewhere else. edges are square, run along the grid, and light up as a batch crosses them: a busy graph glows, a stalled one doesn't.</p>
          </div>
        </div>
      </div>

      <div class="tour-canvas grid-bg" aria-hidden="true">
        <div class="tour-graph" id="graph">
          <svg class="edges" viewBox="0 0 1140 1100" width="1140" height="1100">
            <!-- three edges leaving the parent's bottom face, fanned out, each into its child's top face -->
            <path d="M552 450 V560 Q552 566 546 566 H186 Q180 566 180 572 V700"/>
            <path d="M570 450 V700"/>
            <path d="M588 450 V580 Q588 586 594 586 H954 Q960 586 960 592 V700"/>
            <path class="pulse" d="M552 450 V560 Q552 566 546 566 H186 Q180 566 180 572 V700"/>
            <path class="pulse" d="M570 450 V700"/>
            <path class="pulse" d="M588 450 V580 Q588 586 594 586 H954 Q960 586 960 592 V700"/>
          </svg>

          <!-- ================= the parent card, the real thing -->
          <div class="card selected" id="card">
            <header><span class="title">sensors</span><span class="max">⤢</span></header>

            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>config</div>
              <div class="tabs">
                <span class="tab active" data-tab="0">inputs (1)</span>
                <span class="tab" data-tab="1">transforms (2)</span>
                <span class="tab" data-tab="2">outputs (2)</span>
              </div>
              <div class="pane">
                <div class="pane-body active" data-pane="0">
                  <div class="section">
                    <div class="section-kind">nats</div>
                    <div class="property"><span class="name">connection</span><span class="value">plant-nats</span></div>
                    <div class="property"><span class="name">subject</span><span class="value">sensors.&gt;</span></div>
                    <div class="property"><span class="name">max_batch</span><span class="value">500</span></div>
                    <div class="property"><span class="name">envelope</span><span class="value">merge</span></div>
                    <div class="property"><span class="name">ack</span><span class="value">on_delivery</span></div>
                  </div>
                </div>
                <div class="pane-body" data-pane="1">
                  <div class="section">
                    <div class="section-kind">filter</div>
                    <div class="property"><span class="name">field</span><span class="value">value</span></div>
                    <div class="property"><span class="name">operator</span><span class="value">gt</span></div>
                    <div class="property"><span class="name">value</span><span class="value">0</span></div>
                  </div>
                  <div class="section">
                    <div class="section-kind">map</div>
                    <div class="property"><span class="name">mappings</span><span class="value">copy sensor · cast value → float · copy ts → recorded_at</span></div>
                    <div class="property"><span class="name">on_missing</span><span class="value">omit</span></div>
                  </div>
                </div>
                <div class="pane-body" data-pane="2">
                  <div class="section">
                    <div class="section-kind">postgres</div>
                    <div class="property"><span class="name">connection</span><span class="value">warehouse</span></div>
                    <div class="property"><span class="name">table</span><span class="value">readings</span></div>
                    <div class="property"><span class="name">columns</span><span class="value">sensor text · value float · recorded_at timestamp</span></div>
                  </div>
                  <div class="section">
                    <div class="section-kind">kafka</div>
                    <div class="property"><span class="name">connection</span><span class="value">plant-kafka</span></div>
                    <div class="property"><span class="name">topic</span><span class="value">readings.clean</span></div>
                  </div>
                </div>
              </div>
            </div>

            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>stats</div>
              <div class="chart">
                <div class="chart-bar">
                  <span class="series in">in</span><span class="series out">out</span>
                  <span class="units"><span class="chip active">5s</span><span class="chip">1m</span><span class="chip">5m</span></span>
                </div>
                <div class="chart-plot">
                  <svg class="chart-svg" viewBox="0 0 100 100" preserveAspectRatio="none"><path class="in" data-series="in"/><path class="out" data-series="out"/></svg>
                  <div class="chart-axis">
                    <div class="axis-mark" style="top:0"><span class="axis-label">500</span></div>
                    <div class="axis-mark" style="top:50%"><span class="axis-label">250</span></div>
                  </div>
                </div>
                <div class="chart-errors quiet"><svg class="chart-svg" viewBox="0 0 100 100" preserveAspectRatio="none"><path d=""/></svg></div>
              </div>
            </div>

            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>logs</div>
              <div class="log-bar">
                <span class="chip active">in</span><span class="chip active">out</span><span class="chip active">err</span>
                <span class="rate" id="rate">92/s</span>
                <span class="act">flat</span><span class="act">pause</span><span class="act">copy</span><span class="act">clear</span>
              </div>
              <div class="log-body" id="log"></div>
            </div>
          </div>

          <!-- ================= the three downstream cards -->
          <div class="card child c0">
            <header><span class="title">sensors_10s_avg</span><span class="max">⤢</span></header>
            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>config</div>
              <div class="tabs"><span class="tab active">inputs (1)</span><span class="tab">transforms (1)</span><span class="tab">outputs (1)</span></div>
              <div class="pane" style="height:92px"><div class="pane-body active">
                <div class="section"><div class="section-kind">pipeline</div>
                  <div class="property"><span class="name">upstream</span><span class="value">sensors</span></div>
                  <div class="property"><span class="name">buffer</span><span class="value">tumbling · 10s</span></div>
                </div></div></div>
            </div>
            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>stats</div>
              <div class="chart">
                <div class="chart-bar"><span class="series in">in</span><span class="series out">out</span><span class="units"><span class="chip active">5s</span><span class="chip">1m</span><span class="chip">5m</span></span></div>
                <div class="chart-plot"><svg class="chart-svg" viewBox="0 0 100 100" preserveAspectRatio="none"><path class="in" data-series="in" data-shape="rollup"/><path class="out" data-series="out" data-shape="rollup"/></svg>
                  <div class="chart-axis"><div class="axis-mark" style="top:0"><span class="axis-label">500</span></div><div class="axis-mark" style="top:50%"><span class="axis-label">250</span></div></div></div>
                <div class="chart-errors quiet"><svg class="chart-svg" viewBox="0 0 100 100" preserveAspectRatio="none"><path d=""/></svg></div>
              </div>
            </div>
            <div class="card-section"><div class="section-head"><span class="chevron">▸</span>logs</div></div>
          </div>

          <div class="card child c1">
            <header><span class="title">sensors_archive</span><span class="max">⤢</span></header>
            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>config</div>
              <div class="tabs"><span class="tab active">inputs (1)</span><span class="tab">transforms (0)</span><span class="tab">outputs (1)</span></div>
              <div class="pane" style="height:92px"><div class="pane-body active">
                <div class="section"><div class="section-kind">pipeline</div>
                  <div class="property"><span class="name">upstream</span><span class="value">sensors</span></div>
                  <div class="property"><span class="name">buffer</span><span class="value">batch · 100 or 5s</span></div>
                </div></div></div>
            </div>
            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>stats</div>
              <div class="chart">
                <div class="chart-bar"><span class="series in">in</span><span class="series out">out</span><span class="units"><span class="chip active">5s</span><span class="chip">1m</span><span class="chip">5m</span></span></div>
                <div class="chart-plot"><svg class="chart-svg" viewBox="0 0 100 100" preserveAspectRatio="none"><path class="in" data-series="in" data-shape="same"/><path class="out" data-series="out" data-shape="same"/></svg>
                  <div class="chart-axis"><div class="axis-mark" style="top:0"><span class="axis-label">500</span></div><div class="axis-mark" style="top:50%"><span class="axis-label">250</span></div></div></div>
                <div class="chart-errors quiet"><svg class="chart-svg" viewBox="0 0 100 100" preserveAspectRatio="none"><path d=""/></svg></div>
              </div>
            </div>
            <div class="card-section"><div class="section-head"><span class="chevron">▸</span>logs</div></div>
          </div>

          <div class="card child c2">
            <header><span class="title">hot_alerts</span><span class="max">⤢</span></header>
            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>config</div>
              <div class="tabs"><span class="tab active">inputs (1)</span><span class="tab">transforms (1)</span><span class="tab">outputs (1)</span></div>
              <div class="pane" style="height:92px"><div class="pane-body active">
                <div class="section"><div class="section-kind">pipeline</div>
                  <div class="property"><span class="name">upstream</span><span class="value">sensors</span></div>
                </div></div></div>
            </div>
            <div class="card-section">
              <div class="section-head"><span class="chevron">▾</span>stats</div>
              <div class="chart">
                <div class="chart-bar"><span class="series in">in</span><span class="series out">out</span><span class="series err">err</span><span class="units"><span class="chip active">5s</span><span class="chip">1m</span><span class="chip">5m</span></span></div>
                <div class="chart-plot"><svg class="chart-svg" viewBox="0 0 100 100" preserveAspectRatio="none"><path class="in" data-series="in" data-shape="sparse"/><path class="out" data-series="out" data-shape="sparse"/></svg>
                  <div class="chart-axis"><div class="axis-mark" style="top:0"><span class="axis-label">500</span></div><div class="axis-mark" style="top:50%"><span class="axis-label">250</span></div></div></div>
                <div class="chart-errors"><svg class="chart-svg" viewBox="0 0 100 100" preserveAspectRatio="none"><path data-series="err"/></svg></div>
              </div>
            </div>
            <div class="card-section"><div class="section-head"><span class="chevron">▸</span>logs</div></div>
          </div>
        </div>
      </div>

    </div>
  </div>
</section>

<!-- ============================================================= config -->
<section class="section-page" id="config">
  <div class="inner">
    <div class="section-head-row">
      <div>
        <span class="label">the config file</span>
        <h2>pipelines are configuration</h2>
      </div>
      <p class="lede">JSON or YAML — the extension decides. the file is a load source and a save target, never a mirror: editing from the canvas starts the new pipeline immediately and writes nothing until you save, and what it writes is deterministic and meant to be committed.</p>
    </div>

    <div class="two-col">
      <div class="code-card">
        <header>kafka → postgres <span class="file">pipelines.yaml</span></header>
<pre><span class="c"># one pipeline, no transforms: every batch off the topic
# goes into the table as it arrives</span>
- <span class="k">id</span>: <span class="s">orders_archive</span>
  <span class="k">inputs</span>:
    - <span class="k">type</span>: <span class="s">kafka</span>
      <span class="k">connection</span>: <span class="s">prod-kafka</span>
      <span class="k">topic</span>: <span class="s">orders</span>
      <span class="k">group</span>: <span class="s">kayak</span>
      <span class="k">ack</span>: <span class="s">on_delivery</span>
  <span class="k">outputs</span>:
    - <span class="k">type</span>: <span class="s">postgres</span>
      <span class="k">connection</span>: <span class="s">warehouse</span>
      <span class="k">table</span>: <span class="s">orders</span></pre>
        <div class="note">without <code>columns</code> the table is <code>id</code> / <code>received_at</code> / <code>payload jsonb</code> — the whole message, as it came. <code>on_delivery</code> commits the offset once postgres has it.</div>
      </div>

      <div class="code-card">
        <header>buffer, filter, reduce, map columns <span class="file">pipelines.yaml</span></header>
<pre>- <span class="k">id</span>: <span class="s">sensors_10s_avg</span>
  <span class="k">inputs</span>:
    - <span class="k">type</span>: <span class="s">pipeline</span>
      <span class="k">upstream</span>: <span class="s">sensors</span>            <span class="c"># another pipeline's output</span>
      <span class="k">buffer</span>: { <span class="k">type</span>: <span class="s">tumbling</span>, <span class="k">window_seconds</span>: <span class="n">10</span> }
  <span class="k">transforms</span>:
    - <span class="k">type</span>: <span class="s">filter</span>
      <span class="k">Number</span>: { <span class="k">field</span>: <span class="s">value</span>, <span class="k">operator</span>: <span class="s">gt</span>, <span class="k">value</span>: <span class="n">0</span> }
    - <span class="k">type</span>: <span class="s">reducer</span>
      <span class="k">group_by</span>: [<span class="s">sensor</span>, <span class="s">_meta.subject</span>]
      <span class="k">on_missing</span>: <span class="s">skip</span>
      <span class="k">aggregations</span>:
        - { <span class="k">function</span>: <span class="s">avg</span>,   <span class="k">field</span>: <span class="s">value</span>, <span class="k">as</span>: <span class="s">mean</span> }
        - { <span class="k">function</span>: <span class="s">max</span>,   <span class="k">field</span>: <span class="s">value</span>, <span class="k">as</span>: <span class="s">highest</span> }
        - { <span class="k">function</span>: <span class="s">count</span>, <span class="k">as</span>: <span class="s">readings</span> }
  <span class="k">outputs</span>:
    - <span class="k">type</span>: <span class="s">clickhouse</span>
      <span class="k">connection</span>: <span class="s">analytics</span>
      <span class="k">table</span>: <span class="s">sensor_rollups</span>
      <span class="k">columns</span>:
        - { <span class="k">name</span>: <span class="s">sensor</span>,   <span class="k">type</span>: <span class="s">text</span> }
        - { <span class="k">name</span>: <span class="s">mean</span>,     <span class="k">type</span>: <span class="s">float</span> }
        - { <span class="k">name</span>: <span class="s">highest</span>,  <span class="k">type</span>: <span class="s">float</span> }
        - { <span class="k">name</span>: <span class="s">readings</span>, <span class="k">type</span>: <span class="s">bigint</span> }
      <span class="k">order_by</span>: [<span class="s">sensor</span>]
    - <span class="k">type</span>: <span class="s">stdout</span></pre>
        <div class="note">the reducer emits one message per distinct <code>(sensor, subject)</code> every ten seconds, and the columns are checked against the plan at build time — a mapped field that isn't there is a decision (<code>on_missing</code>), not a surprise at 3am.</div>
      </div>
    </div>

    <div class="two-col" style="margin-top:24px">
      <div class="code-card">
        <header>declare a system once <span class="file">pipelines.connections.yaml</span></header>
<pre><span class="k">prod-kafka</span>:
  <span class="k">type</span>: <span class="s">kafka</span>
  <span class="k">brokers</span>: <span class="s">kafka-1:9092,kafka-2:9092</span>
<span class="k">warehouse</span>:
  <span class="k">type</span>: <span class="s">postgres</span>
  <span class="k">host</span>: <span class="s">db.internal</span>
  <span class="k">database</span>: <span class="s">events</span>
  <span class="k">user</span>: <span class="s">kayak</span>
  <span class="k">password</span>: <span class="l">${POSTGRES_PASSWORD}</span>
<span class="k">analytics</span>:
  <span class="k">type</span>: <span class="s">clickhouse</span>
  <span class="k">url</span>: <span class="s">https://ch.internal:8443</span>
  <span class="k">database</span>: <span class="s">plant</span>
  <span class="k">user</span>: <span class="s">kayak</span>
  <span class="k">password</span>: <span class="l">${CLICKHOUSE_PASSWORD}</span></pre>
        <div class="note">what a system <em>is</em> lives here under a name; a component says which one it uses and only what it wants from it — a topic, a table, a prefix. credentials are <code>${ENV}</code> references, resolved at startup and never sent back out to the browser.</div>
      </div>

      <div class="code-card">
        <header>the same thing, as JSON <span class="file">pipelines.json</span></header>
<pre>[
  {
    <span class="k">"id"</span>: <span class="s">"orders_archive"</span>,
    <span class="k">"inputs"</span>: [
      { <span class="k">"type"</span>: <span class="s">"kafka"</span>, <span class="k">"connection"</span>: <span class="s">"prod-kafka"</span>,
        <span class="k">"topic"</span>: <span class="s">"orders"</span>, <span class="k">"group"</span>: <span class="s">"kayak"</span>,
        <span class="k">"ack"</span>: <span class="s">"on_delivery"</span> }
    ],
    <span class="k">"transforms"</span>: [],
    <span class="k">"outputs"</span>: [
      { <span class="k">"type"</span>: <span class="s">"postgres"</span>, <span class="k">"connection"</span>: <span class="s">"warehouse"</span>,
        <span class="k">"table"</span>: <span class="s">"orders"</span> }
    ]
  }
]</pre>
        <div class="note">every field, every type and every closed set of values is in the generated reference at <code>/docs</code> — reflected out of the config structs kayak actually deserializes, so it cannot drift from what the server accepts.</div>
      </div>
    </div>
  </div>
</section>

<!-- ============================================================= deploy -->
<section class="section-page" id="deploy">
  <div class="inner">
    <div class="section-head-row">
      <div>
        <span class="label">deploying &amp; configuring</span>
        <h2>one binary, a handful of flags</h2>
      </div>
      <p class="lede">axum server, leptos frontend compiled into it, no runtime dependency, no database of its own, no sidecar. the container image has nothing baked in: started bare it serves an empty graph, and a deployment is a directory mounted in and named on the command line.</p>
    </div>

    <div class="deploy-grid">
      <div class="stack">
        <div class="code-card term">
          <header>run it</header>
<pre><span class="c"># the image; the container's arguments are the server's flags</span>
<span class="prompt">$ </span>docker run -p 6767:6767 -v "$PWD/pipelines:/kayak" \
    -e POSTGRES_PASSWORD \
    ghcr.io/niclasgrahm/kayak \
      --config /kayak/pipelines.yaml \
      --secrets /kayak/secrets.json \
      --data-dir /data

<span class="c"># or the binary, anywhere</span>
<span class="prompt">$ </span>kayak --config pipelines.yaml --listen 0.0.0.0:6767</pre>
          <div class="note">the connections and layout files are found beside the config by name (<code>pipelines.connections.yaml</code>, <code>pipelines.layout.json</code>), so mounting the directory is what you want. <code>linux/amd64</code> and <code>arm64</code>, uid 10001, read-only filesystem unless you'll save from the ui.</div>
        </div>

        <div class="code-card">
          <header>the server's own settings <span class="file">server.yaml</span></header>
<pre><span class="k">auth</span>:                       <span class="c"># off unless declared</span>
  <span class="k">type</span>: <span class="s">basic</span>
  <span class="k">users</span>:
    <span class="k">ops</span>:    { <span class="k">password</span>: <span class="l">${OPS_PASSWORD}</span>,    <span class="k">role</span>: <span class="s">admin</span> }
    <span class="k">viewer</span>: { <span class="k">password</span>: <span class="l">${VIEWER_PASSWORD}</span>, <span class="k">role</span>: <span class="s">read</span> }
<span class="k">history</span>:
  <span class="k">retention_secs</span>: <span class="n">86400</span>     <span class="c"># a day of counters per card; 0 turns it off</span></pre>
          <div class="note">how the server is run, as against what the graph is — a second file so the graph can travel between environments untouched. two roles: <code>admin</code> edits, <code>read</code> looks.</div>
        </div>
      </div>

      <div class="stack">
        <div class="props">
          <header><span class="label">flags</span> <span class="dim" style="font-weight:400;font-size:11px;font-family:var(--font-mono)">kayak --help</span></header>
          <div class="rows">
            <div class="row"><span class="name">--config &lt;path&gt;</span><span class="desc">the pipelines, JSON or YAML. optional: without it the server starts with an empty graph and the first <code>save as…</code> creates the file.</span></div>
            <div class="row"><span class="name">--connections &lt;path&gt;</span><span class="desc">the systems file, when it isn't beside the config — fixed for the process, so two configs can share one.</span></div>
            <div class="row"><span class="name">--secrets &lt;path&gt;</span><span class="desc">a JSON map for <code>${NAME}</code> references. environment variables are tried first; an unresolved name refuses to start rather than connecting without credentials.</span></div>
            <div class="row"><span class="name">--data-dir &lt;path&gt;</span><span class="desc">the one directory <code>file</code> outputs may write under. without it they refuse to build — closed by default.</span></div>
            <div class="row"><span class="name">--server-config &lt;path&gt;</span><span class="desc">auth and history: what belongs to the deployment rather than to the graph.</span></div>
            <div class="row"><span class="name">--listen &lt;addr&gt;</span><span class="desc">where to bind. defaults to <code>127.0.0.1:6767</code>; the image sets <code>0.0.0.0</code>.</span></div>
            <div class="row"><span class="name">--debug</span><span class="desc">more tracing.</span></div>
          </div>
        </div>

        <div class="props">
          <header><span class="label">what it costs to leave running</span></header>
          <div class="rows">
            <div class="row"><span class="name">headless</span><span class="desc">nothing for the ui. run loops publish to the event feed only while a browser is attached, and at most ten passes a second when one is.</span></div>
            <div class="row"><span class="name">history</span><span class="desc">about 58 kB per pipeline for a day, flat in throughput — buckets hold counts, never messages.</span></div>
            <div class="row"><span class="name">runtime</span><span class="desc">~7M pipeline passes/s on one core, i/o excluded. the pipeline is not the bottleneck; whatever it talks to is.</span></div>
          </div>
        </div>
      </div>
    </div>

    <div class="facts">
      <div><span class="label">one process</span><p>no clustering, no distributed state, no exactly-once across a fleet. the jobs below the line where you'd need one.</p></div>
      <div><span class="label">state &amp; history in memory</span><p>a decision, not a stage: durable state without checkpointed input positions would be wrong invisibly.</p></div>
      <div><span class="label">tables created, never altered</span><p><code>IF NOT EXISTS</code> and nothing more. migrating a live table from a config file is a bigger promise than kayak makes.</p></div>
      <div><span class="label">pre-1.0</span><p>things move. the roadmap is in the repo, and the reference is regenerated by the test suite.</p></div>
    </div>
  </div>
</section>

<!-- ================================================================ api -->
<section class="section-page" id="api">
  <div class="inner">
    <div class="section-head-row">
      <div>
        <span class="label">the http api</span>
        <h2>every button is an endpoint</h2>
      </div>
      <p class="lede">the canvas is a client. anything it does — create a pipeline, post messages into one, read a bucket, save the file — is a JSON request you could have made yourself, from a script, a ci job, or an agent that has read the spec.</p>
    </div>

    <div class="api-grid">
      <div class="endpoints">
        <header>endpoints <span class="count">from the same table the router is built from</span></header>
        <ul>
          <li><span class="verb">GET</span><span class="path">/api/pipelines</span><span class="what">the running graph</span></li>
          <li><span class="verb post">POST</span><span class="path">/api/pipelines</span><span class="what">create one — it starts now</span></li>
          <li><span class="verb del">DEL</span><span class="path">/api/pipelines/<i>{id}</i></span><span class="what">stop and remove</span></li>
          <li><span class="verb post">POST</span><span class="path">/api/pipelines/<i>{id}</i>/messages</span><span class="what">ingest, via an http input</span></li>
          <li><span class="verb">GET</span><span class="path">/api/pipelines/<i>{id}</i>/history</span><span class="what">a day of counters &amp; failures</span></li>
          <li><span class="verb post">POST</span><span class="path">/api/pipelines/dry-run</span><span class="what">run a chain, emit nothing</span></li>
          <li><span class="verb post">POST</span><span class="path">/api/inputs/sample</span><span class="what">a few messages off an input</span></li>
          <li><span class="verb post">POST</span><span class="path">/api/scripts/dry-run</span><span class="what">compile &amp; try a script</span></li>
          <li><span class="verb">GET</span><span class="path">/api/connections</span><span class="what">the systems</span></li>
          <li><span class="verb">GET</span><span class="path">/api/state/<i>{bucket}</i></span><span class="what">a bucket, key by key</span></li>
          <li><span class="verb post">POST</span><span class="path">/api/config/save</span><span class="what">write the file</span></li>
          <li><span class="verb post">POST</span><span class="path">/api/config/revert</span><span class="what">reload it</span></li>
          <li><span class="verb put">PUT</span><span class="path">/api/layout</span><span class="what">where the cards are</span></li>
          <li><span class="verb">GET</span><span class="path">/events</span><span class="what">sse — the live feed</span></li>
          <li><span class="verb">GET</span><span class="path">/api/docs</span><span class="what">every component, as JSON</span></li>
          <li><span class="verb">GET</span><span class="path">/api/openapi.json</span><span class="what">openapi 3.1</span></li>
        </ul>
        <div class="more">and auth, settings, and a rendered reference at <code>/api/reference</code>. an endpoint missing from the table isn't routed — the table is the routes.</div>
      </div>

      <div class="stack">
        <div class="code-card term">
          <header>from a shell</header>
<pre><span class="c"># a pipeline with an http input is an ingest endpoint at its own id</span>
<span class="prompt">$ </span>curl -X POST localhost:6767/api/pipelines/ingest/messages \
    -H 'authorization: Bearer $DEVICE_TOKEN' \
    -d '[{"sensor":"line1/temp","value":21.5}]'
<span class="out">HTTP/1.1 202 Accepted</span>

<span class="c"># try a chain against real messages before it exists</span>
<span class="prompt">$ </span>curl -X POST localhost:6767/api/pipelines/dry-run -d @draft.json | jq '.stages[].batches'

<span class="c"># what broke overnight, with nobody watching</span>
<span class="prompt">$ </span>curl localhost:6767/api/pipelines/hot_alerts/history?resolution=coarse
<span class="out">{ "errors": [ { "stage": "output http", "count": 240,
                "first_seen": "02:14:07Z", "last_seen": "08:03:11Z",
                "message": "webhook returned 503 …" } ], … }</span></pre>
          <div class="note">the ingest endpoint's credential is its own — bearer or header, per pipeline — and not the server's sign-in, because a device posting readings isn't an operator. a full queue is a <code>503</code>, never a request held open.</div>
        </div>

        <div class="code-card">
          <header>for software that reads specs</header>
          <div class="note" style="border-top:none;font-size:13.5px;color:var(--text)">
            <p><code>/api/openapi.json</code> is generated from the same table the server builds its routes from, and the request and response bodies from the rust types — so it describes the server you are actually talking to, not the one the docs were written against.</p>
            <p><code>/api/docs</code> is the component reference as JSON: every input, transform, output and connection kind, every field with its type, whether it's required, and the closed set of values it takes. that is enough for an agent to write a valid pipeline without having seen one — and <code>dry-run</code> is how it finds out whether the pipeline does what it meant, without emitting anything.</p>
            <p style="margin:0" class="dim">integration tokens sign in at <code>/api/auth/token</code>; the same two roles apply.</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</section>

<footer class="footer grid-bg">
  <div class="inner">
    <div>
      <span class="label">run it</span>
      <pre class="install" style="margin-top:10px"><span class="p">$ </span>git clone https://github.com/niclasgrahm/kayak &amp;&amp; cd kayak
<span class="p">$ </span>just dev                    <span class="c"># → localhost:6767</span></pre>
    </div>
    <div class="links">
      <a href="https://github.com/niclasgrahm/kayak">github</a>
      <a href="https://propell.dev/kayak/getting-started">getting started</a>
      <a href="https://propell.dev/kayak/reference/">component reference</a>
      <a href="https://propell.dev/kayak/operating/deployment">deployment</a>
    </div>
    <p class="fine">kayak — built with rust, axum, tokio and leptos. the name is lowercase, even here.</p>
  </div>
</footer>
</div>
</template>

<style>
/* Every colour is the product's own token (style/main.scss), verbatim. Every
   rule is prefixed `.landing` so nothing here reaches the rest of the site
   and nothing of the site's reaches in — `.card`, `.tabs` and `.label` are
   names kayak.css and vitepress both have opinions about. */
.landing {--bg-canvas: #1d2129; --bg-panel: #262b33; --bg-titlebar: #1b1f26; --bg-hover: #2f3540; --border: #14171c; --text: #cdced2; --text-dim: #85878c; --accent: #699ce8; --error: #e06c75; --error-bg: #3a2226; --stat-in: #699ce8; --stat-out: #d8a657; --json-key: #7fbbb3; --json-str: #a7c080; --json-num: #d8a657; --json-literal: #d699b6; --radius: 4px; --grid: 20px; --font-sans: "Noto Sans", "Open Sans", system-ui, -apple-system, sans-serif; --font-mono: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace; --measure: 34rem; --page-x: clamp(20px, 5vw, 72px); }
.landing *, .landing *::before, .landing *::after {box-sizing: border-box; }
.landing {margin: 0; overflow-x: clip; background: var(--bg-canvas); color: var(--text); font-family: var(--font-sans); font-size: 15px; line-height: 1.6; -webkit-font-smoothing: antialiased; }
.landing a {color: var(--accent); text-decoration: none; }
.landing a:hover {text-decoration: underline; text-underline-offset: 2px; }
.landing :focus-visible {outline: 2px solid var(--accent); outline-offset: 2px; }
.landing code, .landing kbd, .landing pre {font-family: var(--font-mono); }
.landing p code, .landing li code, .landing h2 code, .landing h3 code, .landing dt code {font-size: 0.88em; background: var(--bg-titlebar); border: 1px solid var(--border); border-radius: 2px; padding: 0 4px; color: var(--text); }
.landing .grid-bg {background-image: linear-gradient(rgba(255,255,255,0.045) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.045) 1px, transparent 1px); background-size: var(--grid) var(--grid); }
.landing .label {font-size: 11px; letter-spacing: 0.07em; text-transform: uppercase; color: var(--text-dim); font-weight: 600; }
.landing h1, .landing h2, .landing h3 {font-weight: 700; letter-spacing: -0.01em; line-height: 1.15; margin: 0; }
.landing h2 {font-size: clamp(26px, 3.4vw, 40px); }
.landing h3 {font-size: 18px; }
.landing .lede {color: var(--text-dim); font-size: 17px; max-width: var(--measure); }
.landing p {margin: 0 0 1em; }
.landing .dim {color: var(--text-dim); }
.landing .navbar {position: sticky; top: 0; z-index: 20; background: var(--bg-titlebar); border-bottom: 1px solid var(--border); padding: 6px var(--page-x); font-size: 12px; display: flex; align-items: center; gap: 4px; }
.landing .navbar .brand {font-weight: 700; color: var(--text); margin-right: 8px; }
.landing .navbar a.tab, .landing .navbar span.tab {color: var(--text-dim); padding: 2px 8px; border-radius: var(--radius); }
.landing .navbar a.tab:hover {color: var(--text); background: var(--bg-hover); text-decoration: none; }
.landing .navbar .tab.active {background: var(--bg-hover); color: var(--text); }
.landing .navbar .right {margin-left: auto; display: flex; align-items: center; gap: 10px; }
.landing .navbar .version {font-family: var(--font-mono); color: var(--text-dim); font-size: 11px; }
.landing .navbar .btn {border: 1px solid var(--border); background: var(--bg-panel); color: var(--text); padding: 2px 8px; border-radius: var(--radius); }
.landing .navbar .btn:hover {background: var(--bg-hover); text-decoration: none; }
.landing .hero {position: relative; padding: clamp(64px, 12vh, 140px) var(--page-x) clamp(56px, 10vh, 120px); border-bottom: 1px solid var(--border); overflow: hidden; }
.landing .hero-inner > * {min-width: 0; }
.landing .hero-inner {display: grid; grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr); gap: 48px; align-items: center; max-width: 1240px; margin: 0 auto; }
.landing .hero h1 {font-size: clamp(40px, 6.2vw, 78px); font-weight: 800; letter-spacing: -0.035em; line-height: 1.02; max-width: 12ch; }
.landing .hero h1 .thin {font-weight: 400; color: var(--text-dim); }
.landing .hero .lede {margin: 26px 0 30px; font-size: clamp(16px, 1.5vw, 19px); }
.landing .hero .lede code {font-size: 0.9em; }
.landing .install {display: inline-block; background: var(--bg-titlebar); border: 1px solid var(--border); border-radius: var(--radius); padding: 12px 16px; font-family: var(--font-mono); font-size: 12.5px; line-height: 1.75; white-space: pre; color: var(--text); max-width: 100%; overflow-x: auto; }
.landing .install .c {color: var(--text-dim); }
.landing .install .p {color: var(--text-dim); user-select: none; }
.landing .hero-links {margin-top: 18px; font-size: 13px; color: var(--text-dim); display: flex; gap: 18px; flex-wrap: wrap; }
.landing .hero-graph {width: 100%; height: auto; display: block; }
.landing .hero-graph .node {fill: var(--bg-panel); stroke: var(--border); }
.landing .hero-graph .bar {fill: var(--bg-titlebar); }
.landing .hero-graph text {font-family: var(--font-mono); font-size: 10px; fill: var(--text); }
.landing .hero-graph text.small {font-size: 8px; fill: var(--text-dim); letter-spacing: 0.06em; }
.landing .hero-graph .edge {fill: none; stroke: var(--text-dim); stroke-width: 2; }
.landing .hero-graph .pulse {fill: none; stroke: var(--accent); stroke-width: 3; stroke-linecap: round; opacity: 0; }
.landing .hero-graph .bars rect.in {fill: var(--stat-in); }
.landing .hero-graph .bars rect.out {fill: var(--stat-out); }
@keyframes edge-pulse {
  0% {opacity: 0; }
  6% {opacity: 1; }
  100% {opacity: 0; } }
.landing .hero-graph .pulse {animation: edge-pulse 2.6s ease-out infinite; }
.landing .hero-graph .pulse:nth-child(2) {animation-delay: 0.7s; }
.landing .hero-graph .pulse:nth-child(3) {animation-delay: 1.3s; }
.landing .hero-graph .pulse:nth-child(4) {animation-delay: 1.9s; }
@media (prefers-reduced-motion: reduce) {
  .landing .hero-graph .pulse {animation: none; opacity: 0.5; } }
.landing .tour {position: relative; }
.landing .tour-track {height: 560vh; }
.landing .tour-stage {position: sticky; top: var(--vp-nav-height, 0px); height: calc(100vh - var(--vp-nav-height, 0px)); height: calc(100svh - var(--vp-nav-height, 0px)); display: grid; grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr); gap: 24px; align-items: center; padding: 0 var(--page-x); max-width: 1240px; margin: 0 auto; overflow: hidden; }
.landing .tour-copy {position: relative; display: grid; grid-template-columns: 172px minmax(0, 1fr); gap: 28px; align-items: start; }
.landing .steps {list-style: none; margin: 0; padding: 0; border: 1px solid var(--border); border-radius: var(--radius); background: var(--bg-panel); overflow: hidden; }
.landing .steps .head {padding: 5px 10px; background: var(--bg-titlebar); border-bottom: 1px solid var(--border); }
.landing .steps li a {display: flex; align-items: baseline; justify-content: space-between; gap: 8px; padding: 5px 10px; font-size: 13px; color: var(--text-dim); border-left: 2px solid transparent; }
.landing .steps li a:hover {background: var(--bg-hover); color: var(--text); text-decoration: none; }
.landing .steps li a small {font-family: var(--font-mono); font-size: 10px; color: var(--text-dim); }
.landing .steps li.active a {background: var(--bg-hover); color: var(--text); border-left-color: var(--accent); }
.landing .tour-texts {display: grid; }
.landing .tour-text {grid-area: 1 / 1; opacity: 0; transform: translateY(10px); transition: opacity 320ms ease, transform 320ms ease; pointer-events: none; }
.landing .tour-text.active {opacity: 1; transform: none; pointer-events: auto; }
.landing .tour-text h2 {margin-bottom: 14px; font-size: clamp(24px, 2.6vw, 34px); }
.landing .tour-text p {max-width: var(--measure); color: var(--text); }
.landing .tour-text p.dim {color: var(--text-dim); }
.landing .inventory {display: flex; flex-wrap: wrap; gap: 4px; margin: 14px 0 4px; }
.landing .inventory code {font-size: 11px; padding: 1px 6px; background: var(--bg-panel); border: 1px solid var(--border); border-radius: 2px; color: var(--text); }
.landing .inventory code.soon {color: var(--text-dim); }
@media (prefers-reduced-motion: reduce) {
  .landing .tour-text {transition: none; } }
.landing .tour-canvas {position: relative; height: min(88vh, 760px); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
.landing .tour-graph {position: absolute; left: 50%; top: 50%; width: 1140px; transform-origin: 0 0; transition: transform 700ms cubic-bezier(.2,.7,.2,1); }
.landing .tour-graph .edges {position: absolute; inset: 0; width: 1140px; height: 1100px; pointer-events: none; overflow: visible; }
.landing .tour-graph .edges path {fill: none; stroke: var(--text-dim); stroke-width: 2; opacity: 0; transition: opacity 500ms ease 200ms; }
.landing .tour-graph .edges .pulse {stroke: var(--accent); stroke-width: 3; }
.landing .tour.step-4 .tour-graph .edges path {opacity: 1; }
.landing .tour.step-4 .tour-graph .edges .pulse {animation: edge-pulse 2.4s ease-out infinite; }
.landing .tour.step-4 .tour-graph .edges .pulse:nth-of-type(2) {animation-delay: 0.8s; }
.landing .tour.step-4 .tour-graph .edges .pulse:nth-of-type(3) {animation-delay: 1.6s; }
@media (prefers-reduced-motion: reduce) {
  .landing .tour-graph {transition: none; }
  .landing .tour.step-4 .tour-graph .edges .pulse {animation: none; opacity: 0; } }
.landing .card {position: absolute; left: 390px; top: 0; width: 360px; background: var(--bg-panel); border: 1px solid var(--border); border-radius: var(--radius); box-shadow: 0 4px 16px rgba(0,0,0,0.45); overflow: hidden; display: flex; flex-direction: column; font-size: 13px; line-height: 1.4; color: var(--text); transition: border-color 300ms ease; }
.landing .card.selected {border-color: var(--accent); }
.landing .card > header {background: var(--bg-titlebar); padding: 8px 10px; font-weight: 600; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 8px; }
.landing .card > header .title {flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.landing .card > header .max {color: var(--text-dim); font-size: 11px; }
.landing .card-section {display: flex; flex-direction: column; min-height: 0; }
.landing .card-section + .card-section > .section-head {border-top: 1px solid var(--border); }
.landing .section-head {display: flex; align-items: center; gap: 5px; padding: 3px 8px; color: var(--text-dim); font-size: 10px; letter-spacing: 0.05em; text-transform: uppercase; font-weight: 400; }
.landing .section-head .chevron {width: 9px; font-size: 10px; line-height: 1; }
.landing .tabs {display: flex; background: var(--bg-titlebar); border-bottom: 1px solid var(--border); }
.landing .tabs .tab {flex: 1; text-align: center; border-right: 1px solid var(--border); border-top: 2px solid transparent; color: var(--text-dim); font-size: 11px; padding: 4px 6px; transition: background 200ms ease, border-color 200ms ease; }
.landing .tabs .tab:last-child {border-right: none; }
.landing .tabs .tab.active {background: var(--bg-panel); border-top-color: var(--accent); color: var(--text); }
.landing .pane {padding: 6px; height: 176px; overflow: hidden; position: relative; }
.landing .pane-body {position: absolute; inset: 6px; opacity: 0; transition: opacity 260ms ease; }
.landing .pane-body.active {opacity: 1; }
.landing .section + .section {margin-top: 6px; }
.landing .section-kind {background: var(--bg-hover); border-radius: 2px; padding: 2px 6px; margin-bottom: 4px; font-size: 10px; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; }
.landing .property {display: grid; grid-template-columns: 40% 1fr; gap: 6px; align-items: center; padding: 1px 2px; }
.landing .property .name {color: var(--text-dim); font-size: 11px; }
.landing .property .value {background: var(--bg-canvas); border: 1px solid var(--border); border-radius: 2px; padding: 1px 5px; font-family: var(--font-mono); font-size: 11px; }
.landing .property .name, .landing .property .value {overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.landing .property.hl .value {border-color: var(--accent); }
.landing .empty {color: var(--text-dim); font-size: 11px; font-style: italic; padding: 2px; }
.landing .chart {display: flex; flex-direction: column; gap: 5px; padding: 5px 8px 7px; }
.landing .chart-bar {display: flex; align-items: center; gap: 8px; font-size: 10px; color: var(--text-dim); }
.landing .chart-bar .series {display: flex; align-items: center; gap: 4px; }
.landing .chart-bar .series::before {content: ""; width: 6px; height: 6px; border-radius: 1px; }
.landing .chart-bar .series.in::before {background: var(--stat-in); }
.landing .chart-bar .series.out::before {background: var(--stat-out); }
.landing .chart-bar .series.err::before {background: var(--error); }
.landing .chart-bar .units {display: flex; gap: 2px; }
.landing .chart-bar .chip {border: 1px solid transparent; border-radius: var(--radius); padding: 0 4px; font-size: 10px; }
.landing .chart-bar .chip.active {border-color: var(--accent); color: var(--text); }
.landing .chart-plot {position: relative; height: 54px; border-bottom: 1px solid var(--border); }
.landing .chart-svg {display: block; width: 100%; height: 100%; }
.landing .chart-svg path {stroke: none; }
.landing .chart-svg path.in {fill: var(--stat-in); }
.landing .chart-svg path.out {fill: var(--stat-out); }
.landing .chart-axis {position: absolute; inset: 0; pointer-events: none; }
.landing .axis-mark {position: absolute; left: 0; right: 0; border-top: 1px dashed var(--border); height: 0; }
.landing .axis-label {position: absolute; right: 0; bottom: 1px; padding-left: 3px; font-family: var(--font-mono); font-size: 9px; line-height: 1; color: var(--text-dim); background: var(--bg-panel); opacity: 0.85; }
.landing .chart-errors {position: relative; height: 14px; margin-top: 3px; border-bottom: 1px solid var(--border); }
.landing .chart-errors .chart-svg path {fill: var(--error); }
.landing .chart-errors.quiet {opacity: 0.25; }
.landing .log-bar {display: flex; align-items: center; gap: 3px; padding: 2px 6px; background: var(--bg-titlebar); font-size: 10px; }
.landing .log-bar .chip {border: 1px solid var(--border); border-radius: var(--radius); padding: 1px 5px; color: var(--text-dim); }
.landing .log-bar .chip.active {border-color: var(--accent); color: var(--text); }
.landing .log-bar .rate {margin-left: auto; color: var(--text-dim); font-family: var(--font-mono); }
.landing .log-bar .act {color: var(--text-dim); padding: 1px 5px; }
.landing .log-body {padding: 4px 8px; font-family: var(--font-mono); font-size: 11px; overflow: hidden; }
.landing .log-row {display: flex; gap: 6px; padding: 0 6px; line-height: 1.6; color: var(--text-dim); white-space: nowrap; overflow: hidden; }
.landing .log-row:last-child {color: var(--text); }
.landing .log-row .t, .landing .log-row .s {flex: none; }
.landing .log-row .s {width: 3ch; }
.landing .log-row .m {overflow: hidden; text-overflow: ellipsis; }
.landing .log-row.error {color: var(--error); background: var(--error-bg); }
.landing .card.child {top: 700px; opacity: 0; transition: opacity 500ms ease 150ms; }
.landing .tour.step-4 .card.child {opacity: 1; }
.landing .card.child.c0 {left: 0; }
.landing .card.child.c1 {left: 390px; }
.landing .card.child.c2 {left: 780px; }
.landing .section-page {padding: clamp(64px, 10vh, 120px) var(--page-x); border-top: 1px solid var(--border); }
.landing .section-page > .inner {max-width: 1240px; margin: 0 auto; }
.landing .section-head-row {display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.3fr); gap: 32px 64px; align-items: end; margin-bottom: 40px; }
.landing .section-head-row .label {margin-bottom: 12px; display: block; }
.landing .section-head-row .lede {margin: 0; }
.landing .two-col {display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 24px; }
.landing .code-card {border: 1px solid var(--border); border-radius: var(--radius); background: var(--bg-panel); overflow: hidden; display: flex; flex-direction: column; min-width: 0; }
.landing .code-card > header {background: var(--bg-titlebar); border-bottom: 1px solid var(--border); padding: 6px 10px; font-size: 12px; font-weight: 600; display: flex; align-items: center; gap: 10px; }
.landing .code-card > header .file {font-family: var(--font-mono); font-weight: 400; font-size: 11px; color: var(--text-dim); margin-left: auto; }
.landing .code-card pre {margin: 0; padding: 12px 14px; font-size: 12px; line-height: 1.6; overflow-x: auto; color: var(--text); flex: 1; }
.landing .code-card .note {padding: 8px 12px; border-top: 1px solid var(--border); font-size: 12.5px; color: var(--text-dim); }
.landing .code-card .note code {color: var(--text); }
.landing .k {color: var(--json-key); }
.landing .s {color: var(--json-str); }
.landing .n {color: var(--json-num); }
.landing .l {color: var(--json-literal); }
.landing .c {color: var(--text-dim); }
.landing .d {color: var(--text-dim); }
.landing .pre-annot {color: var(--text-dim); }
.landing .deploy-grid {display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(0, 1fr); gap: 24px; align-items: start; }
.landing .props {border: 1px solid var(--border); border-radius: var(--radius); background: var(--bg-panel); overflow: hidden; }
.landing .props > header {background: var(--bg-titlebar); border-bottom: 1px solid var(--border); padding: 6px 10px; font-size: 12px; font-weight: 600; display: flex; gap: 10px; align-items: center; }
.landing .props > header .label {font-weight: 600; }
.landing .props .rows {padding: 6px; }
.landing .props .row {display: grid; grid-template-columns: 200px minmax(0, 1fr); gap: 8px; align-items: baseline; padding: 4px 2px; border-top: 1px solid transparent; }
.landing .props .row + .row {border-top-color: rgba(20,23,28,0.6); }
.landing .props .row .name {font-family: var(--font-mono); font-size: 12px; color: var(--text); background: var(--bg-canvas); border: 1px solid var(--border); border-radius: 2px; padding: 1px 6px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.landing .props .row .desc {font-size: 13px; color: var(--text-dim); }
.landing .props .row .desc code {color: var(--text); }
.landing .stack {display: flex; flex-direction: column; gap: 24px; }
.landing .facts {display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 1px; background: var(--border); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; margin-top: 40px; }
.landing .facts div {background: var(--bg-panel); padding: 14px 16px; }
.landing .facts .label {display: block; margin-bottom: 6px; }
.landing .facts p {margin: 0; font-size: 13px; color: var(--text); }
.landing .facts p code {font-size: 12px; }
.landing .api-grid {display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.2fr); gap: 24px; align-items: start; }
.landing .endpoints {border: 1px solid var(--border); border-radius: var(--radius); background: var(--bg-panel); overflow: hidden; }
.landing .endpoints > header {background: var(--bg-titlebar); border-bottom: 1px solid var(--border); padding: 6px 10px; font-size: 12px; font-weight: 600; display: flex; align-items: center; gap: 10px; }
.landing .endpoints > header .count {margin-left: auto; font-family: var(--font-mono); font-weight: 400; font-size: 11px; color: var(--text-dim); }
.landing .endpoints ul {list-style: none; margin: 0; padding: 4px 0; }
.landing .endpoints li {display: grid; grid-template-columns: 5ch minmax(0, 1fr) auto; gap: 10px; padding: 3px 10px; font-family: var(--font-mono); font-size: 12px; align-items: baseline; }
.landing .endpoints li:hover {background: var(--bg-hover); }
.landing .endpoints .verb {color: var(--text-dim); font-size: 10px; letter-spacing: 0.04em; }
.landing .endpoints .verb.post {color: var(--stat-out); }
.landing .endpoints .verb.put {color: var(--json-key); }
.landing .endpoints .verb.del {color: var(--error); }
.landing .endpoints .path {color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.landing .endpoints .path i {font-style: normal; color: var(--json-literal); }
.landing .endpoints .what {color: var(--text-dim); font-family: var(--font-sans); font-size: 12px; white-space: nowrap; }
.landing .endpoints .more {padding: 6px 10px 8px; font-size: 12px; color: var(--text-dim); border-top: 1px solid var(--border); }
.landing .term pre .out {color: var(--text-dim); }
.landing .term pre .prompt {color: var(--text-dim); user-select: none; }
.landing .footer {border-top: 1px solid var(--border); padding: 48px var(--page-x) 40px; }
.landing .footer .inner {max-width: 1240px; margin: 0 auto; display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 24px 48px; align-items: end; }
.landing .footer .links {display: flex; gap: 18px; flex-wrap: wrap; font-size: 13px; }
.landing .footer .fine {font-size: 12px; color: var(--text-dim); margin-top: 24px; grid-column: 1 / -1; }
@media (max-width: 1080px) {
  .landing .tour-stage {top: 0; height: 100vh; height: 100svh; }
  .landing .hero-inner {grid-template-columns: 1fr; }
  .landing .install {display: block; font-size: 11.5px; }
  .landing .hero-graph {max-width: 640px; }
  .landing .tour-stage {grid-template-columns: 1fr; grid-template-rows: auto minmax(0, 1fr); gap: 12px; align-items: stretch; padding-top: 16px; padding-bottom: 16px; }
  .landing .tour-copy {grid-template-columns: 1fr; gap: 12px; }
  .landing .steps {display: flex; }
  .landing .steps .head {display: none; }
  .landing .steps li {flex: 1; }
  .landing .steps li a {justify-content: center; padding: 6px 4px; font-size: 12px; border-left: none; border-bottom: 2px solid transparent; }
  .landing .steps li.active a {border-bottom-color: var(--accent); }
  .landing .steps li a small {display: none; }
  .landing .tour-text h2 {font-size: 20px; margin-bottom: 8px; }
  .landing .tour-text p {font-size: 14px; }
  .landing .tour-canvas {height: auto; min-height: 0; }
  .landing .two-col, .landing .deploy-grid, .landing .api-grid, .landing .section-head-row {grid-template-columns: 1fr; }
  .landing .facts {grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .landing .footer .inner {grid-template-columns: 1fr; } }
@media (max-width: 560px) {
  .landing .facts {grid-template-columns: 1fr; }
  .landing .props .row {grid-template-columns: 1fr; gap: 2px; }
  .landing .endpoints li {grid-template-columns: 5ch minmax(0, 1fr); }
  .landing .endpoints .what {display: none; }
  .landing .inventory code {font-size: 10px; } }

/* vitepress' base styles reach into the page; these put back what the design assumes */
.landing h1, .landing h2, .landing h3 { border: none; padding: 0; margin: 0; letter-spacing: -0.01em; }
.landing h2 { font-size: clamp(26px, 3.4vw, 40px); }
.landing h3 { font-size: 18px; }
.landing p { margin: 0 0 1em; line-height: 1.6; }
.landing pre, .landing code { font-family: var(--font-mono); }
.landing pre { background: none; }
.landing ol, .landing ul { padding: 0; margin: 0; }
.landing a { text-decoration: none; }
.landing a:hover { text-decoration: underline; }
</style>
