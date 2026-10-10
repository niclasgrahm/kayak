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
      <h1>stream processing <span class="thin">in one binary</span></h1>
      <p class="lede">kayak is a stream processor written in Rust. You write pipelines in a config file and keep the file in version control. You run the container image with that file. That is the complete deployment.</p>
      <pre class="install"><span class="p">$ </span>docker run -p 6767:6767 -v "$PWD:/kayak" \
    ghcr.io/niclasgrahm/kayak --config /kayak/config.yaml</pre>
      <div class="hero-links">
        <a href="https://propell.dev/kayak/getting-started">getting started</a>
        <a href="https://propell.dev/kayak/reference/">reference</a>
        <a href="https://github.com/niclasgrahm/kayak">github</a>
      </div>
      <p class="hero-fine">one rust binary · JSON or YAML · no cluster · no database of its own</p>
    </div>

    <div class="code-card">
      <header>nats → filter → clickhouse <span class="file">config.yaml</span></header>
<pre>- <span class="k">id</span>: <span class="s">warm_sensors</span>
  <span class="k">inputs</span>:
    - <span class="k">type</span>: <span class="s">nats</span>
      <span class="k">connection</span>: <span class="s">plant-nats</span>
      <span class="k">subject</span>: <span class="s">sensors.&gt;</span>
      <span class="k">max_batch</span>: <span class="n">500</span>
  <span class="k">transforms</span>:
    - <span class="k">type</span>: <span class="s">filter</span>
      <span class="k">conditions</span>:
        - { <span class="k">type</span>: <span class="s">numeric</span>, <span class="k">field</span>: <span class="s">value</span>,
            <span class="k">operator</span>: <span class="s">greater_than</span>, <span class="k">value</span>: <span class="n">30</span> }
  <span class="k">outputs</span>:
    - <span class="k">type</span>: <span class="s">clickhouse</span>
      <span class="k">connection</span>: <span class="s">analytics</span>
      <span class="k">table</span>: <span class="s">warm_readings</span></pre>
      <div class="note">A pipeline is <code>inputs → transforms → outputs</code>. The names <code>plant-nats</code> and <code>analytics</code> are connections. You declare them one time, in <code>config.connections.yaml</code> beside this file.</div>
    </div>
  </div>
</header>

<!-- ============================================================ the tour -->
<section class="tour" id="how" aria-label="how a pipeline works">
  <div class="tour-track">
    <div class="tour-stage">

      <div class="tour-copy">
        <ol class="steps" aria-label="steps">
          <li class="head label">how it works</li>
          <li class="active"><a href="#s-pipeline" data-step="0">a pipeline <small>config</small></a></li>
          <li><a href="#s-inputs" data-step="1">inputs <small>step 1</small></a></li>
          <li><a href="#s-transforms" data-step="2">transforms <small>step 2</small></a></li>
          <li><a href="#s-outputs" data-step="3">outputs <small>step 3</small></a></li>
          <li><a href="#s-graph" data-step="4">the graph <small>zoom out</small></a></li>
        </ol>

        <div class="tour-texts">
          <div class="tour-text active" id="s-pipeline">
            <h2>a pipeline is three lists</h2>
            <p>A pipeline is <code>inputs → transforms → outputs</code>, and all three are arrays. kayak merges the inputs into one stream. The transforms run in the order of the config. Each output gets every batch.</p>
            <p class="dim">The card on the right shows one pipeline. The web UI draws it from the same config: the config in three tabs, a throughput chart, and the batches that went through.</p>
          </div>
          <div class="tour-text" id="s-inputs">
            <h2>inputs: where the messages come from</h2>
            <p>An input reads a subject, a topic, a channel, a table or a set of OPC UA nodes. The <code>http</code> input receives posts. Each input gives the pipeline batches of plain JSON. You do not declare a schema.</p>
            <p class="dim">Every input can <strong>buffer</strong> by count, by time window, or both. Every input can add an <strong>envelope</strong> with its metadata, for example the subject, the partition or the offset. The envelope is ordinary JSON fields.</p>
            <div class="inventory"><code>nats</code><code>kafka</code><code>mqtt</code><code>redis</code><code>opcua</code><code>http</code><code>http_poll</code><code>postgres</code><code>clickhouse</code><code>indu</code><code>pipeline</code><code>dummy</code></div>
          </div>
          <div class="tour-text" id="s-transforms">
            <h2>transforms: what happens between</h2>
            <p>A transform is a small step that does one thing. <code>filter</code> keeps or drops messages. <code>map</code> copies, casts and calculates fields. <code>reducer</code> aggregates with <code>group_by</code>. The streaming transforms keep state per key. Use <code>script</code> when configuration is not sufficient.</p>
            <p class="dim">Every transform addresses fields by path, for example <code>sensor.id</code> or <code>_meta.subject</code>. Each transform has a policy for a missing field. kayak does not guess.</p>
            <div class="inventory"><code>filter</code><code>map</code><code>reducer</code><code>splitter</code><code>buffer</code><code>remember</code><code>recall</code><code>script</code><code>http</code><code>deadband</code><code>throttle</code><code>pivot</code><code>derive</code><code>rolling</code><code>smooth</code><code>detect</code><code>resample</code><code>features</code></div>
          </div>
          <div class="tour-text" id="s-outputs">
            <h2>outputs: where the messages go</h2>
            <p>Each output gets every batch. To archive to postgres and also send to kafka, use one pipeline with two outputs. The database outputs map fields to typed columns. The file and s3 outputs rotate parts by rows or by time.</p>
            <p class="dim">kayak retries an output that it cannot reach at startup. If postgres starts 30 seconds after kayak, the pipeline waits and then continues.</p>
            <div class="inventory"><code>postgres</code><code>clickhouse</code><code>s3</code><code>file</code><code>kafka</code><code>nats</code><code>mqtt</code><code>redis</code><code>http</code><code>indu</code><code>tidepool</code><code>stdout</code></div>
          </div>
          <div class="tour-text" id="s-graph">
            <h2>pipelines feed pipelines</h2>
            <p>The <code>pipeline</code> input reads the output of another pipeline. One pipeline reads the broker one time. Many pipelines downstream use its messages: a rollup, an archive, an alert. Your config is a graph of small pipelines.</p>
            <p class="dim">Each pipeline has its own transforms and outputs. kayak shares each message between them and does not copy it. In the web UI, a line lights up when a batch goes along it.</p>
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

<!-- ========================================================= performance -->
<section class="section-page" id="performance">
  <div class="inner">
    <div class="section-head-row">
      <div>
        <span class="label">performance</span>
        <h2>the runtime is not the bottleneck</h2>
      </div>
      <p class="lede">kayak has no garbage collector. The <code>just bench</code> harness measures the cost of the run loop. These numbers come from an Apple M1 Max, in process, with no network and no disk. They exclude I/O, so they are not end-to-end throughput.</p>
    </div>

    <div class="stats">
      <div class="stat"><span class="num">7M</span><span class="unit">passes/s</span><p>one pipeline, no transforms</p></div>
      <div class="stat"><span class="num">31M</span><span class="unit">messages/s</span><p>one pipeline, batches of 100, one <code>filter</code></p></div>
      <div class="stat"><span class="num">5.6B</span><span class="unit">messages/s</span><p>1000 pipelines at the same time, batches of 100, 14 MiB resident</p></div>
      <div class="stat"><span class="num">9</span><span class="unit">MiB</span><p>resident memory of one pipeline</p></div>
    </div>

    <ul class="points">
      <li><strong>Fan-out does not copy.</strong> Each output and each downstream pipeline gets the same shared batch (<code>Arc</code>).</li>
      <li><strong>Batches share the cost.</strong> Set <code>max_batch</code> on a broker input, or an input <code>buffer</code>. kayak then does the work per batch one time for many messages.</li>
      <li><strong>Databases get batches.</strong> The <code>clickhouse</code> output writes one insert per batch.</li>
      <li><strong>No browser, no UI cost.</strong> A server with no web UI attached does no work for the UI.</li>
    </ul>
  </div>
</section>

<!-- ======================================================= composability -->
<section class="section-page" id="composability">
  <div class="inner">
    <div class="section-head-row">
      <div>
        <span class="label">composability</span>
        <h2>small parts that connect</h2>
      </div>
      <p class="lede">Each transform does one thing. You connect transforms in a chain, and you connect pipelines in a graph. A message is plain JSON from the input to the output. There is no schema to declare.</p>
    </div>

    <div class="compose-grid">
      <svg class="graph" viewBox="0 0 520 300" role="img" aria-label="one pipeline that reads nats and feeds three downstream pipelines">
        <g transform="translate(180 20)">
          <rect class="node" width="160" height="56" rx="3"/>
          <text x="10" y="24">sensors</text>
          <text class="small" x="10" y="42">NATS · sensors.&gt;</text>
        </g>
        <g>
          <path class="edge" d="M240 76 V130 Q240 136 234 136 H86 Q80 136 80 142 V200"/>
          <path class="edge" d="M260 76 V200"/>
          <path class="edge" d="M280 76 V130 Q280 136 286 136 H434 Q440 136 440 142 V200"/>
        </g>
        <g>
          <path class="pulse" d="M240 76 V130 Q240 136 234 136 H86 Q80 136 80 142 V200"/>
          <path class="pulse" d="M260 76 V200"/>
          <path class="pulse" d="M280 76 V130 Q280 136 286 136 H434 Q440 136 440 142 V200"/>
        </g>
        <g transform="translate(0 200)">
          <rect class="node" width="160" height="56" rx="3"/>
          <text x="10" y="24">sensors_10s_avg</text>
          <text class="small" x="10" y="42">BUFFER · REDUCER → CLICKHOUSE</text>
        </g>
        <g transform="translate(180 200)">
          <rect class="node" width="160" height="56" rx="3"/>
          <text x="10" y="24">sensors_archive</text>
          <text class="small" x="10" y="42">S3 · NDJSON</text>
        </g>
        <g transform="translate(360 200)">
          <rect class="node" width="160" height="56" rx="3"/>
          <text x="10" y="24">hot_alerts</text>
          <text class="small" x="10" y="42">FILTER → HTTP</text>
        </g>
      </svg>

      <ul class="points">
        <li><strong>Many inputs, many outputs.</strong> kayak merges the inputs of a pipeline into one stream. Each output gets each batch.</li>
        <li><strong>Pipelines feed pipelines.</strong> The <code>pipeline</code> input reads the output of another pipeline. You can make fan-out, fan-in and chains of any depth.</li>
        <li><strong>Connections.</strong> You declare a broker, a database or a bucket one time, with a name. Many components refer to that name.</li>
        <li><strong>State buckets.</strong> <code>remember</code> writes a value to a named bucket. <code>recall</code> reads it, also in a different pipeline.</li>
        <li><strong>Metadata is data.</strong> An input can add the subject, topic or offset to the message as ordinary JSON fields. Each transform can then use them.</li>
        <li><strong>Scripts when you need them.</strong> Use <code>script</code> (rhai) when the other transforms are not sufficient.</li>
      </ul>
    </div>

    <div class="two-col" style="margin-top:32px">
      <div class="code-card">
        <header>a pipeline fed by a pipeline <span class="file">config.yaml</span></header>
<pre>- <span class="k">id</span>: <span class="s">sensors_10s_avg</span>
  <span class="k">inputs</span>:
    - <span class="k">type</span>: <span class="s">pipeline</span>
      <span class="k">upstream</span>: <span class="s">sensors</span>
      <span class="k">buffer</span>: { <span class="k">type</span>: <span class="s">tumbling</span>, <span class="k">window_seconds</span>: <span class="n">10</span> }
  <span class="k">transforms</span>:
    - <span class="k">type</span>: <span class="s">reducer</span>
      <span class="k">group_by</span>: [<span class="s">sensor</span>, <span class="s">_meta.subject</span>]
      <span class="k">aggregations</span>:
        - { <span class="k">function</span>: <span class="s">avg</span>,   <span class="k">field</span>: <span class="s">value</span>, <span class="k">as</span>: <span class="s">mean</span> }
        - { <span class="k">function</span>: <span class="s">max</span>,   <span class="k">field</span>: <span class="s">value</span>, <span class="k">as</span>: <span class="s">highest</span> }
        - { <span class="k">function</span>: <span class="s">count</span>, <span class="k">as</span>: <span class="s">readings</span> }
  <span class="k">outputs</span>:
    - <span class="k">type</span>: <span class="s">clickhouse</span>
      <span class="k">connection</span>: <span class="s">analytics</span>
      <span class="k">table</span>: <span class="s">sensor_rollups</span>
      <span class="k">order_by</span>: [<span class="s">sensor</span>]
      <span class="k">columns</span>:
        - { <span class="k">name</span>: <span class="s">sensor</span>,   <span class="k">type</span>: <span class="s">text</span> }
        - { <span class="k">name</span>: <span class="s">mean</span>,     <span class="k">type</span>: <span class="s">float</span> }
        - { <span class="k">name</span>: <span class="s">highest</span>,  <span class="k">type</span>: <span class="s">float</span> }
        - { <span class="k">name</span>: <span class="s">readings</span>, <span class="k">type</span>: <span class="s">bigint</span> }</pre>
        <div class="note">The reducer sends one message for each <code>(sensor, subject)</code> pair every 10 s. kayak checks the column mapping when it builds the pipeline.</div>
      </div>

      <div class="code-card">
        <header>declare a system one time <span class="file">config.connections.yaml</span></header>
<pre><span class="k">plant-nats</span>:
  <span class="k">type</span>: <span class="s">nats</span>
  <span class="k">urls</span>: <span class="s">nats://nats.internal:4222</span>
<span class="k">analytics</span>:
  <span class="k">type</span>: <span class="s">clickhouse</span>
  <span class="k">url</span>: <span class="s">https://ch.internal:8443</span>
  <span class="k">database</span>: <span class="s">plant</span>
  <span class="k">user</span>: <span class="s">kayak</span>
  <span class="k">password</span>: <span class="l">${CLICKHOUSE_PASSWORD}</span>
<span class="k">archive</span>:
  <span class="k">type</span>: <span class="s">s3</span>
  <span class="k">bucket</span>: <span class="s">events</span>
  <span class="k">region</span>: <span class="s">eu-north-1</span>
  <span class="k">access_key_id</span>: <span class="l">${S3_KEY_ID}</span>
  <span class="k">secret_access_key</span>: <span class="l">${S3_SECRET}</span></pre>
        <div class="note">A connection holds what the system is: hosts, URLs and credentials. A component adds only what it wants from the system, for example a subject or a table. kayak reads <code>${NAME}</code> from the environment or from a secrets file.</div>
      </div>
    </div>
  </div>
</section>

<!-- ========================================================== inventory -->
<section class="section-page" id="components">
  <div class="inner">
    <div class="section-head-row">
      <div>
        <span class="label">feature completeness</span>
        <h2>the components</h2>
      </div>
      <p class="lede">The reference documents each component and each field. kayak generates the reference from the config types, so it agrees with the server that you run.</p>
    </div>

    <div class="props inventory-table">
      <div class="rows">
        <div class="row"><span class="name">inputs</span><span class="desc inventory"><code>nats</code><code>kafka</code><code>mqtt</code><code>redis</code><code>opcua</code><code>http</code><code>http_poll</code><code>postgres</code><code>clickhouse</code><code>indu</code><code>pipeline</code><code>dummy</code></span></div>
        <div class="row"><span class="name">transforms</span><span class="desc inventory"><code>filter</code><code>map</code><code>reducer</code><code>splitter</code><code>buffer</code><code>remember</code><code>recall</code><code>http</code><code>script</code><code>deadband</code><code>throttle</code><code>pivot</code><code>derive</code><code>rolling</code><code>smooth</code><code>detect</code><code>resample</code><code>features</code></span></div>
        <div class="row"><span class="name">outputs</span><span class="desc inventory"><code>postgres</code><code>clickhouse</code><code>s3</code><code>file</code><code>kafka</code><code>nats</code><code>mqtt</code><code>redis</code><code>http</code><code>indu</code><code>tidepool</code><code>stdout</code></span></div>
        <div class="row"><span class="name">connections</span><span class="desc inventory"><code>kafka</code><code>nats</code><code>mqtt</code><code>redis</code><code>postgres</code><code>clickhouse</code><code>s3</code><code>file</code><code>opcua</code><code>indu</code><code>tidepool</code></span></div>
      </div>
    </div>

    <div class="facts" style="margin-top:24px">
      <div><span class="label">buffers</span><p>Each input can buffer by count, by time window, or by the first of the two.</p></div>
      <div><span class="label">acknowledgement</span><p>With <code>ack: on_delivery</code>, a <code>kafka</code> or <code>mqtt</code> input acknowledges a message after the outputs return.</p></div>
      <div><span class="label">column mapping</span><p>The <code>postgres</code> and <code>clickhouse</code> outputs map fields to typed columns and create the table.</p></div>
      <div><span class="label">rotation</span><p>The <code>file</code> and <code>s3</code> outputs rotate parts by row count or by time.</p></div>
      <div><span class="label">streaming statistics</span><p>Rolling windows, smoothing, rates, resampling and anomaly detection, per key.</p></div>
      <div><span class="label">model calls</span><p>The <code>http</code> transform sends messages to a service. It can merge the reply into each message.</p></div>
      <div><span class="label">industrial</span><p>The <code>opcua</code> input subscribes to nodes and sends one message for each value change.</p></div>
      <div><span class="label">ingest endpoint</span><p>The <code>http</code> input gives a pipeline its own endpoint, with an optional credential.</p></div>
    </div>
  </div>
</section>

<!-- ========================================================== operation -->
<section class="section-page" id="operation">
  <div class="inner">
    <div class="section-head-row">
      <div>
        <span class="label">operation</span>
        <h2>run it like any other service</h2>
      </div>
      <p class="lede">The container image contains the binary and nothing else. You mount your config and name it on the command line. The arguments of the container are the flags of the server.</p>
    </div>

    <div class="deploy-grid">
      <div class="stack">
        <div class="code-card term">
          <header>run it</header>
<pre><span class="prompt">$ </span>docker run -p 6767:6767 -v "$PWD:/kayak" \
    -e CLICKHOUSE_PASSWORD \
    ghcr.io/niclasgrahm/kayak \
      --config /kayak/config.yaml \
      --server-config /kayak/server.yaml</pre>
          <div class="note">kayak finds <code>config.connections.yaml</code> beside the config, so mount the directory. The image is for <code>linux/amd64</code> and <code>linux/arm64</code> and runs as uid 10001. In Kubernetes, put the config in a ConfigMap and the secrets in environment variables.</div>
        </div>

        <div class="code-card">
          <header>the settings of the server <span class="file">server.yaml</span></header>
<pre><span class="k">auth</span>:
  <span class="k">type</span>: <span class="s">basic</span>
  <span class="k">users</span>:
    <span class="k">ops</span>:    { <span class="k">password</span>: <span class="l">${OPS_PASSWORD}</span>,    <span class="k">role</span>: <span class="s">admin</span> }
    <span class="k">viewer</span>: { <span class="k">password</span>: <span class="l">${VIEWER_PASSWORD}</span>, <span class="k">role</span>: <span class="s">read</span> }
<span class="k">history</span>:
  <span class="k">retention_secs</span>: <span class="n">86400</span>     <span class="c"># 0 turns history off</span></pre>
          <div class="note">Authentication has two roles. <code>admin</code> can change the graph. <code>read</code> can only look. Accounts come from this file (<code>basic</code>) or from an identity provider (<code>jwt</code>).</div>
        </div>
      </div>

      <div class="stack">
        <div class="props">
          <header><span class="label">flags</span> <span class="dim" style="font-weight:400;font-size:11px;font-family:var(--font-mono)">kayak --help</span></header>
          <div class="rows">
            <div class="row"><span class="name">--config &lt;path&gt;</span><span class="desc">The pipelines, as JSON or YAML. The file extension sets the format.</span></div>
            <div class="row"><span class="name">--connections &lt;path&gt;</span><span class="desc">The connections file. Without the flag, kayak uses <code>&lt;config&gt;.connections.&lt;ext&gt;</code> beside the config.</span></div>
            <div class="row"><span class="name">--secrets &lt;path&gt;</span><span class="desc">A JSON file of values for <code>${NAME}</code>. kayak reads the environment first. kayak does not start if a name has no value.</span></div>
            <div class="row"><span class="name">--data-dir &lt;path&gt;</span><span class="desc">The directory for <code>file</code> outputs. Without it, <code>file</code> outputs do not build.</span></div>
            <div class="row"><span class="name">--server-config &lt;path&gt;</span><span class="desc">Authentication and history. Without it, the server authenticates nobody.</span></div>
            <div class="row"><span class="name">--listen &lt;addr&gt;</span><span class="desc">The bind address. The default is <code>127.0.0.1:6767</code>. The image binds <code>0.0.0.0:6767</code>.</span></div>
            <div class="row"><span class="name">--debug</span><span class="desc">More log output.</span></div>
          </div>
        </div>

        <div class="code-card term">
          <header>the http api</header>
<pre><span class="c"># send messages to a pipeline with an http input</span>
<span class="prompt">$ </span>curl -X POST localhost:6767/api/pipelines/ingest/messages \
    -d '[{"sensor":"line1/temp","value":21.5}]'
<span class="out">HTTP/1.1 202 Accepted</span>

<span class="c"># counters and failure records of the last day</span>
<span class="prompt">$ </span>curl localhost:6767/api/pipelines/ingest/history?resolution=coarse

<span class="c"># the complete api, as OpenAPI 3.1</span>
<span class="prompt">$ </span>curl localhost:6767/api/openapi.json</pre>
          <div class="note">kayak builds its routes and the OpenAPI document from one table. <code>/api/docs</code> gives the component reference as JSON.</div>
        </div>
      </div>
    </div>
  </div>
</section>

<!-- ============================================================= limits -->
<section class="section-page" id="limits">
  <div class="inner">
    <div class="section-head-row">
      <div>
        <span class="label">limits</span>
        <h2>what kayak does not do</h2>
      </div>
      <p class="lede">Read these limits before you choose kayak for a job.</p>
    </div>
    <div class="facts">
      <div><span class="label">one process</span><p>kayak does not cluster. It has no distributed state and no exactly-once delivery across machines.</p></div>
      <div><span class="label">state in memory</span><p>State buckets and history are in memory. A restart clears them.</p></div>
      <div><span class="label">no table migrations</span><p>The <code>postgres</code> and <code>clickhouse</code> outputs create a table if it does not exist. They do not change an existing table.</p></div>
      <div><span class="label">pre-1.0</span><p>The config format can change between minor versions. The roadmap is in the repository.</p></div>
    </div>
  </div>
</section>

<!-- ============================================================= web ui -->
<section class="section-page" id="web-ui">
  <div class="inner narrow">
    <span class="label">optional</span>
    <h3>web ui</h3>
    <p class="dim">The server also has a web UI. Use it to look at the running graph and the messages in each pipeline. You do not need it to run kayak. <a href="https://propell.dev/kayak/canvas/the-canvas">Read about the web UI.</a></p>
  </div>
</section>

<footer class="footer grid-bg">
  <div class="inner">
    <div>
      <span class="label">try it</span>
      <pre class="install" style="margin-top:10px"><span class="p">$ </span>docker run --rm -p 6767:6767 --entrypoint sh ghcr.io/niclasgrahm/kayak -c \
    'echo "[{id: ticker, inputs: [{type: dummy, duration: 1}], outputs: [{type: stdout}]}]" &gt; c.yaml &amp;&amp; exec kayak --config c.yaml'</pre>
    </div>
    <div class="links">
      <a href="https://github.com/niclasgrahm/kayak">github</a>
      <a href="https://propell.dev/kayak/getting-started">getting started</a>
      <a href="https://propell.dev/kayak/reference/">reference</a>
      <a href="https://propell.dev/kayak/operating/deployment">deployment</a>
    </div>
    <p class="fine">kayak is built with Rust, Axum, Tokio and Leptos. The name is lowercase.</p>
  </div>
</footer>
</div>
</template>

<style>
/* Every colour is the product's own token (style/main.scss), verbatim. Every
   rule is prefixed `.landing` so nothing here reaches the rest of the site
   and nothing of the site's reaches in — `.label` and `.row` are names
   kayak.css and vitepress both have opinions about. */
.landing {--bg-canvas: #1d2129; --bg-panel: #262b33; --bg-titlebar: #1b1f26; --bg-hover: #2f3540; --border: #14171c; --text: #cdced2; --text-dim: #85878c; --accent: #699ce8; --error: #e06c75; --stat-in: #699ce8; --stat-out: #d8a657; --json-key: #7fbbb3; --json-str: #a7c080; --json-num: #d8a657; --json-literal: #d699b6; --radius: 4px; --grid: 20px; --font-sans: "Noto Sans", "Open Sans", system-ui, -apple-system, sans-serif; --font-mono: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace; --measure: 34rem; --page-x: clamp(20px, 5vw, 72px); }
.landing *, .landing *::before, .landing *::after {box-sizing: border-box; }
.landing {margin: 0; overflow-x: clip; background: var(--bg-canvas); color: var(--text); font-family: var(--font-sans); font-size: 15px; line-height: 1.6; -webkit-font-smoothing: antialiased; }
.landing a {color: var(--accent); text-decoration: none; }
.landing a:hover {text-decoration: underline; text-underline-offset: 2px; }
.landing :focus-visible {outline: 2px solid var(--accent); outline-offset: 2px; }
.landing code, .landing kbd, .landing pre {font-family: var(--font-mono); }
.landing p code, .landing li code, .landing h2 code, .landing h3 code {font-size: 0.88em; background: var(--bg-titlebar); border: 1px solid var(--border); border-radius: 2px; padding: 0 4px; color: var(--text); }
.landing .grid-bg {background-image: linear-gradient(rgba(255,255,255,0.045) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.045) 1px, transparent 1px); background-size: var(--grid) var(--grid); }
.landing .label {font-size: 11px; letter-spacing: 0.07em; text-transform: uppercase; color: var(--text-dim); font-weight: 600; }
.landing h1, .landing h2, .landing h3 {font-weight: 700; letter-spacing: -0.01em; line-height: 1.15; margin: 0; }
.landing h2 {font-size: clamp(26px, 3.4vw, 40px); }
.landing h3 {font-size: 18px; }
.landing .lede {color: var(--text-dim); font-size: 17px; max-width: var(--measure); }
.landing p {margin: 0 0 1em; }
.landing .dim {color: var(--text-dim); }

/* hero */
.landing .hero {position: relative; padding: clamp(64px, 12vh, 140px) var(--page-x) clamp(56px, 10vh, 120px); border-bottom: 1px solid var(--border); overflow: hidden; }
.landing .hero-inner > * {min-width: 0; }
.landing .hero-inner {display: grid; grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr); gap: 48px; align-items: center; max-width: 1240px; margin: 0 auto; }
.landing .hero h1 {font-size: clamp(40px, 6.2vw, 78px); font-weight: 800; letter-spacing: -0.035em; line-height: 1.02; max-width: 12ch; }
.landing .hero h1 .thin {font-weight: 400; color: var(--text-dim); }
.landing .hero .lede {margin: 26px 0 30px; font-size: clamp(16px, 1.5vw, 19px); }
.landing .install {display: inline-block; background: var(--bg-titlebar); border: 1px solid var(--border); border-radius: var(--radius); padding: 12px 16px; font-family: var(--font-mono); font-size: 12.5px; line-height: 1.75; white-space: pre; color: var(--text); max-width: 100%; overflow-x: auto; }
.landing .install .c {color: var(--text-dim); }
.landing .install .p {color: var(--text-dim); user-select: none; }
.landing .hero-links {margin-top: 18px; font-size: 14px; display: flex; gap: 18px; flex-wrap: wrap; }
.landing .hero-fine {margin-top: 12px; font-size: 13px; color: var(--text-dim); }

/* sections */
.landing .section-page {padding: clamp(64px, 10vh, 120px) var(--page-x); border-top: 1px solid var(--border); }
.landing .section-page > .inner {max-width: 1240px; margin: 0 auto; }
.landing .section-page > .inner.narrow {max-width: 1240px; }
.landing .section-page > .inner.narrow p {max-width: var(--measure); margin-top: 10px; }
.landing #web-ui {padding-block: 40px; }
.landing #web-ui .label {display: block; margin-bottom: 8px; }
.landing .section-head-row {display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.3fr); gap: 32px 64px; align-items: end; margin-bottom: 40px; }
.landing .section-head-row .label {margin-bottom: 12px; display: block; }
.landing .section-head-row .lede {margin: 0; }
.landing .two-col {display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 24px; }
.landing .stack {display: flex; flex-direction: column; gap: 24px; }

/* the performance numbers */
.landing .stats {display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 1px; background: var(--border); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
.landing .stat {background: var(--bg-panel); padding: 18px 18px 14px; }
.landing .stat .num {font-family: var(--font-mono); font-size: clamp(32px, 4vw, 48px); font-weight: 600; color: var(--stat-in); letter-spacing: -0.02em; line-height: 1; }
.landing .stat .unit {font-family: var(--font-mono); font-size: 13px; color: var(--stat-out); margin-left: 8px; }
.landing .stat p {margin: 10px 0 0; font-size: 13px; color: var(--text-dim); }
.landing .points {list-style: none; margin: 32px 0 0; padding: 0; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px 32px; }
.landing .points li {font-size: 14.5px; color: var(--text-dim); padding-left: 14px; border-left: 2px solid var(--border); }
.landing .points li strong {color: var(--text); font-weight: 600; }

/* composability */
.landing .compose-grid {display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.1fr); gap: 48px; align-items: center; }
.landing .compose-grid .points {margin: 0; grid-template-columns: 1fr; }
.landing .graph {width: 100%; height: auto; display: block; }
.landing .graph .node {fill: var(--bg-panel); stroke: var(--border); }
.landing .graph text {font-family: var(--font-mono); font-size: 12px; fill: var(--text); }
.landing .graph text.small {font-size: 8.5px; fill: var(--text-dim); letter-spacing: 0.06em; }
.landing .graph .edge {fill: none; stroke: var(--text-dim); stroke-width: 2; }
.landing .graph .pulse {fill: none; stroke: var(--accent); stroke-width: 3; stroke-linecap: round; opacity: 0; animation: edge-pulse 2.6s ease-out infinite; }
.landing .graph .pulse:nth-child(2) {animation-delay: 0.8s; }
.landing .graph .pulse:nth-child(3) {animation-delay: 1.6s; }
@keyframes edge-pulse {
  0% {opacity: 0; }
  6% {opacity: 1; }
  100% {opacity: 0; } }
@media (prefers-reduced-motion: reduce) {
  .landing .graph .pulse {animation: none; opacity: 0; } }

/* code */
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
.landing .term pre .out {color: var(--text-dim); }
.landing .term pre .prompt {color: var(--text-dim); user-select: none; }

/* tables of names */
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
.landing .inventory-table .row {grid-template-columns: 140px minmax(0, 1fr); padding: 8px 4px; }
.landing .inventory {display: flex; flex-wrap: wrap; gap: 4px; }
.landing .inventory code {font-size: 12px; padding: 1px 6px; background: var(--bg-canvas); border: 1px solid var(--border); border-radius: 2px; color: var(--text); }
.landing .facts {display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 1px; background: var(--border); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
.landing .facts div {background: var(--bg-panel); padding: 14px 16px; }
.landing .facts .label {display: block; margin-bottom: 6px; }
.landing .facts p {margin: 0; font-size: 13px; color: var(--text); }
.landing .facts p code {font-size: 12px; }

/* footer */
.landing .footer {border-top: 1px solid var(--border); padding: 48px var(--page-x) 40px; }
.landing .footer .inner {max-width: 1240px; margin: 0 auto; display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 24px 48px; align-items: end; }
.landing .footer .links {display: flex; gap: 18px; flex-wrap: wrap; font-size: 13px; }
.landing .footer .fine {font-size: 12px; color: var(--text-dim); margin-top: 24px; grid-column: 1 / -1; }

@media (max-width: 1080px) {
  .landing .hero-inner {grid-template-columns: 1fr; }
  .landing .install {display: block; font-size: 11.5px; }
  .landing .two-col, .landing .deploy-grid, .landing .compose-grid, .landing .section-head-row {grid-template-columns: 1fr; }
  .landing .graph {max-width: 640px; }
  .landing .stats, .landing .facts {grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .landing .footer .inner {grid-template-columns: 1fr; } }
@media (max-width: 560px) {
  .landing .stats, .landing .facts, .landing .points {grid-template-columns: 1fr; }
  .landing .props .row, .landing .inventory-table .row {grid-template-columns: 1fr; gap: 4px; }
  .landing .inventory code {font-size: 11px; } }

/* vitepress' base styles reach into the page; these put back what the design assumes */
.landing h1, .landing h2, .landing h3 { border: none; padding: 0; margin: 0; letter-spacing: -0.01em; }
.landing h2 { font-size: clamp(26px, 3.4vw, 40px); }
.landing h3 { font-size: 18px; }
.landing p { margin: 0 0 1em; line-height: 1.6; }
.landing pre, .landing code { font-family: var(--font-mono); }
.landing pre { background: none; }
.landing ol, .landing ul { padding: 0; margin: 0; }
.landing .points { margin-top: 32px; }
.landing .compose-grid .points { margin-top: 0; }
.landing a { text-decoration: none; }
.landing a:hover { text-decoration: underline; }

/* the scroll-driven tour, and the card replica it shows */
@keyframes edge-pulse {
  0% {opacity: 0; }
  6% {opacity: 1; }
  100% {opacity: 0; } }
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
.landing .section-head-row {display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.3fr); gap: 32px 64px; align-items: end; margin-bottom: 40px; }
.landing .section-head-row .label {margin-bottom: 12px; display: block; }
.landing .section-head-row .lede {margin: 0; }
@media (max-width: 1080px) {
  .landing .tour-stage {top: 0; height: 100vh; height: 100svh; }
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
  .landing .two-col, .landing .deploy-grid, .landing .api-grid, .landing .section-head-row {grid-template-columns: 1fr; } }
</style>
