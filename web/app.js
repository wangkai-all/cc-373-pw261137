/* skewpipe dashboard — plain ES2020, no build step, no dependencies. */
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
const SVG_NS = "http://www.w3.org/2000/svg";

const state = {
  runId: null,
  snapshot: null,
  results: null,
  base: null,
  contribKey: "HOT",
  playing: false,
  playTimer: null,
  meta: null,
  metric: "count",
  mode: "naive",
  tablesOpen: {},
};

/* ------------------------------------------------------------------ utils */

function el(name, attrs = {}, children = []) {
  const node = document.createElement(name);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const child of [].concat(children)) {
    if (child == null) continue;
    node.append(typeof child === "string" || typeof child === "number"
      ? document.createTextNode(String(child)) : child);
  }
  return node;
}

function svgEl(name, attrs = {}) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [k, v] of Object.entries(attrs)) {
    if (v != null) node.setAttribute(k, v);
  }
  return node;
}

const fmtInt = new Intl.NumberFormat("zh-CN");
const num = (v) => (v == null || Number.isNaN(v) ? "—" : fmtInt.format(Math.round(v)));
const num1 = (v) => (v == null || Number.isNaN(v) ? "—" : (Math.round(v * 10) / 10).toFixed(1));
const num4 = (v) => (v == null || Number.isNaN(v) ? "—" : String(Math.round(v * 10000) / 10000));
const pct = (v) => (v == null ? "—" : (v * 100).toFixed(v < 0.1 ? 1 : 0) + "%");
const compact = (v) => {
  if (v == null) return "—";
  const a = Math.abs(v);
  if (a >= 1e6) return (v / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
  if (a >= 1e5) return (v / 1e3).toFixed(0) + "K";
  if (a >= 1e4) return (v / 1e3).toFixed(1).replace(/\.0$/, "") + "K";
  return fmtInt.format(Math.round(v));
};
const bytes = (v) => {
  if (v == null) return "—";
  if (v >= 1024 * 1024) return (v / 1024 / 1024).toFixed(1) + " MB";
  if (v >= 1024) return (v / 1024).toFixed(1) + " KB";
  return v + " B";
};
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

/* --------------------------------------------------------------- tooltip */

const tooltip = $("#tooltip");
function showTip(evt, title, rows) {
  tooltip.replaceChildren();
  tooltip.append(el("div", { class: "t-title", text: title }));
  for (const [label, value] of rows) {
    tooltip.append(el("div", { class: "t-row" }, [el("span", { text: label }), el("b", { text: value })]));
  }
  tooltip.classList.add("show");
  moveTip(evt);
}
function moveTip(evt) {
  const pad = 14;
  const w = tooltip.offsetWidth, h = tooltip.offsetHeight;
  let x = evt.clientX + pad, y = evt.clientY + pad;
  if (x + w > window.innerWidth - 8) x = evt.clientX - w - pad;
  if (y + h > window.innerHeight - 8) y = evt.clientY - h - pad;
  tooltip.style.left = Math.max(8, x) + "px";
  tooltip.style.top = Math.max(8, y) + "px";
}
function hideTip() { tooltip.classList.remove("show"); }

function hoverable(node, titleFn, rowsFn) {
  node.setAttribute("tabindex", "0");
  node.addEventListener("pointerenter", (e) => showTip(e, titleFn(), rowsFn()));
  node.addEventListener("pointermove", moveTip);
  node.addEventListener("pointerleave", hideTip);
  node.addEventListener("focus", (e) => {
    const r = node.getBoundingClientRect();
    showTip({ clientX: r.left + r.width / 2, clientY: r.top }, titleFn(), rowsFn());
  });
  node.addEventListener("blur", hideTip);
  return node;
}

/* ------------------------------------------------------------------- api */

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (_) { data = { error: text }; }
  if (!res.ok) throw new Error((data && data.error) || res.statusText);
  return data;
}

/* ----------------------------------------------------------------- theme */

function applyTheme(theme) {
  if (theme) document.documentElement.setAttribute("data-theme", theme);
  else document.documentElement.removeAttribute("data-theme");
  state.tablesOpen = state.tablesOpen || {};
  renderAll();
}
(function initTheme() {
  const saved = localStorage.getItem("skewpipe-theme");
  if (saved) document.documentElement.setAttribute("data-theme", saved);
})();

/* ------------------------------------------------------------- controls */

function buildMetricTabs() {
  const wrap = $("#metric-tabs");
  const metrics = (state.meta && state.meta.metrics) || [];
  wrap.replaceChildren();
  for (const m of metrics) {
    const btn = el("button", {
      role: "tab",
      "aria-selected": String(m.id === state.metric),
      text: m.label.split(" ")[0],
      title: m.label,
    });
    btn.addEventListener("click", () => {
      state.metric = m.id;
      $$("button", wrap).forEach((b) => b.setAttribute("aria-selected", String(b === btn)));
      startRun();
    });
    wrap.append(btn);
  }
}

function syncModeNote() {
  const notes = {
    naive: "朴素哈希分区：整条热点键被哈希到同一个聚合进程。计数/求和结果正确，但该进程长期卡住，积压持续增长。",
    preroute: "入口预路由：分发器在接收时就改写键名，热点从一开始就分散。结果正确，但无法处理已经聚合过一半的键——没有旧证据可搬运。",
    split: "运行时拆分重键：运行中检测到倾斜后创建拆分任务，把已聚合证据按指标规则搬运进新分片，再把未处理的行重新分配；分片名带拆分契约束，旧任务/旧代结果会被发布器拒绝。",
  };
  $("#mode-note").textContent = notes[state.mode] || "";
  $("#strategy-group").classList.toggle("hidden", state.mode === "naive");
}

async function startRun() {
  const settings = {
    mode: $("#mode").value,
    metric: state.metric,
    strategy: $("#strategy").value,
    fanout: Number($("#fanout").value),
    num_aggregators: Number($("#aggs").value),
    seed: Number($("#seed").value) || 7,
  };
  state.mode = settings.mode;
  stopPlay();
  syncModeNote();
  try {
    const created = await api("/api/runs", {
      method: "POST",
      body: JSON.stringify({ settings, autoplay: false }),
    });
    state.runId = created.id;
    state.snapshot = created.snapshot;
    state.results = null;
    state.base = null;
    state.contribKey = (created.snapshot.skew[0] && created.snapshot.skew[0].key) || "HOT";
    populateSplitKeys();
    await refresh({ skipResults: true });
  } catch (err) {
    flash(`创建运行失败：${err.message}`);
  }
}

async function ensureRun() {
  if (!state.runId) await startRun();
  return state.runId;
}

async function stepRun(n = 1) {
  const id = await ensureRun();
  state.snapshot = await api(`/api/runs/${id}/step`, { method: "POST", body: JSON.stringify({ n }) });
  await refresh();
}

async function finishRun() {
  const id = await ensureRun();
  stopPlay();
  state.snapshot = await api(`/api/runs/${id}/finish`, { method: "POST", body: "{}" });
  await refresh();
}

async function refresh({ skipResults = false } = {}) {
  if (!state.runId) return;
  const id = state.runId;
  const needsResults = !skipResults && (state.snapshot.finished || state.snapshot.progress.processed > 0);
  const [base, results] = await Promise.all([
    api(`/api/runs/${id}/base?key=${encodeURIComponent(state.contribKey)}`).catch(() => null),
    needsResults ? api(`/api/runs/${id}/results`).catch(() => null) : Promise.resolve(null),
  ]);
  state.base = base;
  if (results) state.results = results;
  renderAll();
  if (state.snapshot.finished && state.playing) stopPlay();
}

/* -------------------------------------------------------- auto playback */

function startPlay() {
  if (state.playing) return;
  state.playing = true;
  $("#btn-play").setAttribute("aria-pressed", "true");
  $("#btn-play").textContent = "暂停播放";
  const loop = async () => {
    if (!state.playing) return;
    try {
      await stepRun(1);
    } catch (err) {
      stopPlay();
      flash(`运行失败：${err.message}`);
      return;
    }
    if (state.snapshot.finished) { stopPlay(); return; }
    state.playTimer = setTimeout(loop, 90);
  };
  loop();
}
function stopPlay() {
  state.playing = false;
  clearTimeout(state.playTimer);
  const btn = $("#btn-play");
  if (btn) { btn.setAttribute("aria-pressed", "false"); btn.textContent = "自动播放"; }
}

/* ------------------------------------------------------------- rendering */

function renderAll() {
  if (!state.snapshot) return;
  renderKpis();
  renderFlow();
  renderSkew();
  renderTasks();
  renderBacklog();
  renderProcesses();
  renderContrib();
  renderResults();
  renderEvents();
  $("#inc-chip").textContent = `inc ${state.snapshot.inc} · tick ${state.snapshot.tick}`;
  $("#foot-status").textContent = state.snapshot.finished
    ? `运行结束 · ${state.snapshot.final_tick || state.snapshot.tick} ticks`
    : (state.snapshot.note || "运行中");
}

function renderKpis() {
  const s = state.snapshot;
  const res = state.results;
  const hot = res && res.rows.find((r) => r.base === state.contribKey);
  const hotRows = res ? res.rows : [];
  const totalRows = hotRows.reduce((a, r) => a + (r.truth != null ? 1 : 0), 0);
  const badRows = hotRows.filter((r) => r.error != null && Math.abs(r.error) > 1e-9).length;
  const finished = s.finished;

  $("#kpi-result").textContent = hot ? num4(hot.value) : "—";
  $("#kpi-result").classList.remove("is-good", "is-bad");
  $("#kpi-result-sub").textContent = hot
    ? `${state.contribKey} · 真值 ${num4(hot.truth)} · ${hot.rows} 行证据`
    : "运行到有局部结果后显示";

  const errValue = hot ? hot.error : null;
  $("#kpi-error").textContent = errValue == null ? "—" : (Math.abs(errValue) < 1e-9 ? "0" : num4(errValue));
  $("#kpi-error").classList.toggle("is-good", errValue != null && Math.abs(errValue) < 1e-9);
  $("#kpi-error").classList.toggle("is-bad", errValue != null && Math.abs(errValue) >= 1e-9);
  $("#kpi-error-sub").textContent = badRows === 0 && hotRows.length
    ? `全部 ${totalRows} 个键与真值一致`
    : (hotRows.length ? `${badRows} 个键存在误差` : "发布器按指标规则合并");

  const ownPeak = Math.max(0, ...s.history.map((h) => h.max_queue || 0));
  const basePeak = s.baseline && s.baseline.peak_queue != null ? s.baseline.peak_queue : null;
  $("#kpi-backlog").textContent = num(ownPeak);
  $("#kpi-backlog-sub").textContent = state.mode === "naive"
    ? `朴素基准即当前运行 · 单进程堆到 ${num(ownPeak)} 行`
    : (basePeak != null ? `朴素基准 ${num(basePeak)} 行 · ${ownPeak <= basePeak ? "已缓解" : "未改善"}` : "与朴素基准对照");

  $("#kpi-ticks").textContent = String(s.final_tick || s.tick);
  const baseTicks = s.baseline && s.baseline.ticks;
  $("#kpi-ticks-sub").textContent = finished
    ? (state.mode !== "naive" && baseTicks ? `已完成 · 朴素基准 ${baseTicks} ticks` : "已完成 · 虚拟 tick")
    : `进度 ${pct(s.progress.processed / Math.max(1, s.progress.total))}`;

  const rej = s.publisher.rejected;
  $("#kpi-rejected").textContent = num(rej);
  const reasons = Object.entries(s.publisher.reject_reasons || {});
  $("#kpi-rejected-sub").textContent = reasons.length
    ? reasons.map(([k, v]) => `${labelReason(k)} ${v}`).join(" · ")
    : "异代 / 旧拆分 / 重复";
}

function labelReason(reason) {
  return ({
    "foreign-incarnation": "异代",
    "stale-code": "旧代码",
    "unknown-split": "未知拆分",
    "retired-split": "旧拆分任务",
    "lane-mismatch": "分片不匹配",
    "duplicate-publish": "重复",
  })[reason] || reason;
}

function renderFlow() {
  const s = state.snapshot;
  const flow = $("#flow");
  flow.replaceChildren();
  const splitKeys = Object.keys(s.splits.info || {}).filter((k) => !s.splits.info[k].retired);
  const hotSplit = s.splits.info[state.contribKey];

  const dist = el("div", { class: "flow-box distributor" }, [
    el("div", { class: "fb-name", text: "distributor" }),
    el("div", { class: "fb-metric", text: num(s.progress.admitted) }),
    el("div", { class: "fb-sub", text: `已分发 · in-flight ${num(s.progress.inflight)}` }),
  ]);
  if (splitKeys.length) dist.classList.add("is-split");

  const aggBoxes = s.processes.map((p) => {
    const loaded = p.top && p.top[0] ? `${p.top[0].identity} ${compact(p.top[0].value)}` : "空闲";
    const box = el("div", { class: "flow-box" }, [
      el("div", { class: "fb-name", text: p.name }),
      el("div", { class: "fb-metric", text: compact(p.traffic) }),
      el("div", { class: "fb-sub", text: `累计接收 · 排队 ${num(p.queue)}` }),
      el("div", { class: "fb-hot", text: loaded }),
    ]);
    if (p.suspended) box.style.borderColor = `color-mix(in srgb, ${css("--critical")} 55%, var(--line))`;
    hoverable(box,
      () => `${p.name} · inc ${p.inc}`,
      () => [
        ["已接收行", num(p.rows_in)],
        ["已聚合行", num(p.rows_processed)],
        ["当前排队", num(p.queue)],
        ["状态条目", num(p.state_entries)],
        ["状态大小", bytes(p.state_bytes)],
        ["重启次数", String(p.restarts)],
      ]);
    return box;
  });

  const pub = el("div", { class: "flow-box publisher" }, [
    el("div", { class: "fb-name", text: "publisher" }),
    el("div", { class: "fb-metric", text: num(s.publisher.accepted) }),
    el("div", { class: "fb-sub", text: `已接受 · 拒绝 ${num(s.publisher.rejected)}` }),
  ]);
  hoverable(pub,
    () => "result publisher",
    () => [
      ["已接受局部结果", num(s.publisher.accepted)],
      ["拒绝", num(s.publisher.rejected)],
      ["合并状态条目", num(s.publisher.state_entries)],
      ["状态大小", bytes(s.publisher.state_bytes)],
      ["排队", num(s.publisher.queue)],
    ]);

  const activeSplit = hotSplit && !hotSplit.retired ? hotSplit : null;
  $("#flow-caption").textContent = activeSplit
    ? `聚合进程按「各自处理的键」展示；拆分后 ${activeSplit.base_key} 的 ${activeSplit.fanout} 个分片分别落到不同进程，单点不再被一个热键占满。`
    : splitKeys.length
      ? `已拆分 ${splitKeys.length} 个键；分片按 lane 显式放置，避免重键后又哈希回同一个热点进程。`
      : `朴素分区下 ${s.settings.hot_key} 整键落在同一进程——下方积压会持续增长，直到该键处理完。`;
  const stack = el("div", { class: "agg-stack" }, aggBoxes);
  stack.style.setProperty("--agg-cols", String(Math.min(4, s.processes.length)));
  flow.append(
    el("div", { class: "flow-row" }, [dist]),
    el("div", { class: "flow-arrow", text: "↓ 按 identity 分区" }),
    stack,
    el("div", { class: "flow-arrow", text: "↓ 局部证据" }),
    el("div", { class: "flow-row" }, [pub]),
  );

  const lanesHost = $("#lane-strip");
  if (lanesHost) {
    lanesHost.replaceChildren();
    const base = state.base;
    const lanes = base && base.lanes ? base.lanes : [];
    if (lanes.length) {
      lanesHost.append(el("div", { class: "lane-strip-title", text: `分片归属 · ${base.base}` }));
      const grid = el("div", { class: "lane-strip" });
      for (const lane of lanes) {
        const owners = lane.local && lane.local.length
          ? lane.local.map((l) => l.process).join(" + ")
          : "尚未聚合";
        grid.append(el("div", { class: "lane" }, [
          el("span", { class: "mono", text: lane.identity }),
          el("span", { class: "lane-owner", text: `→ ${owners}` }),
          el("span", { class: "lane-rows", text: lane.published ? `${num(lane.published.rows)} 行` : "—" }),
        ]));
      }
      lanesHost.append(grid);
    }
  }
  const p = s.progress;
  const ratio = p.total ? Math.min(1, p.processed / p.total) : 0;
  $("#progress-fill").style.width = (ratio * 100).toFixed(1) + "%";
  $("#progress-label").textContent = `${num(p.processed)} / ${num(p.total)} 行已聚合` +
    (s.finished ? " · 完成" : "");
  $("#flow-hint").textContent = hotSplit
    ? `${hotSplit.split_id} · ${hotSplit.fanout} 片 · ${hotSplit.strategy}`
    : "分发 → 聚合 → 发布";
}

function renderSkew() {
  const body = $("#skew-table tbody");
  body.replaceChildren();
  const s = state.snapshot;
  if (!s.skew.length) {
    body.append(el("tr", {}, [el("td", { colspan: "5", class: "muted", text: "未检测到超过阈值的键" })]));
    return;
  }
  const splitKeys = s.splits.info || {};
  for (const row of s.skew) {
    const info = splitKeys[row.key];
    const status = info && !info.retired
      ? el("span", { class: "badge warn", text: `已拆分 → ${info.fanout} 片` })
      : el("span", { class: "badge", text: row.share >= 0.15 ? "建议拆分" : "观察" });
    const tr = el("tr", {}, [
      el("td", {}, [el("span", { class: "mono", text: row.key })]),
      el("td", { class: "num", text: num(row.rows) }),
      el("td", { class: "num", text: pct(row.share) }),
      el("td", { class: "num", text: `${num1(row.single_process_ticks)} ticks` }),
      el("td", {}, [status]),
    ]);
    body.append(tr);
  }
}

function populateSplitKeys() {
  const sel = $("#split-key");
  const prev = sel.value;
  const keys = (state.snapshot && state.snapshot.skew) || [];
  sel.replaceChildren();
  for (const row of keys) {
    sel.append(el("option", { value: row.key, text: `${row.key} · ${pct(row.share)}` }));
  }
  if (prev && keys.some((k) => k.key === prev)) sel.value = prev;
}

function renderTasks() {
  const wrap = $("#task-list");
  wrap.replaceChildren();
  const tasks = state.snapshot.splits.tasks || [];
  if (!tasks.length) {
    wrap.append(el("p", { class: "card-hint", text: "尚无拆分任务。朴素分区不会产生任务；运行时拆分会在检测到倾斜或手动触发时创建。" }));
    return;
  }
  for (const t of tasks.slice().reverse()) {
    const retired = t.retired;
    const status = retired
      ? el("span", { class: "badge", text: "已退役" })
      : el("span", { class: "badge good", text: "生效中" });
    wrap.append(el("div", { class: `task ${retired ? "retired" : ""}` }, [
      el("span", { class: "tid", text: t.split_id }),
      el("span", { class: "meta", text: `${t.base_key} → ${t.fanout} 片 · ${t.strategy} · epoch ${t.epoch}` }),
      el("span", { class: "meta", text: `搬运 ${num(t.merged_rows)} 行 · 名单 ${t.lane_digest}` }),
      el("span", { class: "spacer" }),
      status,
    ]));
  }
}

/* ---------------------------------------------------------- SVG helpers */

function svgRoot(host, width, height) {
  host.replaceChildren();
  const svg = svgEl("svg", {
    viewBox: `0 0 ${width} ${height}`,
    width: "100%",
    height: String(height),
    role: "img",
  });
  host.append(svg);
  return svg;
}

function niceTicks(max, count = 4) {
  if (!(max > 0)) return [0, 1];
  if (max <= count) {
    // small integer ranges: one tick per unit, never duplicated
    return Array.from({ length: Math.floor(max) + 1 }, (_, i) => i);
  }
  const raw = max / count;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / mag;
  let step = (norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10) * mag;
  if (step < 1) step = 1;
  const top = Math.ceil(max / step) * step;
  const out = [];
  for (let v = 0; v <= top + step * 1e-6; v += step) out.push(Math.round(v));
  return out;
}

/* ------------------------------------------------------------- backlog */

function renderBacklog() {
  const host = $("#backlog-chart");
  const s = state.snapshot;
  const hist = s.history || [];
  const baseHist = (s.baseline && s.baseline.history) || [];
  const showBase = state.mode !== "naive" && baseHist.length > 0;
  $("#backlog-legend").replaceChildren(
    legendItem("当前运行", "var(--series-1)"),
    ...(showBase ? [legendItem("朴素基准", "var(--text-muted)", true)] : [])
  );
  buildTableToggle("backlog");
  if (hist.length < 2) {
    host.replaceChildren(el("p", { class: "empty-note", text: "运行几个 tick 后显示积压曲线。" }));
    renderBacklogTable(hist, baseHist, showBase);
    return;
  }
  const W = Math.max(280, host.clientWidth || 520), H = 220;
  const m = { t: 12, r: 14, b: 26, l: 42 };
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const series = [
    { name: "当前运行", color: css("--series-1"), data: hist.map((h) => [h.tick, h.max_queue || 0]) },
  ];
  if (showBase) series.push({
    name: "朴素基准", color: css("--text-muted"), dash: true,
    data: baseHist.map((h) => [h.tick, h.max_queue || 0]),
  });
  const maxY = Math.max(4, ...series.flatMap((s2) => s2.data.map((d) => d[1])));
  const ticks = niceTicks(maxY);
  const yMax = ticks[ticks.length - 1] || 1;
  const maxX = Math.max(...series.flatMap((s2) => s2.data.map((d) => d[0])), 1);
  const x = (v) => m.l + (v / maxX) * iw;
  const y = (v) => m.t + ih - (v / yMax) * ih;

  const svg = svgRoot(host, W, H);
  for (const t of ticks) {
    svg.append(svgEl("line", { x1: m.l, x2: W - m.r, y1: y(t), y2: y(t), class: "gridline" }));
    svg.append(textNode(m.l - 8, y(t) + 3.5, compact(t), "end"));
  }
  svg.append(svgEl("line", { x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih, class: "gridline" }));
  const xTicks = [1, Math.round(maxX / 2), maxX].filter((v, i, a) => v > 0 && a.indexOf(v) === i);
  for (const t of xTicks) {
    const anchor = t === maxX ? "end" : (t === 1 ? "start" : "middle");
    svg.append(textNode(x(t), H - 8, `tick ${t}`, anchor));
  }

  for (const s2 of series) {
    const path = s2.data.map((d, i) => `${i ? "L" : "M"}${x(d[0]).toFixed(1)},${y(d[1]).toFixed(1)}`).join(" ");
    svg.append(svgEl("path", {
      d: path, fill: "none", stroke: s2.color, "stroke-width": s2.dash ? 2 : 2,
      "stroke-linejoin": "round", "stroke-linecap": "round",
      ...(s2.dash ? { "stroke-dasharray": "5 4", opacity: ".8" } : {}),
    }));
    if (!s2.dash) {
      const last = s2.data[s2.data.length - 1];
      svg.append(svgEl("circle", {
        cx: x(last[0]), cy: y(last[1]), r: 4, fill: s2.color,
        stroke: css("--surface-1"), "stroke-width": 2,
      }));
      svg.append(textNode(x(last[0]) - 6, y(last[1]) - 8, compact(last[1]), "end", "label-strong"));
    }
  }

  const cross = svgEl("line", { y1: m.t, y2: m.t + ih, stroke: css("--line-strong"), "stroke-width": 1, opacity: "0" });
  svg.append(cross);
  const hit = svgEl("rect", { x: m.l, y: m.t, width: iw, height: ih, class: "hit" });
  svg.append(hit);
  const byTick = new Map(hist.map((h) => [h.tick, h]));
  const baseByTick = new Map(baseHist.map((h) => [h.tick, h]));
  hit.addEventListener("pointermove", (evt) => {
    const box = svg.getBoundingClientRect();
    const px = (evt.clientX - box.left) * (W / box.width);
    const tick = Math.min(maxX, Math.max(1, Math.round(((px - m.l) / iw) * maxX)));
    const cur = byTick.get(tick), base = baseByTick.get(tick);
    cross.setAttribute("x1", x(tick));
    cross.setAttribute("x2", x(tick));
    cross.setAttribute("opacity", "1");
    showTip(evt, `tick ${tick}`, [
      ["当前最大积压", cur ? num(cur.max_queue) : "—"],
      ["当前已聚合", cur ? num(cur.processed) : "—"],
      ...(showBase ? [["朴素基准最大积压", base ? num(base.max_queue) : "—"]] : []),
    ]);
  });
  hit.addEventListener("pointerleave", () => { cross.setAttribute("opacity", "0"); hideTip(); });
  renderBacklogTable(hist, baseHist, showBase);
}

function renderBacklogTable(hist, baseHist, showBase) {
  const wrap = $("#backlog-table");
  const baseByTick = new Map(baseHist.map((h) => [h.tick, h]));
  const sample = sampleRows(hist, 24);
  const head = ["tick", "已聚合", "最大积压", ...(showBase ? ["基准最大积压"] : [])];
  wrap.replaceChildren(el("table", { class: "data-table" }, [
    el("thead", {}, [el("tr", {}, head.map((h, i) => el("th", { class: i ? "num" : "", text: h })))]),
    el("tbody", {}, sample.map((h) => el("tr", {}, [
      el("td", { class: "mono", text: String(h.tick) }),
      el("td", { class: "num", text: num(h.processed) }),
      el("td", { class: "num", text: num(h.max_queue) }),
      ...(showBase ? [el("td", { class: "num", text: num((baseByTick.get(h.tick) || {}).max_queue) })] : []),
    ]))),
  ]));
}

function sampleRows(rows, n) {
  if (rows.length <= n) return rows;
  const out = [];
  for (let i = 0; i < n; i++) out.push(rows[Math.round((i * (rows.length - 1)) / (n - 1))]);
  return out;
}

/* ------------------------------------------------------- process chart */

function processColor(idx) {
  return css(`--series-${(idx % 8) + 1}`);
}

function renderProcesses() {
  const host = $("#proc-chart");
  const s = state.snapshot;
  const procs = s.processes;
  const totalTraffic = procs.reduce((acc, p) => acc + p.traffic, 0);
  $("#proc-legend").replaceChildren(
    legendItem(`累计流量（上）· 合计 ${compact(totalTraffic)}`, "var(--series-1)"),
    legendItem("状态条目（下，按峰值缩放）", "var(--series-1)") );
  buildTableToggle("proc");
  const W = Math.max(280, host.clientWidth || 520), H = 232;
  const m = { t: 14, r: 16, b: 42, l: 62 };
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const maxTraffic = Math.max(1, ...procs.map((p) => p.traffic));
  const maxState = Math.max(1, ...procs.map((p) => p.state_bytes));
  const ticks = niceTicks(maxTraffic);
  const yMax = ticks[ticks.length - 1] || 1;
  const band = iw / Math.max(1, procs.length);
  const barW = Math.min(24, Math.max(8, band * 0.34));
  const svg = svgRoot(host, W, H);
  for (const t of ticks) {
    svg.append(svgEl("line", { x1: m.l, x2: W - m.r, y1: m.t + ih - (t / yMax) * ih, y2: m.t + ih - (t / yMax) * ih, class: "gridline" }));
    svg.append(textNode(m.l - 8, m.t + ih - (t / yMax) * ih + 3.5, compact(t), "end"));
  }
  svg.append(svgEl("line", { x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih, class: "gridline" }));
  procs.forEach((p, i) => {
    const cx = m.l + band * (i + 0.5);
    const h = (p.traffic / yMax) * ih;
    const color = css("--series-1");
    const bar = svgEl("rect", {
      x: cx - barW / 2, y: m.t + ih - h, width: barW, height: Math.max(1, h),
      rx: 4, fill: color,
    });
    // square at the baseline: overlay a rect to square off the bottom corners
    const square = svgEl("rect", {
      x: cx - barW / 2, y: m.t + ih - Math.min(4, h), width: barW, height: Math.min(4, h), fill: color,
    });
    svg.append(bar, square);
    const stateH = Math.max(2, (p.state_bytes / maxState) * 5);
    svg.append(svgEl("rect", {
      x: cx - barW / 2, y: m.t + ih + 5, width: barW, height: stateH,
      rx: 1.5, fill: color, opacity: ".4",
    }));
    svg.append(textNode(cx, m.t + ih + 25, p.name, "middle", "axis-label"));
    svg.append(textNode(cx, m.t + ih + 36, `${num(p.queue)} 排队`, "middle", "axis-label"));
    if (p.suspended) {
      // status pill: icon + label, never color alone; clamped inside the plot
      const pw = 46, ph = 15;
      const px = Math.max(m.l, Math.min(W - m.r - pw, cx - pw / 2));
      const py = Math.max(m.t + 1, m.t + ih - h - ph - 4);
      svg.append(svgEl("rect", {
        x: px, y: py, width: pw, height: ph, rx: 7,
        fill: css("--surface-1"), stroke: css("--critical"), "stroke-width": 1,
      }));
      const gx = px + 6, gy = py + 4.5;
      svg.append(svgEl("rect", { x: gx, y: gy, width: 2.4, height: 6.4, rx: .8, fill: css("--critical") }));
      svg.append(svgEl("rect", { x: gx + 4, y: gy, width: 2.4, height: 6.4, rx: .8, fill: css("--critical") }));
      svg.append(textNode(px + 14, py + 10.8, "已暂停", "start", "status-pill"));
    }
  });
  svg.append(textNode(m.l - 8, m.t + ih + 25, "流量", "end", "axis-label"));
  svg.append(textNode(m.l - 8, m.t + ih + 36, "状态", "end", "axis-label"));
  // hover bands
  procs.forEach((p, i) => {
    const cx = m.l + band * (i + 0.5);
    const hit = svgEl("rect", { x: m.l + band * i, y: m.t, width: band, height: ih, class: "hit" });
    hoverable(hit,
      () => `${p.name}${p.suspended ? "（已暂停）" : ""}`,
      () => [
        ["累计流量", num(p.traffic)],
        ["已聚合行", num(p.rows_processed)],
        ["当前排队", num(p.queue)],
        ["状态条目", num(p.state_entries)],
        ["状态大小", bytes(p.state_bytes)],
        ["重启次数", String(p.restarts)],
        ["热点键", p.top && p.top[0] ? `${p.top[0].identity} (${num4(p.top[0].value)})` : "—"],
      ]);
    svg.append(hit);
  });
  renderProcessTable(procs);
}

function renderProcessTable(procs) {
  const wrap = $("#proc-table");
  wrap.replaceChildren(el("table", { class: "data-table" }, [
    el("thead", {}, [el("tr", {}, ["进程", "流量", "已聚合", "排队", "状态条目", "状态大小", "重启", "热点键"].map((h, i) => el("th", { class: i && i < 7 ? "num" : "", text: h })))]),
    el("tbody", {}, procs.map((p) => el("tr", {}, [
      el("td", {}, [el("span", { class: "mono", text: p.name })]),
      el("td", { class: "num", text: num(p.traffic) }),
      el("td", { class: "num", text: num(p.rows_processed) }),
      el("td", { class: "num", text: num(p.queue) }),
      el("td", { class: "num", text: num(p.state_entries) }),
      el("td", { class: "num", text: bytes(p.state_bytes) }),
      el("td", { class: "num", text: String(p.restarts) }),
      el("td", { class: "mono", text: p.top && p.top[0] ? p.top[0].identity : "—" }),
    ]))),
  ]));
}

/* ---------------------------------------------------- contribution view */

function renderContrib() {
  const s = state.snapshot;
  const base = state.base;
  const host = $("#contrib-chart");
  buildTableToggle("contrib");
  const keys = (s.skew || []).map((r) => r.key);
  const sel = $("#contrib-key");
  if (sel.options.length !== keys.length || keys.some((k, i) => sel.options[i] && sel.options[i].value !== k)) {
    const prev = state.contribKey;
    sel.replaceChildren(...keys.map((k) => el("option", { value: k, text: k })));
    if (keys.includes(prev)) sel.value = prev;
  }
  if (!base) {
    host.replaceChildren(el("p", { class: "empty-note", text: "选择运行并推进几个 tick，即可看到局部贡献。" }));
    return;
  }
  const info = base.info || {};
  $("#contrib-hint").textContent = `${base.base} · ${info.label || base.metric} · 合并规则：${info.merge || "—"}`;
  const lanes = (base.lanes || []).filter((l) => (l.published && l.published.rows) || (l.local || []).length);
  const contributions = lanes.map((l) => ({
    identity: l.identity,
    value: l.published ? l.published.value : 0,
    rows: l.published ? l.published.rows : 0,
    local: l.local || [],
  }));
  const contribLegend = lanes.map((l, i) => legendItem(l.identity.split("#p").pop(), processColor(i)));
  if (base.merged && base.merged.rows > 0) contribLegend.push(legendItem("合并", "var(--text-primary)"));
  if (base.truth != null) contribLegend.push(legendItem("真值", "var(--text-muted)", true));
  $("#contrib-legend").replaceChildren(...contribLegend);

  if (!contributions.length) {
    host.replaceChildren(el("p", { class: "empty-note", text: "该键尚无已合并的分片证据。拆分后继续推进 tick。" }));
    renderCompare(null, base);
    renderContribTable([]);
    return;
  }

  const W = Math.max(300, host.clientWidth || 560), H = 252;
  const m = { t: 16, r: 78, b: 34, l: 46 };
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const metric = base.metric;
  const values = contributions.map((c) => c.value);
  const mergedValue = base.merged && base.merged.rows > 0 ? base.merged.value : null;
  const bars = contributions.map((c, i) => ({ ...c, color: processColor(i), isMerged: false }));
  if (mergedValue != null) bars.push({
    identity: "合并", value: mergedValue, rows: base.merged.rows,
    color: css("--text-primary"), isMerged: true, local: [],
  });
  const truth = base.truth;
  const maxV = Math.max(1, ...values, mergedValue == null ? 0 : mergedValue, truth == null ? 0 : truth);
  const ticks = niceTicks(maxV);
  const yMax = ticks[ticks.length - 1] || 1;
  const band = iw / bars.length;
  const barW = Math.min(24, band * 0.46);
  const svg = svgRoot(host, W, H);
  for (const t of ticks) {
    svg.append(svgEl("line", { x1: m.l, x2: W - m.r, y1: m.t + ih - (t / yMax) * ih, y2: m.t + ih - (t / yMax) * ih, class: "gridline" }));
    svg.append(textNode(m.l - 8, m.t + ih - (t / yMax) * ih + 3.5, metric === "avg" ? num4(t) : compact(t), "end"));
  }
  svg.append(svgEl("line", { x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih, class: "gridline" }));
  if (truth != null) {
    // truth is a reference line, not a second axis
    const ty = m.t + ih - (truth / yMax) * ih;
    svg.append(svgEl("line", {
      x1: m.l, x2: W - m.r, y1: ty, y2: ty,
      stroke: css("--text-muted"), "stroke-width": 1.5, "stroke-dasharray": "5 4", opacity: ".9",
    }));
    svg.append(textNode(W - m.r + 6, ty + 3.5, `真值 ${num4(truth)}`, "start", "axis-label"));
  }
  bars.forEach((c, i) => {
    const cx = m.l + band * (i + 0.5);
    const h = (c.value / yMax) * ih;
    svg.append(svgEl("rect", {
      x: cx - barW / 2, y: m.t + ih - h, width: barW, height: Math.max(1, h), rx: 4, fill: c.color,
    }));
    svg.append(svgEl("rect", {
      x: cx - barW / 2, y: m.t + ih - Math.min(4, h), width: barW, height: Math.min(4, h), fill: c.color,
    }));
    const narrow = band < 52;
    svg.append(textNode(cx, m.t + ih - h - 6, metric === "avg" ? num4(c.value) : compact(c.value), "middle", "axis-label"));
    const label = c.isMerged ? "合并" : "p" + c.identity.split("#p").pop();
    svg.append(textNode(cx, m.t + ih + 16, label, "middle", "axis-label"));
    // the row count is secondary; drop it before labels start colliding
    if (!narrow) svg.append(textNode(cx, m.t + ih + 27, `${num(c.rows)} 行`, "middle", "axis-label"));
    const hit = svgEl("rect", { x: m.l + band * i, y: m.t, width: band, height: ih, class: "hit" });
    hoverable(hit,
      () => (c.isMerged ? `合并结果 · ${base.base}` : c.identity),
      () => [
        [c.isMerged ? "最终结果" : "局部结果", num4(c.value)],
        ["行数", num(c.rows)],
        ["驻留进程", c.isMerged ? "发布器" : ((c.local || []).map((l) => l.process).join(", ") || "—")],
        ["证据", c.isMerged ? evidenceText(metric, base.merged && base.merged.support)
          : evidenceText(metric, lanes[i] && lanes[i].published && lanes[i].published.support)],
      ]);
    svg.append(hit);
  });
  renderCompare(contributions, base);
  renderContribTable(bars.filter((b) => !b.isMerged).map((c, i) => ({
    ...c, support: lanes[i] && lanes[i].published && lanes[i].published.support,
  })));
}

function evidenceText(metric, support) {
  if (!support) return "—";
  if (support.kind === "counter") return `n=${num(support.n)}`;
  if (support.kind === "sum") return `sum=${num(support.sum)}`;
  if (support.kind === "fraction") return `sum=${num(support.sum)} / cnt=${num(support.cnt)}`;
  if (support.kind === "member-set") return `${num(support.members)} 个成员`;
  return "—";
}

function renderCompare(contributions, base) {
  const wrap = $("#compare");
  wrap.replaceChildren();
  const info = base.info || {};
  const merged = base.merged || {};
  if (!contributions || !contributions.length || !(merged.rows > 0)) {
    wrap.append(el("p", {
      class: "empty-note",
      text: "还没有已合并的证据。推进 tick、拆分该键后，这里会并列显示合并结果、真值与朴素相加的错误值。",
    }));
    return;
  }
  wrap.append(compareBox("合并后最终结果", num4(merged.value),
    `${info.merge || ""} · ${num(merged.rows)} 行`, "right"));
  if (base.truth != null) {
    wrap.append(compareBox("真值", num4(base.truth), "同一份虚构数据直接聚合", "right"));
  }
  const naive = base.naive;
  if (naive) {
    const wrong = !naive.correct && Math.abs(naive.error == null ? 0 : naive.error) > 1e-9;
    const box = compareBox(
      naive.correct ? "朴素相加（与该指标相符）" : "朴素相加（错误基准）",
      num4(naive.value),
      `${naive.rule}${naive.error != null && wrong ? ` · 偏差 ${num4(naive.error)}` : ""}`,
      wrong ? "wrong" : "right",
      wrong
    );
    wrap.append(box);
  }
  if (base.error != null) {
    const ok = Math.abs(base.error) < 1e-9;
    wrap.append(compareBox(
      "合并结果与真值之差",
      ok ? "0" : num4(base.error),
      ok ? "完全一致" : "存在偏差",
      ok ? "right" : "wrong",
      ok
    ));
  }
}

function compareBox(label, value, sub, kind, withWarning = false) {
  const icon = kind === "wrong"
    ? pathIcon("M12 3 2.5 20h19L12 3Zm0 5.2v5.4m0 2.6v.2")
    : pathIcon("M4.5 12.5 9.6 17.5 19.5 6.8");
  const head = el("div", { class: "c-label" }, [
    el("span", {
      class: "status-icon",
      style: `color:${kind === "wrong" ? css("--critical") : css("--good")}`,
    }, [icon]),
    el("span", { text: label }),
  ]);
  return el("div", { class: `compare-box is-${kind}` }, [
    head,
    el("div", { class: "c-value", text: value }),
    el("div", { class: "c-sub", text: sub }),
  ]);
}

function pathIcon(d) {
  const svg = svgEl("svg", { viewBox: "0 0 24 24", width: "13", height: "13", "aria-hidden": "true" });
  svg.append(svgEl("path", { d, fill: "none", stroke: "currentColor", "stroke-width": "2", "stroke-linecap": "round", "stroke-linejoin": "round" }));
  return svg;
}

function renderContribTable(rows) {
  const wrap = $("#contrib-table");
  wrap.replaceChildren(el("table", { class: "data-table" }, [
    el("thead", {}, [el("tr", {}, ["分片", "局部结果", "行数", "证据"].map((h, i) => el("th", { class: i === 1 || i === 2 ? "num" : "", text: h })))]),
    el("tbody", {}, rows.map((r) => el("tr", {}, [
      el("td", {}, [el("span", { class: "mono", text: r.identity })]),
      el("td", { class: "num", text: num4(r.value) }),
      el("td", { class: "num", text: num(r.rows) }),
      el("td", { class: "mono", text: evidenceText(state.base.metric, r.support) }),
    ]))),
  ]));
}

/* -------------------------------------------------------------- results */

function renderResults() {
  const body = $("#results-table tbody");
  const res = state.results;
  if (!res) {
    $("#results-hint").textContent = "";
    return;
  }
  $("#results-hint").textContent = `${res.info.label} · 合并规则：${res.info.merge}`;
  body.replaceChildren();
  for (const row of res.rows.slice(0, 300)) {
    const bad = row.error != null && Math.abs(row.error) > 1e-9;
    body.append(el("tr", {}, [
      el("td", {}, [el("span", { class: "mono", text: row.base })]),
      el("td", { class: "num", text: num4(row.value) }),
      el("td", { class: "num muted", text: num4(row.truth) }),
      el("td", { class: "num" }, [
        bad
          ? el("span", { class: "badge bad", text: num4(row.error) })
          : el("span", { class: "badge good", text: "0" }),
      ]),
      el("td", { class: "num", text: String(row.lanes.length) }),
      el("td", { class: "mono muted", text: evidenceText(res.metric, row.support) }),
    ]));
  }
}

/* --------------------------------------------------------------- events */

function renderEvents() {
  const wrap = $("#event-list");
  wrap.replaceChildren();
  const events = (state.snapshot.events || []).slice().reverse().slice(0, 90);
  if (!events.length) {
    wrap.append(el("p", { class: "empty-note", text: "尚无事件。" }));
    return;
  }
  for (const e of events) {
    wrap.append(el("div", { class: "event" }, [
      el("span", { class: "et", text: `t${num1(e.t)}` }),
      el("span", { class: `ek ${e.kind}`, text: labelKind(e.kind) }),
      el("span", { class: "em", text: e.message }),
    ]));
  }
}

function labelKind(kind) {
  return ({
    split: "拆分",
    fail: "故障",
    publisher: "发布器",
    recover: "恢复",
    system: "系统",
    death: "故障",
  })[kind] || kind;
}

/* ------------------------------------------------------------- actions */

async function runAction(action, body = {}) {
  const id = await ensureRun();
  const name = (state.snapshot && state.snapshot.processes[0] && state.snapshot.processes[0].name) || "agg-0";
  const payload = { action, ...body };
  if (["kill", "suspend"].includes(action) && !payload.name) payload.name = name;
  try {
    const out = await api(`/api/runs/${id}/action`, { method: "POST", body: JSON.stringify(payload) });
    flash(describeAction(action, out));
    await refresh();
  } catch (err) {
    flash(`操作失败：${err.message}`);
  }
}

function describeAction(action, out) {
  if (!out) return "";
  if (out.error) return out.error;
  switch (action) {
    case "kill":
      return `${out.killed} 已退出：${num(out.lost_rows)} 行尚未发布丢失（可重放），状态条目 ${num(out.lost_state_entries)} 丢失。`;
    case "kill-all":
      return `全部聚合进程重启，丢失 ${num(out.lost_rows)} 行。`;
    case "kill-publisher":
      return "结果发布服务已重启；将由聚合进程保留的证据重建。";
    case "suspend":
      return out.suspended ? `${out.process} 已暂停（积压会持续增长）` : `${out.process} 已恢复`;
    case "duplicate":
      return `已重复回放 ${out.queued} 条局部结果，发布器按 event_id 去重。`;
    case "replay-old":
      return `已回放 ${out.queued} 条旧代结果，应全部被拒绝。`;
    case "replay-stale-split":
      return `已回放 ${out.queued} 条旧拆分任务结果，应全部被拒绝。`;
    case "replay-orphaned":
      return out.queued ? `已重放 ${out.queued} 行丢失数据，继续运行可补齐。` : "没有需要重放的丢失行。";
    default:
      return "已完成。";
  }
}

let flashTimer = null;
function flash(message) {
  const note = $("#action-note");
  note.textContent = message;
  clearTimeout(flashTimer);
  flashTimer = setTimeout(() => { note.textContent = ""; }, 7000);
}

/* ------------------------------------------------------------ scaffolding */

function legendItem(label, color, dashed = false) {
  const key = el("span", { class: `key ${dashed ? "dash" : ""}` });
  key.style.background = color;
  if (dashed) key.style.color = color;
  return el("span", { class: "item" }, [key, el("span", { text: label })]);
}

function textNode(x, y, textContent, anchor = "start", cls = "axis-label") {
  const node = svgEl("text", { x, y, "text-anchor": anchor, class: cls });
  node.textContent = textContent;
  return node;
}

function buildTableToggle(name) {
  const btn = document.querySelector(`[data-table-toggle="${name}"]`);
  if (!btn) return;
  const open = !!state.tablesOpen[name];
  btn.textContent = open ? "图表" : "表格";
  const chart = $(`#${name}-chart`), table = $(`#${name}-table`);
  if (chart) chart.classList.toggle("hidden", open);
  if (table) table.classList.toggle("hidden", !open);
  if (!btn.dataset.bound) {
    btn.dataset.bound = "1";
    btn.addEventListener("click", () => {
      state.tablesOpen[name] = !state.tablesOpen[name];
      buildTableToggle(name);
    });
  }
}

/* ----------------------------------------------------------------- boot */

function bindControls() {
  $("#btn-start").addEventListener("click", () => { stopPlay(); startRun(); });
  $("#btn-step").addEventListener("click", () => {
    if (state.playing) stopPlay();
    stepRun(1).catch((e) => flash(e.message));
  });
  $("#btn-play").addEventListener("click", () => {
    if (state.playing) stopPlay(); else startPlay();
  });
  $("#btn-finish").addEventListener("click", () => finishRun().catch((e) => flash(e.message)));
  $("#theme-toggle").addEventListener("click", () => {
    const cur = document.documentElement.getAttribute("data-theme");
    const dark = cur ? cur === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
    const next = dark ? "light" : "dark";
    localStorage.setItem("skewpipe-theme", next);
    applyTheme(next);
  });
  $("#mode").addEventListener("change", () => {
    state.mode = $("#mode").value;
    syncModeNote();
    stopPlay();
  });
  for (const id of ["fanout", "aggs"]) {
    const input = $(`#${id}`);
    const out = $(`#${id}-out`);
    input.addEventListener("input", () => { out.textContent = input.value; });
  }
  $("#strategy").addEventListener("change", () => {});
  $("#btn-split").addEventListener("click", async () => {
    await ensureRun();
    const key = $("#split-key").value;
    try {
      await api(`/api/runs/${state.runId}/action`, {
        method: "POST",
        body: JSON.stringify({
          action: "split", key,
          strategy: $("#strategy").value,
          fanout: Number($("#fanout").value),
        }),
      });
      flash(`已在运行中拆分 ${key} → ${$("#fanout").value} 片；旧证据已按指标规则搬运。`);
      await refresh();
    } catch (err) { flash(`拆分失败：${err.message}`); }
  });
  $("#btn-retrigger").addEventListener("click", async () => {
    await ensureRun();
    const key = $("#split-key").value;
    try {
      await api(`/api/runs/${state.runId}/action`, {
        method: "POST",
        body: JSON.stringify({
          action: "retrigger", key,
          strategy: $("#strategy").value,
          fanout: Number($("#fanout").value),
        }),
      });
      flash(`重新拆分 ${key}：旧任务退役，其后的局部结果会被发布器拒绝。`);
      await refresh();
    } catch (err) { flash(`重新拆分失败：${err.message}`); }
  });
  $("#contrib-key").addEventListener("change", async () => {
    state.contribKey = $("#contrib-key").value;
    await refresh();
  });
  for (const btn of $$("[data-action]")) {
    btn.addEventListener("click", () => {
      const action = btn.dataset.action;
      if (action === "suspend") {
        const p = state.snapshot && state.snapshot.processes[0];
        const target = p && p.suspended ? false : true;
        runAction("suspend", { on: target });
      } else {
        runAction(action);
      }
    });
  }
  window.addEventListener("resize", debounce(() => { if (state.snapshot) { renderBacklog(); renderProcesses(); renderContrib(); } }, 120));
}

function debounce(fn, ms) {
  let t = null;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

async function boot() {
  bindControls();
  try {
    state.meta = await api("/api/meta");
  } catch (_) {
    state.meta = null;
  }
  buildMetricTabs();
  syncModeNote();
  await startRun();
}

boot();
