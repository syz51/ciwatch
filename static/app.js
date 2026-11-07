"use strict";

const REFRESH_MS = 5000;
const STATE_LABEL = {
  queued: "Queued", waiting: "Waiting", running: "Running",
  success: "Success", failure: "Failed", cancelled: "Cancelled", skipped: "Skipped",
};
const PROVIDER_LABEL = { github: "GitHub", circleci: "CircleCI" };
const SOURCE_LABEL = {
  "github-webhook": "GitHub webhooks", "github-poll": "GitHub poll", "circleci-poll": "CircleCI poll",
};

let view = "active";
let latest = null;
// Server time minus browser time, so ages do not depend on the viewer's clock.
let serverOffset = 0;
let lastOk = null;

const $ = (id) => document.getElementById(id);

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") node.className = v;
    else node.setAttribute(k, v);
  }
  for (const c of children) {
    if (c === null || c === undefined) continue;
    node.append(c instanceof Node ? c : String(c));
  }
  return node;
}

function safeUrl(value) {
  try {
    const u = new URL(value);
    return u.protocol === "https:" || u.protocol === "http:" ? u.href : null;
  } catch {
    return null;
  }
}

function since(iso, now) {
  if (!iso) return null;
  return Math.max(0, (now - Date.parse(iso)) / 1000);
}

function fmtDuration(seconds) {
  if (seconds === null || Number.isNaN(seconds)) return "–";
  const s = Math.round(seconds);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${String(m % 60).padStart(2, "0")}m`;
}

function timeCell(job, now) {
  if (job.state === "running") {
    return [fmtDuration(since(job.started_at || job.created_at, now)), "running"];
  }
  if (job.state === "queued" || job.state === "waiting") {
    return [fmtDuration(since(job.created_at, now)), job.state];
  }
  const took = job.started_at && job.finished_at
    ? (Date.parse(job.finished_at) - Date.parse(job.started_at)) / 1000
    : null;
  const ago = since(job.finished_at, now);
  return [fmtDuration(took), ago === null ? null : `ended ${fmtDuration(ago)} ago`];
}

function matches(job, needle) {
  if (!needle) return true;
  return [job.repository, job.name, job.pipeline, job.branch, job.pool, job.runner, job.provider]
    .some((v) => v && v.toLowerCase().includes(needle));
}

function renderSources(sources, now) {
  const list = $("sources");
  list.replaceChildren();
  for (const [name, h] of Object.entries(sources)) {
    const failing = h.last_error_at && (!h.last_success || h.last_error_at > h.last_success);
    const label = SOURCE_LABEL[name] || name;
    let detail;
    if (failing) detail = `failing: ${h.last_error}`;
    else if (h.last_success) detail = `ok, ${fmtDuration(since(h.last_success, now))} ago`;
    else detail = name === "github-webhook" ? "no deliveries yet" : "waiting for first poll";
    list.append(el("li", { class: failing ? "failing" : null },
      el("span", {}, label), el("span", { class: "detail" }, detail)));
  }
}

function renderPools(jobs, now) {
  const pools = new Map();
  for (const job of jobs) {
    if (!pools.has(job.pool)) pools.set(job.pool, { running: 0, queued: 0, waiting: 0, oldest: null });
    const p = pools.get(job.pool);
    if (job.state in p) p[job.state] += 1;
    if (job.state === "queued" && job.created_at) {
      const age = since(job.created_at, now);
      p.oldest = p.oldest === null ? age : Math.max(p.oldest, age);
    }
  }
  const list = $("pools");
  list.replaceChildren();
  const names = [...pools.keys()].sort((a, b) => {
    const pa = pools.get(a), pb = pools.get(b);
    return (pb.running + pb.queued + pb.waiting) - (pa.running + pa.queued + pa.waiting) || a.localeCompare(b);
  });
  for (const name of names) {
    const p = pools.get(name);
    const idle = p.running + p.queued + p.waiting === 0;
    const counts = idle
      ? el("div", { class: "pool-idle" }, "Idle")
      : el("div", { class: "pool-counts" },
        el("span", {}, el("b", {}, p.running), "running"),
        el("span", {}, el("b", {}, p.queued), "queued"));
    if (p.waiting) counts.append(el("span", {}, el("b", {}, p.waiting), "waiting"));
    list.append(el("li", { class: idle ? "pool idle" : "pool" },
      el("span", { class: "pool-name" }, name),
      counts,
      p.oldest === null ? null : el("span", { class: "pool-oldest" }, `Oldest queued ${fmtDuration(p.oldest)}`)));
  }
  $("pools-empty").hidden = names.length > 0;
}

function renderRows(jobs, now) {
  const needle = $("filter").value.trim().toLowerCase();
  const active = jobs.filter((j) => ["queued", "waiting", "running"].includes(j.state));
  const finished = jobs.filter((j) => !["queued", "waiting", "running"].includes(j.state));
  $("count-active").textContent = active.length;
  $("count-finished").textContent = finished.length;
  const shown = (view === "active" ? active : finished).filter((j) => matches(j, needle));

  const rows = $("rows");
  rows.replaceChildren();
  for (const job of shown) {
    const url = safeUrl(job.url);
    const name = url ? el("a", { href: url, target: "_blank", rel: "noopener noreferrer" }, job.name) : job.name;
    const [time, timeSub] = timeCell(job, now);
    const commit = job.revision ? job.revision.slice(0, 7) : null;
    rows.append(el("tr", {},
      el("td", { class: "cell-state" },
        el("span", { class: `state ${job.state}`, title: `Provider status: ${job.provider_status}` }, STATE_LABEL[job.state] || job.state)),
      el("td", { class: "cell-job" },
        el("span", { class: "job-name" }, name),
        el("span", { class: "provider" }, PROVIDER_LABEL[job.provider] || job.provider),
        job.pipeline ? el("span", { class: "sub" }, job.pipeline) : null),
      el("td", { class: "cell-meta", "data-label": "Repo" }, job.repository),
      el("td", { class: "cell-meta", "data-label": "Branch" },
        job.branch || "–",
        commit ? el("span", { class: "sub mono", title: job.revision }, commit) : null),
      el("td", { class: "cell-meta", "data-label": "Pool" },
        job.pool,
        job.runner ? el("span", { class: "sub" }, job.runner) : null),
      el("td", { class: "num cell-time" }, time, timeSub ? el("span", { class: "sub" }, timeSub) : null)));
  }

  const empty = $("jobs-empty");
  if (shown.length === 0) {
    empty.hidden = false;
    empty.textContent = needle
      ? "No jobs match the filter."
      : view === "active" ? "Nothing is queued or running." : "No finished jobs in the retention window.";
  } else {
    empty.hidden = true;
  }
}

function render() {
  if (!latest) return;
  const now = Date.now() + serverOffset;
  renderSources(latest.sources, now);
  renderPools(latest.jobs, now);
  renderRows(latest.jobs, now);
}

async function refresh() {
  try {
    const res = await fetch("/api/jobs", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    latest = await res.json();
    const serverNow = Date.parse(latest.generated_at);
    if (!Number.isNaN(serverNow)) serverOffset = serverNow - Date.now();
    lastOk = new Date();
    $("updated").textContent = `Updated ${lastOk.toLocaleTimeString()}`;
    render();
  } catch (e) {
    $("updated").textContent = lastOk
      ? `Cannot reach ciwatch (${e.message}); showing data from ${lastOk.toLocaleTimeString()}`
      : `Cannot reach ciwatch (${e.message})`;
  }
}

document.addEventListener("DOMContentLoaded", () => {
  for (const button of document.querySelectorAll(".segmented button")) {
    button.addEventListener("click", () => {
      view = button.dataset.view;
      for (const b of document.querySelectorAll(".segmented button")) {
        b.setAttribute("aria-pressed", String(b === button));
      }
      render();
    });
  }
  $("filter").addEventListener("input", render);
  refresh();
  setInterval(refresh, REFRESH_MS);
});
