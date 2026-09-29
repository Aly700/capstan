"use strict";

// Only this tab's session holds the key. Never serialize it into navigation or data.
const keyName = "capstan.apiKey";
const $ = (id) => document.getElementById(id);
let key = "";
try { key = sessionStorage.getItem(keyName) || ""; } catch { /* The form reports unavailable storage. */ }
let controller = new AbortController();
let revision = 0;
let pageTokens = [""];
let pageIndex = 0;
let nextToken = "";
let events = [];
let selectedRun;
let filters = { status: "", workflowType: "", pageSize: 25 };

const clean = (value) => {
  const text = String(value ?? "");
  return key ? text.split(key).join("[redacted]") : text;
};
function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = clean(text);
  if (className) node.className = className;
  return node;
}
function words(value) {
  return String(value || "Unknown").replace(/^(EVENT_TYPE_|RUN_STATUS_|APPROVAL_(OUTCOME|SOURCE)_|TIMEOUT_TYPE_|TASK_FAILED_CAUSE_)/, "")
    .toLowerCase().replaceAll("_", " ").replace(/^./, (letter) => letter.toUpperCase());
}
function timestamp(value) { return value ? value.replace("T", " ").replace("Z", "") : "—"; }
function notice(message, error = false) { $("message").textContent = clean(message); $("message").className = error ? "error" : ""; }
function connection() {
  $("connect-form").hidden = Boolean(key);
  $("connected").hidden = !key;
  $("refresh").disabled = !key;
  $("apply").disabled = !key;
}
function begin() {
  controller.abort();
  controller = new AbortController();
  return ++revision;
}
async function rpc(method, body) {
  const response = await fetch(`/capstan.v1.ClientService/${method}`, {
    method: "POST", headers: { "Content-Type": "application/json", "Connect-Protocol-Version": "1", Authorization: `Bearer ${key}` },
    body: JSON.stringify(body), signal: controller.signal, credentials: "omit", cache: "no-store", redirect: "error", referrerPolicy: "no-referrer",
  });
  if (!response.ok) {
    // Do not echo a server/proxy body: it could contain request headers.
    throw new Error(response.status === 401 ? "API key rejected. Disconnect and enter a valid key." : `${method} failed (HTTP ${response.status}). Refresh to retry.`);
  }
  return response.json();
}
function report(error, version) { if (version === revision && error.name !== "AbortError") notice(error.message, true); }
function badge(status) {
  const cls = status === "RUN_STATUS_BLOCKED" ? " blocked-status" : status === "RUN_STATUS_COMPLETED" ? " emphasis" : "";
  return element("span", words(status), `badge${cls}`);
}
async function list() {
  const version = begin();
  $("list-view").hidden = false;
  $("detail-view").hidden = true;
  document.title = "Capstan · Runs";
  $("previous").disabled = $("next").disabled = true;
  if (!key) return;
  notice("Loading runs…");
  try {
    const page = await rpc("ListRuns", { ...filters, ...(filters.status ? { status: `RUN_STATUS_${filters.status}` } : { status: "RUN_STATUS_UNSPECIFIED" }), pageToken: pageTokens[pageIndex] });
    if (version !== revision) return;
    const rows = (page.runs || []).map((run) => {
      const row = element("tr");
      const cell = element("td");
      const link = element("a", run.runId);
      link.href = `#run=${encodeURIComponent(run.runId)}`;
      cell.append(link);
      const state = element("td"); state.append(badge(run.status));
      row.append(cell, state, element("td", run.workflowType), element("td", timestamp(run.startedAt)), element("td", run.lastEventId || "0", "numeric"));
      return row;
    });
    if (!rows.length) { const row = element("tr"); const cell = element("td", "No runs match these filters.", "empty"); cell.colSpan = 5; row.append(cell); rows.push(row); }
    $("runs").replaceChildren(...rows);
    nextToken = page.nextPageToken || "";
    $("previous").disabled = pageIndex === 0;
    $("next").disabled = !nextToken;
    $("page-label").textContent = `Page ${pageIndex + 1}`;
    notice(`${(page.runs || []).length} runs on this page. Refreshed ${new Date().toISOString().slice(11, 19)} UTC.`);
  } catch (error) { report(error, version); }
}
function attributes(event) {
  return Object.entries(event).find(([name, value]) => !["eventId", "type", "time"].includes(name) && value && typeof value === "object")?.[1] || {};
}
function unpack(value) {
  if (!value || typeof value !== "object") return value;
  if (typeof value.contentType === "string") {
    const data = value.data || "";
    if (value.contentType === "application/json" || value.contentType.startsWith("text/")) {
      try {
        const text = new TextDecoder("utf-8", { fatal: true }).decode(Uint8Array.from(atob(data), (char) => char.charCodeAt(0)));
        return { contentType: value.contentType, value: value.contentType === "application/json" ? JSON.parse(text) : text };
      } catch { return { contentType: value.contentType, base64: data, note: "Unable to decode payload" }; }
    }
    return { contentType: value.contentType, base64: data };
  }
  return Array.isArray(value) ? value.map(unpack) : Object.fromEntries(Object.entries(value).map(([name, item]) => [name, unpack(item)]));
}
function eventFields(event, byId) {
  const a = attributes(event);
  const scheduled = attributes(byId.get(a.scheduledEventId) || {});
  const started = attributes(byId.get(a.startedEventId) || {});
  const fields = [];
  const field = (name, value) => { if (value !== undefined && value !== "") fields.push(`${name}: ${value}`); };
  field("activity", a.activityType || scheduled.activityType);
  field("attempt", a.attempt);
  field("duration", a.fireAfter || started.fireAfter);
  for (const [label, name] of [["step", "seq"], ["workflow", "workflowType"], ["queue", "taskQueue"], ["name", "name"], ["marker", "markerId"], ["approval", "approvalId"], ["choice", "choice"], ["resolver", "resolver"], ["worker / caller", "identity"], ["reason", "reason"], ["prompt", "prompt"], ["continues as", "newRunId"]]) field(label, a[name]);
  for (const name of ["outcome", "source", "timeoutType"]) if (a[name]) field(name, words(a[name]));
  if (a.taskFailedEventId) field("failed task event", `#${a.taskFailedEventId}`);
  return fields.join(" · ");
}
function failures(failure) {
  const parts = [];
  for (let item = failure, depth = 0; item && depth < 10; item = item.cause, depth++) parts.push(`${item.type || "Failure"}: ${item.message || "No message"}`);
  return parts.join(" → ");
}
async function revealEvent(runId, eventId) {
  while (!$(eventId) && selectedRun?.runId === runId && !$("more-history").hidden) {
    const loaded = events.length;
    await detail(runId, true);
    if (events.length === loaded) return;
  }
  if (selectedRun?.runId === runId) $(eventId)?.scrollIntoView({ block: "center" });
}
function renderTimeline() {
  const byId = new Map(events.map((event) => [event.eventId, event]));
  const rows = events.map((event) => {
    const a = attributes(event);
    const major = !event.type?.startsWith("EVENT_TYPE_TASK_");
    const row = element("li", undefined, `event${major ? " major" : ""}${event.runBlocked ? " blocked-event" : ""}`);
    row.id = `event-${event.eventId}`;
    const coordinate = element("div", undefined, "event-coordinate");
    const id = element("a", `#${event.eventId}`);
    // Event anchors stay local and do not replace the selected run's fragment.
    id.href = `#run=${encodeURIComponent(selectedRun.runId)}`;
    id.addEventListener("click", (e) => { e.preventDefault(); row.scrollIntoView({ block: "center" }); });
    const time = element("time", timestamp(event.time)); time.dateTime = event.time || "";
    coordinate.append(id, time);
    const body = element("div", undefined, "event-body");
    body.append(element("span", words(event.type), `badge${major ? " emphasis" : ""}`));
    const fields = eventFields(event, byId);
    if (fields) body.append(element("p", fields, "event-attributes"));
    if (a.failure || a.lastFailure) body.append(element("p", failures(a.failure || a.lastFailure), "failure"));
    if (Object.keys(a).length) {
      const details = element("details");
      details.append(element("summary", "Attributes & payloads"), element("pre", JSON.stringify(unpack(a), null, 2)));
      body.append(details);
    }
    row.append(coordinate, body);
    return row;
  });
  $("timeline").replaceChildren(...rows);
  $("event-count").textContent = `${events.length} events loaded`;
  const blocked = selectedRun.status === "RUN_STATUS_BLOCKED";
  $("blocked").hidden = !blocked;
  if (blocked) {
    const mismatch = events.findLast((event) => event.runBlocked);
    const failure = mismatch?.runBlocked.failure || selectedRun.failure;
    const metadata = unpack(failure?.details)?.value?.$capstan;
    const hasMismatchEvent = metadata?.kind === "mismatch" && Number.isSafeInteger(metadata.eventId) && metadata.eventId > 0;
    const heading = element("h2", mismatch ? `Replay mismatch · event #${mismatch.eventId}` : "Replay mismatch · run blocked");
    if (hasMismatchEvent) {
      const eventId = `event-${metadata.eventId}`;
      const runId = selectedRun.runId;
      const link = element("a", `Replay mismatch at event ${metadata.eventId}`);
      link.href = `#run=${encodeURIComponent(runId)}&event=${metadata.eventId}`;
      link.addEventListener("click", async (event) => {
        event.preventDefault();
        await revealEvent(runId, eventId);
      });
      heading.replaceChildren(link);
    }
    $("blocked").replaceChildren(
      heading,
      element("p", failures(failure)),
      element("p", "Deploy compatible workflow code, then explicitly resume this run:"),
      element("pre", `capstan resume '${selectedRun.runId.replaceAll("'", "'\\''")}'`),
    );
  }
}
async function detail(runId, more = false) {
  const version = begin();
  $("list-view").hidden = true;
  $("detail-view").hidden = false;
  $("more-history").disabled = true;
  if (!more) { events = []; $("timeline").replaceChildren(); $("blocked").hidden = true; $("run-meta").replaceChildren(); $("event-count").textContent = ""; $("more-history").hidden = true; }
  $("run-title").textContent = clean(runId);
  document.title = "Capstan · Run history";
  if (!key) return;
  notice("Loading history…");
  try {
    const [description, page] = await Promise.all([
      rpc("DescribeRun", { runId }), rpc("GetHistory", { runId, afterEventId: more ? events.at(-1)?.eventId || "0" : "0", pageSize: 500 }),
    ]);
    if (version !== revision) return;
    selectedRun = description.run;
    if (!selectedRun) throw new Error("Run not returned by server.");
    if (page.more && (!page.events?.length || BigInt(page.events.at(-1).eventId) <= BigInt(events.at(-1)?.eventId || 0))) throw new Error("History pagination did not advance.");
    events.push(...(page.events || []));
    $("run-meta").replaceChildren(badge(selectedRun.status), element("span", `Type: ${selectedRun.workflowType}`), element("span", `Queue: ${selectedRun.taskQueue}`), element("span", `Started: ${timestamp(selectedRun.startedAt)} UTC`));
    renderTimeline();
    $("more-history").hidden = !page.more;
    $("more-history").disabled = false;
    notice(`History refreshed ${new Date().toISOString().slice(11, 19)} UTC.`);
  } catch (error) { report(error, version); }
}
async function route() {
  try {
    const match = /^#run=([^&]*)(?:&event=([1-9]\d*))?$/.exec(location.hash);
    if (!match) return list();
    const runId = decodeURIComponent(match[1]);
    await detail(runId);
    if (match[2]) await revealEvent(runId, `event-${match[2]}`);
  }
  catch { notice("Invalid run link.", true); }
}
$("connect-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const entered = $("api-key").value.trim();
  if (!entered) return;
  try { sessionStorage.setItem(keyName, entered); }
  catch { $("api-key").value = ""; notice("Session storage is unavailable. Enable it for this origin to connect.", true); return; }
  key = entered;
  $("api-key").value = "";
  connection(); route();
});
$("disconnect").addEventListener("click", () => {
  begin();
  try { sessionStorage.removeItem(keyName); } catch { /* Clear the in-memory session too. */ }
  key = ""; selectedRun = undefined; events = []; pageTokens = [""]; pageIndex = 0;
  $("runs").replaceChildren(); $("timeline").replaceChildren(); $("blocked").replaceChildren();
  $("run-meta").replaceChildren(); $("run-title").textContent = "";
  connection(); location.hash = ""; route(); notice("Disconnected. Session key cleared.");
});
$("filters").addEventListener("submit", (event) => {
  event.preventDefault();
  filters = { status: $("status").value, workflowType: $("workflow-type").value.trim(), pageSize: Number($("page-size").value) };
  pageTokens = [""]; pageIndex = 0; list();
});
$("refresh").addEventListener("click", list);
$("previous").addEventListener("click", () => { if (pageIndex > 0) { pageIndex--; list(); } });
$("next").addEventListener("click", () => { if (nextToken) { pageTokens[++pageIndex] = nextToken; list(); } });
$("refresh-run").addEventListener("click", route);
$("more-history").addEventListener("click", () => { if (selectedRun) detail(selectedRun.runId, true); });
window.addEventListener("hashchange", route);
connection(); route();
