import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { Evidence, managed, root, until, payload } from "./evidence-lib.mjs";

// npm exec adds the ephemeral package's bin directory to PATH.
const packageFile = process.env.PATH.split(":").map((dir) => join(dirname(dir), "playwright/package.json")).find(existsSync);
if (!packageFile) throw new Error("Run scripts/check-ui.sh (Playwright comes from npx)");
const { chromium } = createRequire(packageFile)("playwright");
await managed(new Evidence("ui", 7300), async (env) => {
  let worker = env.worker();
  const injection = '<img src="/ui/leak" onerror="window.historyInjection=true">';
  for (let n = 1; n <= 12; n++) await env.start("viewerCompleted", `evidence-completed-${String(n).padStart(2, "0")}`, { n, note: injection });
  for (let n = 1; n <= 12; n++) await env.status(`evidence-completed-${String(n).padStart(2, "0")}`, "RUN_STATUS_COMPLETED");
  await env.stop(worker);
  worker = env.worker("blocked-v1.ts");
  await env.start("changedWorkflow", "evidence-blocked");
  await until("v1 idle", async () => (await env.history("evidence-blocked")).length >= 9 && env.sql("select count(*) from task", ["-Atq"]).trim() === "0");
  await env.stop(worker);
  worker = env.worker("blocked-v2.ts");
  await env.rpc("SignalRun", { runId: "evidence-blocked", name: "continue", input: payload(true) });
  await env.status("evidence-blocked", "RUN_STATUS_BLOCKED");
  const browser = await chromium.launch({ channel: "chrome", headless: true });
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 1000 } });
    const page = await context.newPage();
    const errors = [], requests = [];
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
    page.on("request", (request) => requests.push({ url: request.url(), body: request.postData(), method: request.method() }));
    const unauthenticated = await context.request.post(`${env.address}/capstan.v1.ClientService/ListRuns`, { data: {}, headers: { "Connect-Protocol-Version": "1" } });
    assert.equal(unauthenticated.status(), 401);
    const invalid = await context.request.post(`${env.address}/capstan.v1.ClientService/ListRuns`, { data: {}, headers: { Authorization: "Bearer wrong", "Connect-Protocol-Version": "1" } });
    assert.equal(invalid.status(), 401);
    await page.goto(`${env.address}/ui/`);
    await page.getByLabel("API key", { exact: true }).fill(env.key);
    await page.getByRole("button", { name: "Connect", exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll("#runs tr").length === 13);
    await page.getByLabel("Per page").selectOption("10");
    await page.getByRole("button", { name: "Apply filters" }).click();
    await page.waitForFunction(() => document.querySelectorAll("#runs tr").length === 10);
    await page.screenshot({ path: join(root, "docs/evidence/ui-list.png"), fullPage: true });
    await page.getByRole("button", { name: "Next", exact: true }).click();
    await page.waitForFunction(() => document.getElementById("page-label").textContent === "Page 2");
    assert.equal(await page.locator("#runs tr").count(), 3);
    await page.getByRole("button", { name: "Previous", exact: true }).click();
    await page.waitForFunction(() => document.getElementById("page-label").textContent === "Page 1");
    await page.getByLabel("Status", { exact: true }).selectOption("COMPLETED");
    await page.getByLabel("Workflow type", { exact: true }).fill("viewerCompleted");
    await page.getByRole("button", { name: "Apply filters" }).click();
    await page.waitForFunction(() => !document.getElementById("runs").textContent.includes("Blocked") && document.querySelectorAll("#runs tr").length === 10);
    await page.getByRole("link", { name: "evidence-completed-01", exact: true }).click();
    await page.waitForSelector("#timeline .event");
    assert.equal(await page.locator("#timeline details[open]").count(), 0);
    await page.locator("#timeline summary").first().click();
    assert.match(await page.locator("#timeline pre").first().textContent(), /<img src=/);
    assert.equal(await page.evaluate(() => window.historyInjection), undefined);
    await page.locator("#timeline summary").first().click();
    await page.screenshot({ path: join(root, "docs/evidence/ui-completed.png"), fullPage: true });
    await page.goto(`${env.address}/ui/#run=evidence-blocked`);
    await page.waitForSelector("#blocked:not([hidden])");
    assert.match(await page.locator("#blocked").textContent(), /capstan resume 'evidence-blocked'/);
    assert.equal(await page.locator("#blocked h2").textContent(), "Replay mismatch at event 5");
    const mismatchLink = page.locator("#blocked h2 a");
    assert.equal(await mismatchLink.getAttribute("href"), "#run=evidence-blocked&event=5");
    await mismatchLink.click();
    assert.equal(new URL(page.url()).hash, "#run=evidence-blocked");
    assert.equal(await page.locator("#event-5").evaluate((event) => {
      const bounds = event.getBoundingClientRect();
      return bounds.top >= 0 && bounds.bottom <= innerHeight;
    }), true);
    const linkedPage = await context.newPage();
    await linkedPage.goto(`${env.address}/ui/#run=evidence-blocked&event=5`);
    await linkedPage.getByLabel("API key", { exact: true }).fill(env.key);
    await linkedPage.getByRole("button", { name: "Connect", exact: true }).click();
    await linkedPage.waitForFunction(() => {
      const event = document.getElementById("event-5");
      if (!event || document.getElementById("detail-view").hidden) return false;
      const bounds = event.getBoundingClientRect();
      return bounds.top >= 0 && bounds.bottom <= innerHeight;
    });
    assert.equal(await linkedPage.locator("#run-title").textContent(), "evidence-blocked");
    await linkedPage.close();
    await page.evaluate(() => scrollTo(0, 0));
    await page.screenshot({ path: join(root, "docs/evidence/ui-blocked.png"), fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    await page.screenshot({ path: join(root, "docs/evidence/ui-mobile.png"), fullPage: true });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.reload();
    await page.waitForSelector("#blocked:not([hidden])");
    assert.equal(await page.locator("#api-key").inputValue(), "");
    assert.equal(await page.evaluate(() => localStorage.length), 0);
    assert.deepEqual(await context.cookies(), []);
    assert.equal(await page.evaluate(() => Object.keys(sessionStorage).join(",")), "capstan.apiKey");
    assert.equal((await page.content()).includes(env.key), false);
    assert.equal(requests.some((request) => request.url.includes(env.key) || request.body?.includes(env.key)), false);
    assert.equal(requests.some((request) => request.method === "OPTIONS"), false);
    assert.equal(requests.some((request) => !request.url.startsWith(env.address)), false);
    // The mismatch may be on a history page that has not been loaded yet.
    await page.route("**/capstan.v1.ClientService/GetHistory", async (route) => {
      const response = await route.fetch();
      const body = await response.json();
      if (!route.request().postDataJSON().afterEventId || route.request().postDataJSON().afterEventId === "0") {
        body.events = body.events.slice(0, 4);
        body.more = true;
      }
      await route.fulfill({ response, json: body });
    });
    await page.reload();
    await page.waitForSelector("#blocked:not([hidden])");
    assert.equal(await page.locator("#event-5").count(), 0);
    await page.locator("#blocked h2 a").click();
    await page.waitForSelector("#event-5");
    assert.equal(new URL(page.url()).hash, "#run=evidence-blocked");
    await page.unrouteAll();
    // Older workers did not attach the D16 mismatch envelope.
    await page.route("**/capstan.v1.ClientService/*", async (route) => {
      const response = await route.fetch();
      const body = await response.json();
      if (body.run?.failure) delete body.run.failure.details;
      for (const event of body.events || []) {
        if (event.runBlocked?.failure) delete event.runBlocked.failure.details;
      }
      await route.fulfill({ response, json: body });
    });
    await page.reload();
    await page.waitForSelector("#blocked:not([hidden])");
    assert.equal(await page.locator("#blocked h2").textContent(), "Replay mismatch · event #14");
    assert.equal(await page.locator("#blocked h2 a").count(), 0);
    assert.match(await page.locator("#blocked").textContent(), /capstan resume 'evidence-blocked'/);
    await page.unrouteAll();
    await page.getByRole("button", { name: "Disconnect & clear key" }).click();
    assert.equal(await page.evaluate(() => sessionStorage.length), 0);
    assert.equal(await page.locator("#timeline .event").count(), 0);
    for (const name of readdirSync(env.logdir)) assert.equal(readFileSync(join(env.logdir, name), "utf8").includes(env.key), false, `key in ${name}`);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ check: "PASS", date: new Date().toISOString(), browser: await browser.version(), playwright: JSON.parse(readFileSync(packageFile)).version, runs: 13, checks: ["public assets", "JSON auth", "status and type filters", "paging", "collapsed payloads", "text-only injection", "blocked mismatch event 5 and timeline link", "mismatch link loads subsequent history pages", "mismatch link opens the correct event in a new tab", "old-history fallback event 14 and resume command", "mobile overflow", "session-only key", "reload", "disconnect", "no key in URLs, bodies, DOM or logs", "same origin; no CORS preflight"] }, null, 2));
  } finally { await browser.close(); }
});
