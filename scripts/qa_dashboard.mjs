import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
let chromium;
try {
  ({ chromium } = require("playwright"));
} catch (error) {
  throw new Error(
    "Dashboard browser QA is optional and requires the 'playwright' package.",
    { cause: error }
  );
}

const codeRoot = process.cwd();
const workspaceRoot = process.env.JOB_SEARCH_HOME
  ? path.resolve(process.env.JOB_SEARCH_HOME)
  : codeRoot;
const dashboardPath = process.env.JOB_SEARCH_DASHBOARD
  ? path.resolve(process.env.JOB_SEARCH_DASHBOARD)
  : path.join(workspaceRoot, "dashboard", "index.html");
const outputDir = path.join(workspaceRoot, "output", "playwright");
const chromePath =
  process.env.CHROME_PATH || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

if (!fs.existsSync(dashboardPath)) {
  throw new Error(`Dashboard is missing: ${dashboardPath}. Run jobctl.py rebuild first.`);
}
fs.mkdirSync(outputDir, { recursive: true });

const viewports = [
  { name: "desktop", width: 1440, height: 1000 },
  { name: "tablet", width: 900, height: 900 },
  { name: "mobile", width: 390, height: 844 },
];

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function runViewport(browser, viewport) {
  const page = await browser.newPage({ viewport });
  const browserErrors = [];
  page.on("console", (message) => {
    if (["error", "warning"].includes(message.type())) {
      browserErrors.push(`${message.type()}: ${message.text()}`);
    }
  });
  page.on("pageerror", (error) => browserErrors.push(`pageerror: ${error.message}`));

  await page.goto(pathToFileURL(dashboardPath).href);
  await page.screenshot({
    path: path.join(outputDir, `dashboard_${viewport.name}.png`),
    fullPage: viewport.name === "desktop",
  });

  const title = (await page.locator("h1").first().innerText()).trim();
  assert(title.length > 0, `${viewport.name}: dashboard title is empty`);
  assert((await page.locator(".kpi").count()) >= 8, `${viewport.name}: KPI cards are missing`);

  const dataShape = await page.evaluate(() => ({
    vacancies: Array.isArray(DATA?.vacancies),
    review: Array.isArray(DATA?.active_review),
    recent: Array.isArray(DATA?.recent),
    followups: Array.isArray(DATA?.followups),
    kpis: DATA?.kpis && typeof DATA.kpis === "object",
  }));
  assert(Object.values(dataShape).every(Boolean), `${viewport.name}: embedded DATA shape is invalid`);

  const tabs = await page.locator(".tabs button").evaluateAll((buttons) =>
    buttons.map((button) => button.getAttribute("data-tab"))
  );
  assert(
    ["review", "funnel", "today", "followups"].every((tab) => tabs.includes(tab)),
    `${viewport.name}: expected dashboard tabs are missing`
  );
  for (const tab of tabs) {
    await page.click(`button[data-tab='${tab}']`);
    assert(await page.locator(`#tab-${tab}`).isVisible(), `${viewport.name}: ${tab} tab failed`);
  }

  await page.fill("#search", "example-search-with-no-required-result");
  await page.click("#reset");
  assert((await page.inputValue("#search")) === "", `${viewport.name}: reset failed`);

  const layout = await page.evaluate(() => {
    const doc = document.documentElement;
    const tinyControls = [...document.querySelectorAll("button, input, select")].filter((element) => {
      if (element.offsetParent === null || element.matches('input[type="checkbox"]')) return false;
      const rect = element.getBoundingClientRect();
      return rect.width < 28 || rect.height < 28;
    }).length;
    return {
      clientWidth: doc.clientWidth,
      scrollWidth: doc.scrollWidth,
      tinyControls,
      bodyTextLength: document.body.innerText.length,
    };
  });
  assert(layout.bodyTextLength > 100, `${viewport.name}: page content is unexpectedly empty`);
  assert(layout.tinyControls === 0, `${viewport.name}: clipped controls detected`);
  assert(
    layout.scrollWidth <= layout.clientWidth + 2,
    `${viewport.name}: horizontal overflow ${layout.scrollWidth} > ${layout.clientWidth}`
  );
  assert(browserErrors.length === 0, `${viewport.name}: ${browserErrors.join("; ")}`);

  await page.close();
  return { viewport: viewport.name, title, layout };
}

const launchOptions = { headless: true };
if (fs.existsSync(chromePath)) launchOptions.executablePath = chromePath;
const browser = await chromium.launch(launchOptions);

try {
  const results = [];
  for (const viewport of viewports) results.push(await runViewport(browser, viewport));
  console.log(JSON.stringify({ ok: true, dashboardPath, results }, null, 2));
} finally {
  await browser.close();
}
