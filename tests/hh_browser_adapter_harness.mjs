import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath, pathToFileURL } from "node:url";
import { webcrypto } from "node:crypto";

const TESTS_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.dirname(TESTS_DIR);

export function loadFixture() {
  return JSON.parse(
    fs.readFileSync(path.join(TESTS_DIR, "fixtures", "hh_links_synthetic.json"), "utf8")
  );
}

class FakeCard {
  constructor(entry, link) {
    this.entry = entry;
    this.link = link;
    this.textContent = entry.text || "";
  }

  getAttribute(name) {
    if (name === "data-vacancy-id") return this.entry.card_vacancy_id || null;
    return null;
  }

  matches() {
    return false;
  }

  querySelector(selector) {
    if (
      selector.includes("serp-item__title") ||
      selector.includes("vacancy-serp__vacancy-title")
    ) {
      return this.link;
    }
    if (
      selector.includes("vacancy-employer") ||
      selector.includes("data-company-name")
    ) {
      return this.entry.company ? { textContent: this.entry.company } : null;
    }
    return null;
  }

  querySelectorAll(selector) {
    if (selector.includes("vacancyId=")) {
      return (this.entry.response_vacancy_ids || []).map((vacancyId) => ({
        href: `https://example.test/applicant/vacancy_response?vacancyId=${vacancyId}`,
        getClientRects: () => [{}],
      }));
    }
    return [];
  }

  getClientRects() {
    return [{}];
  }
}

class FakeLink {
  constructor(entry, baseUrl) {
    this.entry = entry;
    this.href = new URL(entry.href, baseUrl).href;
    this.textContent = entry.text || "";
    this.parentElement = this;
    this.card = new FakeCard(entry, this);
  }

  getAttribute(name) {
    if (name === "data-qa") return this.entry.data_qa || null;
    if (name === "data-vacancy-id") return null;
    return null;
  }

  closest(selector) {
    if (selector === "[data-vacancy-id]" && this.entry.card_vacancy_id) return this.card;
    if (
      (this.entry.card_vacancy_id || this.entry.in_card) &&
      [
        '[data-qa="vacancy-serp__vacancy"]',
        "article",
        "li",
      ].includes(selector)
    ) {
      return this.card;
    }
    return null;
  }

  matches() {
    return false;
  }

  querySelector() {
    return null;
  }

  querySelectorAll() {
    return [];
  }

  getClientRects() {
    return [{}];
  }
}

function selectorMatchesLink(selector, link) {
  if (selector.includes('data-qa="serp-item__title"')) {
    return link.entry.data_qa === "serp-item__title";
  }
  if (selector.includes('data-qa="vacancy-serp__vacancy-title"')) {
    return link.entry.data_qa === "vacancy-serp__vacancy-title";
  }
  if (selector.startsWith("[data-vacancy-id]")) {
    return Boolean(link.entry.card_vacancy_id);
  }
  if (selector.includes('href*="/vacancy"')) {
    return link.href.includes("/vacancy");
  }
  if (selector.includes("href*='/vacancy/'")) {
    return link.href.includes("/vacancy/");
  }
  return false;
}

function makeDocument(entrySequences, fixture, sourceKind, runtime) {
  const sequences = entrySequences.map((entries) =>
    entries.map((entry) => new FakeLink(entry, fixture.base_url))
  );
  const state = { sampleIndex: -1, links: sequences[0] || [] };
  const countNode = {
    textContent: String(runtime.sourceReportedCount ?? fixture.source_reported_count),
  };
  const paginationLinks = (runtime.paginationUrls || []).map((url) => ({
    href: new URL(url, runtime.baseUrl || fixture.base_url).href,
    getClientRects: () => [{}],
  }));
  const previousLink = runtime.previousUrl
    ? {
        href: new URL(runtime.previousUrl, runtime.baseUrl || fixture.base_url).href,
        getClientRects: () => [{}],
      }
    : null;
  const root = {
    scrollHeight: runtime.heightSequence?.[0] ?? 1000,
    scrollTop: 0,
    get innerText() {
      return state.links.map((link) => link.textContent).join(" ");
    },
    querySelector(selector) {
      for (const link of state.links) {
        if (selectorMatchesLink(selector, link)) return link;
      }
      return null;
    },
    querySelectorAll(selector) {
      return state.links.filter((link) => selectorMatchesLink(selector, link));
    },
    getClientRects() {
      return [{}];
    },
    scrollTo({ top }) {
      this.scrollTop = top;
    },
  };
  const documentElement = {
    scrollHeight: root.scrollHeight,
    scrollTop: 0,
  };
  const document = {
    body: root,
    documentElement,
    querySelector(selector) {
      if (selector.includes("vacancies-search-header")) return countNode;
      if (selector === "[data-search-session-id]") return null;
      if (previousLink && (selector.includes("pager-previous") || selector.includes('rel="prev"'))) {
        return previousLink;
      }
      if (
        paginationLinks.length &&
        (selector.includes('nav[aria-label="Pagination"]') ||
          selector.includes('nav[aria-label="Пагинация"]') ||
          selector.includes('data-qa="pager-block"'))
      ) {
        return paginationLinks[0];
      }
      if (runtime.blockerSelector && selector.includes(runtime.blockerSelector)) {
        return { getClientRects: () => [{}] };
      }
      if (selector === "main") return runtime.missingResultsRoot ? null : root;
      return root.querySelector(selector);
    },
    querySelectorAll(selector) {
      if (selector === "main") {
        if (runtime.missingResultsRoot) return [];
        return runtime.ambiguousResultsRoot ? [root, { ...root }] : [root];
      }
      if (runtime.loaderActive) {
        const isLoaderSelector =
          selector.includes("loader") ||
          selector.includes("aria-busy") ||
          selector.includes("progressbar") ||
          selector.includes("bloko-loading");
        if (isLoaderSelector) return [{ getClientRects: () => [{}] }];
      }
      if (
        selector.includes('nav[aria-label="Pagination"]') ||
        selector.includes('nav[aria-label="Пагинация"]') ||
        selector.includes('data-qa="pager-block"')
      ) {
        return paginationLinks;
      }
      return root.querySelectorAll(selector);
    },
  };
  return {
    document,
    advanceSample() {
      state.sampleIndex += 1;
      state.links = sequences[Math.min(state.sampleIndex, sequences.length - 1)] || [];
      const height =
        runtime.heightSequence?.[
          Math.min(state.sampleIndex, (runtime.heightSequence?.length || 1) - 1)
        ] ?? 1000;
      root.scrollHeight = height;
      documentElement.scrollHeight = height;
    },
  };
}

class FakeMutationObserver {
  observe() {}
  disconnect() {}
}

function installAdapter(entries, fixture, sourceKind, runtime = {}) {
  const location = new URL(runtime.baseUrl || fixture.base_url);
  const entrySequences = runtime.entrySequences || [entries];
  const fake = makeDocument(entrySequences, fixture, sourceKind, runtime);
  const context = {
    URL,
    Date,
    Set,
    crypto: webcrypto,
    location,
    document: fake.document,
    setTimeout(callback) {
      fake.advanceSample();
      callback();
      return 0;
    },
    scrollTo() {},
    getComputedStyle() {
      return { display: "block", visibility: "visible" };
    },
  };
  if (runtime.textEncoder !== false) {
    context.TextEncoder = TextEncoder;
  }
  if (runtime.restrictedPrimitives) {
    context.TextEncoder = undefined;
    context.Uint8Array = undefined;
    context.ArrayBuffer = undefined;
    context.DataView = undefined;
    context.crypto = undefined;
  }
  if (runtime.mutationObserver !== false) {
    context.MutationObserver = runtime.mutationObserverCtor || FakeMutationObserver;
  }
  if (runtime.unsuitableGlobalThis) {
    context.globalThis = Object.freeze({ unsuitable: true });
  }
  const adapterSource = fs.readFileSync(
    path.join(ROOT, "scripts", "hh_browser_adapter.js"),
    "utf8"
  );
  const adapter = vm.runInNewContext(adapterSource, context, {
    filename: "hh_browser_adapter.js",
  });
  return {
    adapter,
    links: context.document.querySelectorAll('a[href*="/vacancy"]'),
  };
}

export function classifyFixtureEntries(group = "success_links") {
  const fixture = loadFixture();
  const entries = fixture[group];
  const { adapter, links } = installAdapter(entries, fixture, "ordinary_search");
  return links.map((link) => {
    const classification = adapter.classifyVacancyLink(link);
    return {
      case: link.entry.case,
      kind: classification.kind,
      vacancy_id: classification.identity?.vacancy_id || "",
    };
  });
}

export async function captureFixture(
  group = "success_links",
  sourceKind = "ordinary_search",
  runtime = {}
) {
  const fixture = loadFixture();
  const { adapter } = installAdapter(fixture[group], fixture, sourceKind, runtime);
  const options = {
    queryFingerprint: "a".repeat(64),
    pageIndex: runtime.pageIndex ?? 0,
    pageSize: 100,
    stabilitySamples: 3,
    stabilityDelayMs: 750,
    stabilityTimeoutMs: 30000,
    maxScrollAttempts: 4,
  };
  return sourceKind === "personal_recommendations"
    ? adapter.capturePersonalRecommendations(options)
    : adapter.captureListPage(options);
}

export async function captureDetailFixture(runtime = {}) {
  const fixture = loadFixture();
  const { adapter } = installAdapter([], fixture, "ordinary_search", runtime);
  return adapter.captureVacancyDetail();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const group = process.argv[2] || "success_links";
  const sourceKind = process.argv[3] || "ordinary_search";
  try {
    process.stdout.write(
      `${JSON.stringify(await captureFixture(group, sourceKind), null, 2)}\n`
    );
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
