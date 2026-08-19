/*
 * Find Dream Job Engine — read-only HeadHunter DOM adapter.
 * Adapter contract: hh-dom-v1.0.0 / capture contract v1.
 *
 * Stable selectors are tried first (`data-qa`, vacancy data attributes, rel
 * pagination).  Documented fallbacks are limited to semantic vacancy links,
 * articles, headings, time elements, and ARIA loading state.  The adapter only
 * reads the visible DOM and scrolls to test stability.  It never clicks or
 * invokes application, message, archive, join, or other mutation controls.
 */
(function installFindDreamJobHHAdapter(global) {
  "use strict";

  const ADAPTER_VERSION = "hh-dom-v1.0.0";
  const CONTRACT_VERSION = 1;
  const VACANCY_LINK_SELECTORS = [
    '[data-qa="serp-item__title"][href*="/vacancy/"]',
    '[data-qa="vacancy-serp__vacancy-title"][href*="/vacancy/"]',
    '[data-vacancy-id] a[href*="/vacancy/"]',
    'a[href*="/vacancy/"]',
  ];
  const CARD_SELECTORS = [
    '[data-qa="vacancy-serp__vacancy"]',
    "[data-vacancy-id]",
    "article",
    "li",
  ];
  const TITLE_SELECTORS = [
    '[data-qa="serp-item__title"]',
    '[data-qa="vacancy-serp__vacancy-title"]',
    "h2 a[href*='/vacancy/']",
    "h3 a[href*='/vacancy/']",
  ];
  const COMPANY_SELECTORS = [
    '[data-qa="vacancy-serp__vacancy-employer"]',
    '[data-qa="vacancy-serp__vacancy-employer-text"]',
    "[data-company-name]",
  ];
  const PUBLICATION_SELECTORS = [
    '[data-qa="vacancy-serp__vacancy-published-text"]',
    "time[datetime]",
    "time",
  ];
  const LOADER_SELECTORS = [
    '[data-qa*="loader"]',
    '[aria-busy="true"]',
    '[role="progressbar"]',
    ".bloko-loading",
  ];
  const NEXT_SELECTORS = [
    'a[rel="next"]',
    '[data-qa="pager-next"] a',
    'a[data-qa="pager-next"]',
  ];
  const PREVIOUS_SELECTORS = [
    'a[rel="prev"]',
    '[data-qa="pager-previous"] a',
    'a[data-qa="pager-previous"]',
  ];
  const RESULT_COUNT_SELECTORS = [
    '[data-qa="vacancies-search-header"]',
    '[data-qa="vacancies-search-header-title"]',
    '[data-qa="search-result-count"]',
  ];

  const wait = (milliseconds) =>
    new Promise((resolve) => global.setTimeout(resolve, Math.max(0, milliseconds)));

  const compactText = (value) => String(value || "").replace(/\s+/g, " ").trim();

  function first(root, selectors) {
    for (const selector of selectors) {
      const node = root.querySelector(selector);
      if (node) return node;
    }
    return null;
  }

  function allUnique(root, selectors) {
    const nodes = [];
    const seen = new Set();
    for (const selector of selectors) {
      for (const node of root.querySelectorAll(selector)) {
        if (!seen.has(node)) {
          seen.add(node);
          nodes.push(node);
        }
      }
    }
    return nodes;
  }

  async function sha256(value) {
    const data = new TextEncoder().encode(String(value));
    const digest = await global.crypto.subtle.digest("SHA-256", data);
    return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
  }

  function parseVacancyIdentity(link) {
    const dataId = compactText(
      link.getAttribute("data-vacancy-id") ||
        link.closest("[data-vacancy-id]")?.getAttribute("data-vacancy-id")
    );
    const url = new URL(link.href, global.location.href);
    if (url.origin !== global.location.origin) return null;
    const match = url.pathname.match(/\/vacancy\/([0-9]{1,32})(?:\/|$)/);
    const pathId = match ? match[1].replace(/^0+(?=\d)/, "") : "";
    const normalizedDataId = dataId.replace(/^hh:/i, "").replace(/^0+(?=\d)/, "");
    if (!pathId || (normalizedDataId && normalizedDataId !== pathId)) {
      return null;
    }
    return {
      vacancy_id: pathId,
      canonical_url: `${url.protocol}//${url.host}/vacancy/${pathId}`,
    };
  }

  function closestCard(link) {
    for (const selector of CARD_SELECTORS) {
      const node = link.closest(selector);
      if (node) return node;
    }
    return link.parentElement || link;
  }

  function textFrom(root, selectors) {
    const node = first(root, selectors);
    return compactText(node?.textContent || "");
  }

  function extractCards() {
    const links = allUnique(document, VACANCY_LINK_SELECTORS);
    const cards = [];
    const malformed = [];
    links.forEach((link, index) => {
      const identity = parseVacancyIdentity(link);
      if (!identity) {
        malformed.push({ position: index + 1, reason: "vacancy_link_without_confirmed_numeric_identity" });
        return;
      }
      const card = closestCard(link);
      const title = textFrom(card, TITLE_SELECTORS) || compactText(link.textContent);
      const company = textFrom(card, COMPANY_SELECTORS);
      const publicationNode = first(card, PUBLICATION_SELECTORS);
      const publication = compactText(
        publicationNode?.getAttribute("datetime") || publicationNode?.textContent || ""
      );
      const markerText = compactText(card.textContent).toLocaleLowerCase();
      const promoted =
        card.matches('[data-promoted="true"], [data-qa*="premium"]') ||
        /(?:promoted|реклама|продвигаемая)/i.test(markerText);
      const pinned =
        card.matches('[data-pinned="true"], [data-qa*="pinned"]') ||
        /(?:pinned|закреплен)/i.test(markerText);
      cards.push({
        ...identity,
        title: title.slice(0, 1024),
        company: company.slice(0, 1024),
        position: index + 1,
        publication_evidence: publication.slice(0, 1024),
        promoted: Boolean(promoted),
        pinned: Boolean(pinned),
      });
    });
    return { cards, malformed };
  }

  function detectBlocker() {
    const body = compactText(document.body?.innerText || "").toLocaleLowerCase();
    const checks = [
      {
        type: "captcha",
        selector: '[data-qa*="captcha"], iframe[src*="captcha"], input[name*="captcha"]',
        pattern: /(?:captcha|капч|подтвердите,? что вы не робот|i am not a robot)/i,
      },
      {
        type: "login",
        selector: '[data-qa="login"], form[action*="login"], input[type="password"]',
        pattern: /(?:войдите|авторизуйтесь|sign in|log in).{0,80}(?:продолж|continue|просмотр|view)/i,
      },
      {
        type: "access_denied",
        selector: '[data-qa*="access-denied"], [role="alert"]',
        pattern: /(?:доступ ограничен|доступ запрещен|access denied|request blocked|нет доступа)/i,
      },
    ];
    for (const check of checks) {
      if (document.querySelector(check.selector) || check.pattern.test(body)) {
        return { type: check.type, evidence: [`visible_dom:${check.type}`] };
      }
    }
    return { type: "none", evidence: [] };
  }

  function activeLoader() {
    return LOADER_SELECTORS.some((selector) =>
      Array.from(document.querySelectorAll(selector)).some((node) => {
        const style = global.getComputedStyle(node);
        return style.display !== "none" && style.visibility !== "hidden" && node.getClientRects().length > 0;
      })
    );
  }

  function sourceReportedCount() {
    const node = first(document, RESULT_COUNT_SELECTORS);
    if (!node) return null;
    const text = compactText(node.textContent).replace(/[\u00a0\s]/g, "");
    const matches = text.match(/[0-9][0-9.,]*/g);
    if (!matches?.length) return null;
    const value = Number(matches[0].replace(/[.,]/g, ""));
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
  }

  function pageIndexFromUrl(urlValue) {
    const url = new URL(urlValue, global.location.href);
    const raw = url.searchParams.get("page");
    const parsed = raw === null ? 0 : Number(raw);
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
  }

  function navigationEvidence(pageIndex) {
    const normalize = (node) => {
      if (!node) return { present: false, page_index: null, url: "" };
      const url = new URL(node.href, global.location.href).href;
      return { present: true, page_index: pageIndexFromUrl(url), url };
    };
    const previous = normalize(first(document, PREVIOUS_SELECTORS));
    const next = normalize(first(document, NEXT_SELECTORS));
    const previousConsistent =
      !previous.present || previous.page_index === null || previous.page_index === pageIndex - 1;
    const nextConsistent = !next.present || next.page_index === null || next.page_index === pageIndex + 1;
    return { previous, next, consistent: previousConsistent && nextConsistent };
  }

  function orderingEvidence(cards) {
    const positions = cards.map((card) => Number(card.position));
    const monotonic = positions.every((value, index) => value === index + 1);
    const publication = cards.map((card) => card.publication_evidence).filter(Boolean);
    return {
      kind: publication.length ? "source_position_with_publication" : "source_position",
      monotonic,
      newest_publication: publication[0] || "",
      oldest_publication: publication[publication.length - 1] || "",
      evidence: monotonic ? "visible source position order" : "inconsistent visible position order",
    };
  }

  function sourceCountDriftWarning(sourceCount, uniqueCount, pageIndex, pageSize) {
    if (sourceCount === null) return [];
    const expected = Math.max(0, Math.min(pageSize, sourceCount - pageIndex * pageSize));
    if (expected === uniqueCount) return [];
    return [
      {
        code: "source_reported_count_drift",
        source_expected_page_count: expected,
        canonical_unique_count: uniqueCount,
      },
    ];
  }

  async function sessionEvidence(sourceKind, queryFingerprint) {
    const url = new URL(global.location.href);
    const exposed =
      url.searchParams.get("searchSessionId") ||
      url.searchParams.get("search_session_id") ||
      document.querySelector("[data-search-session-id]")?.getAttribute("data-search-session-id") ||
      "";
    if (compactText(exposed)) {
      return {
        session_id_state: "exposed",
        search_session_id: compactText(exposed).slice(0, 512),
        alternative_capture_session_fingerprint: "",
        evidence: ["visible_dom:search_session_id"],
      };
    }
    const stableUrl = new URL(url.href);
    stableUrl.searchParams.delete("page");
    stableUrl.searchParams.delete("searchSessionId");
    stableUrl.searchParams.delete("search_session_id");
    stableUrl.hash = "";
    return {
      session_id_state: "not_exposed",
      search_session_id: "",
      alternative_capture_session_fingerprint: await sha256(
        ["hh-capture-session-v1", sourceKind, stableUrl.origin, stableUrl.pathname, stableUrl.search, queryFingerprint].join("|")
      ),
      evidence: ["visible_dom:no_session_id_selector_or_url_parameter"],
    };
  }

  function relevantMutation(mutation) {
    const nodes = [mutation.target, ...Array.from(mutation.addedNodes || []), ...Array.from(mutation.removedNodes || [])];
    return nodes.some((node) => {
      if (!(node instanceof Element)) return false;
      return node.matches?.('a[href*="/vacancy/"], [data-vacancy-id]') ||
        Boolean(node.querySelector?.('a[href*="/vacancy/"], [data-vacancy-id]'));
    });
  }

  async function stabilityProtocol(options) {
    const sampleCount = Math.max(2, Number(options.stabilitySamples || 3));
    const delayMs = Math.max(0, Number(options.stabilityDelayMs ?? 750));
    const maxAttempts = Math.max(sampleCount + 1, Number(options.maxScrollAttempts || 25));
    let stableWindow = [];
    let finalBottomMutationCount = -1;
    let attempts = 0;
    let relevantMutationCount = 0;
    const observer = new MutationObserver((mutations) => {
      relevantMutationCount += mutations.filter(relevantMutation).length;
    });
    observer.observe(document.documentElement, { childList: true, subtree: true, attributes: true });
    try {
      for (let index = 0; index < maxAttempts; index += 1) {
        attempts += 1;
        global.scrollTo({
          top: Math.max(document.documentElement.scrollHeight, document.body?.scrollHeight || 0),
          behavior: "auto",
        });
        await wait(delayMs);
        const { cards } = extractCards();
        const ids = [...new Set(cards.map((card) => `hh:${card.vacancy_id}`))].sort();
        const sample = {
          canonical_id_set_hash: await sha256(JSON.stringify(ids)),
          scroll_height: Math.max(document.documentElement.scrollHeight, document.body?.scrollHeight || 0),
          loader_active: activeLoader(),
          mutation_count: relevantMutationCount,
        };
        const previous = stableWindow[stableWindow.length - 1];
        if (
          !sample.loader_active &&
          previous &&
          previous.canonical_id_set_hash === sample.canonical_id_set_hash &&
          previous.scroll_height === sample.scroll_height
        ) {
          stableWindow.push(sample);
        } else {
          stableWindow = [sample];
        }
        if (stableWindow.length < sampleCount) continue;

        // The final independent bottom attempt is measured separately. Any
        // relevant mutation, ID-set change, height change, or loader activity
        // invalidates the candidate window and collection continues.
        relevantMutationCount = 0;
        global.scrollTo({
          top: Math.max(document.documentElement.scrollHeight, document.body?.scrollHeight || 0),
          behavior: "auto",
        });
        await wait(delayMs);
        const finalCards = extractCards().cards;
        const finalIds = [...new Set(finalCards.map((card) => `hh:${card.vacancy_id}`))].sort();
        const finalSample = {
          canonical_id_set_hash: await sha256(JSON.stringify(finalIds)),
          scroll_height: Math.max(document.documentElement.scrollHeight, document.body?.scrollHeight || 0),
          loader_active: activeLoader(),
          mutation_count: relevantMutationCount,
        };
        finalBottomMutationCount = relevantMutationCount;
        if (
          !finalSample.loader_active &&
          finalBottomMutationCount === 0 &&
          finalSample.canonical_id_set_hash === sample.canonical_id_set_hash &&
          finalSample.scroll_height === sample.scroll_height
        ) {
          stableWindow = [...stableWindow.slice(-(sampleCount - 1)), finalSample];
          break;
        }
        stableWindow = [finalSample];
      }
    } finally {
      observer.disconnect();
    }
    const navigation = navigationEvidence(Number(options.pageIndex));
    return {
      samples: stableWindow.slice(-sampleCount),
      attempts,
      max_attempts: maxAttempts,
      bottom_scroll_attempted: true,
      no_relevant_dom_mutation_after_bottom: finalBottomMutationCount === 0,
      end_of_list_evidence:
        !navigation.next.present || Boolean(document.querySelector('[data-qa="search-end"], [data-end-of-list="true"]')),
    };
  }

  async function captureListPage(options = {}) {
    const sourceKind = String(options.sourceKind || "ordinary_search");
    if (!["ordinary_search", "personal_recommendations"].includes(sourceKind)) {
      throw new Error("sourceKind must be ordinary_search or personal_recommendations");
    }
    const queryFingerprint = String(options.queryFingerprint || "").toLowerCase();
    if (!/^[0-9a-f]{64}$/.test(queryFingerprint)) {
      throw new Error("queryFingerprint must be a lowercase SHA-256 value");
    }
    const pageIndex =
      options.pageIndex === undefined ? pageIndexFromUrl(global.location.href) : Number(options.pageIndex);
    if (!Number.isSafeInteger(pageIndex) || pageIndex < 0) {
      throw new Error("pageIndex must be a non-negative integer");
    }
    const pageSize = Math.max(1, Math.min(100, Number(options.pageSize || 100)));
    const blocker = detectBlocker();
    const stability = await stabilityProtocol({ ...options, pageIndex });
    const { cards, malformed } = extractCards();
    if (malformed.length) {
      throw new Error(`Missing required vacancy identity for ${malformed.length} visible link(s)`);
    }
    const canonicalIds = [...new Set(cards.map((card) => `hh:${card.vacancy_id}`))].sort();
    const count = sourceReportedCount();
    const warnings = sourceCountDriftWarning(count, canonicalIds.length, pageIndex, pageSize);
    return {
      capture_contract: "hh_page_capture_v1",
      contract_version: CONTRACT_VERSION,
      adapter_version: ADAPTER_VERSION,
      source_kind: sourceKind,
      canonical_url: new URL(global.location.href).href,
      query_fingerprint: queryFingerprint,
      page_index: pageIndex,
      captured_at: new Date().toISOString(),
      source_reported_result_count: count,
      navigation: navigationEvidence(pageIndex),
      ordering: orderingEvidence(cards),
      cards,
      loader: { active: activeLoader(), evidence: activeLoader() ? ["visible_dom:loader"] : [] },
      blocker,
      stability,
      canonical_id_set_hash: await sha256(JSON.stringify(canonicalIds)),
      session: await sessionEvidence(sourceKind, queryFingerprint),
      warnings,
    };
  }

  async function capturePersonalRecommendations(options = {}) {
    return captureListPage({ ...options, sourceKind: "personal_recommendations" });
  }

  function detailField(selectors) {
    return textFrom(document, selectors);
  }

  async function captureVacancyDetail() {
    const blocker = detectBlocker();
    const canonical = parseVacancyIdentity({
      href: global.location.href,
      getAttribute: () => "",
      closest: () => null,
    });
    if (!canonical) throw new Error("The visible URL does not contain a confirmed vacancy identity");
    return {
      capture_contract: "hh_detail_capture_v1",
      contract_version: CONTRACT_VERSION,
      adapter_version: ADAPTER_VERSION,
      captured_at: new Date().toISOString(),
      vacancy_id: canonical.vacancy_id,
      canonical_url: canonical.canonical_url,
      loader: { active: activeLoader() },
      blocker,
      fields: {
        title: detailField(['[data-qa="vacancy-title"]', "h1"]),
        company: detailField(['[data-qa="vacancy-company-name"]', '[data-qa="vacancy-company"]']),
        description: detailField(['[data-qa="vacancy-description"]', "main article"]),
        salary: detailField(['[data-qa="vacancy-salary"]']),
        location: detailField(['[data-qa="vacancy-view-location"]', '[data-qa="vacancy-view-raw-address"]']),
        schedule: detailField(['[data-qa="vacancy-view-employment-mode"]']),
        employment_format: detailField(['[data-qa="vacancy-view-employment-mode"]']),
        requirements: detailField(['[data-qa="vacancy-description"]']),
        experience: detailField(['[data-qa="vacancy-experience"]']),
        skills: allUnique(document, ['[data-qa="skills-element"]']).map((node) => compactText(node.textContent)).filter(Boolean),
        publication_evidence: detailField(['[data-qa="vacancy-creation-time"]', "time[datetime]"]),
      },
      source_evidence: ["visible_dom:vacancy_detail"],
    };
  }

  global.FindDreamJobHHAdapter = Object.freeze({
    version: ADAPTER_VERSION,
    contractVersion: CONTRACT_VERSION,
    selectors: Object.freeze({
      vacancyLinks: [...VACANCY_LINK_SELECTORS],
      cards: [...CARD_SELECTORS],
      loaders: [...LOADER_SELECTORS],
      next: [...NEXT_SELECTORS],
      previous: [...PREVIOUS_SELECTORS],
    }),
    detectBlocker,
    captureListPage,
    capturePersonalRecommendations,
    captureVacancyDetail,
  });
})(globalThis);
