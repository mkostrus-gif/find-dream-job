/*
 * Find Dream Job Engine — read-only HeadHunter DOM adapter.
 * Adapter contract: hh-dom-v1.0.2 / capture contract v1.
 *
 * Stable selectors are tried first (`data-qa`, vacancy data attributes, rel
 * pagination).  Documented fallbacks are limited to semantic vacancy links,
 * articles, headings, time elements, and ARIA loading state.  The adapter only
 * reads the visible DOM and scrolls to test stability.  It never clicks or
 * invokes application, message, archive, join, or other mutation controls.
 */
(() => {
  "use strict";

  const ADAPTER_VERSION = "hh-dom-v1.0.2";
  const CONTRACT_VERSION = 1;
  const VACANCY_LINK_SELECTORS = [
    '[data-qa="serp-item__title"][href]',
    '[data-qa="vacancy-serp__vacancy-title"][href]',
    '[data-vacancy-id] a[href]',
    'a[href*="/vacancy"]',
  ];
  const VACANCY_TITLE_MARKERS = new Set([
    "serp-item__title",
    "vacancy-serp__vacancy-title",
  ]);
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
  const RESULTS_ROOT_SELECTORS = [
    '[data-qa="vacancy-serp__results"]',
    '[data-qa="vacancy-serp__results-list"]',
    '[data-qa="search-results"]',
    "main",
  ];

  const wait = (milliseconds) =>
    new Promise((resolve) => setTimeout(resolve, Math.max(0, milliseconds)));

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

  function isVisible(node) {
    if (!node) return false;
    const style = getComputedStyle(node);
    return (
      style.display !== "none" &&
      style.visibility !== "hidden" &&
      (typeof node.getClientRects !== "function" || node.getClientRects().length > 0)
    );
  }

  function resultsRootEvidence() {
    for (const selector of RESULTS_ROOT_SELECTORS) {
      const candidates = Array.from(document.querySelectorAll(selector)).filter(isVisible);
      if (!candidates.length) continue;
      if (candidates.length !== 1) {
        throw new Error(`Visible results root is ambiguous for selector ${selector}`);
      }
      return { root: candidates[0], selector };
    }
    throw new Error("Visible results root is missing");
  }

  function utf8Bytes(value) {
    const text = String(value);
    const bytes = [];
    for (let index = 0; index < text.length; index += 1) {
      let codePoint = text.codePointAt(index);
      const codeUnit = text.charCodeAt(index);
      if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
        const next = text.charCodeAt(index + 1);
        if (next >= 0xdc00 && next <= 0xdfff) index += 1;
        else codePoint = 0xfffd;
      } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
        codePoint = 0xfffd;
      }
      if (codePoint <= 0x7f) {
        bytes.push(codePoint);
      } else if (codePoint <= 0x7ff) {
        bytes.push(0xc0 | (codePoint >> 6), 0x80 | (codePoint & 0x3f));
      } else if (codePoint <= 0xffff) {
        bytes.push(
          0xe0 | (codePoint >> 12),
          0x80 | ((codePoint >> 6) & 0x3f),
          0x80 | (codePoint & 0x3f)
        );
      } else {
        bytes.push(
          0xf0 | (codePoint >> 18),
          0x80 | ((codePoint >> 12) & 0x3f),
          0x80 | ((codePoint >> 6) & 0x3f),
          0x80 | (codePoint & 0x3f)
        );
      }
    }
    return bytes;
  }

  function rotateRight(value, amount) {
    return (value >>> amount) | (value << (32 - amount));
  }

  function sha256WithoutWebCrypto(value) {
    const constants = [
      0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
      0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
      0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
      0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
      0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
      0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
      0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
      0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
      0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
      0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
      0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
      0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
      0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
      0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
      0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
      0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];
    const hash = [
      0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
      0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    const bytes = utf8Bytes(value);
    const byteLength = bytes.length;
    bytes.push(0x80);
    while (bytes.length % 64 !== 56) bytes.push(0);
    const bitLengthHigh = Math.floor(byteLength / 0x20000000) >>> 0;
    const bitLengthLow = (byteLength << 3) >>> 0;
    for (let shift = 24; shift >= 0; shift -= 8) {
      bytes.push((bitLengthHigh >>> shift) & 0xff);
    }
    for (let shift = 24; shift >= 0; shift -= 8) {
      bytes.push((bitLengthLow >>> shift) & 0xff);
    }

    const words = new Array(64).fill(0);
    for (let offset = 0; offset < bytes.length; offset += 64) {
      for (let index = 0; index < 16; index += 1) {
        const cursor = offset + index * 4;
        words[index] = (
          (bytes[cursor] << 24) |
          (bytes[cursor + 1] << 16) |
          (bytes[cursor + 2] << 8) |
          bytes[cursor + 3]
        ) >>> 0;
      }
      for (let index = 16; index < 64; index += 1) {
        const sigma0 =
          rotateRight(words[index - 15], 7) ^
          rotateRight(words[index - 15], 18) ^
          (words[index - 15] >>> 3);
        const sigma1 =
          rotateRight(words[index - 2], 17) ^
          rotateRight(words[index - 2], 19) ^
          (words[index - 2] >>> 10);
        words[index] = (
          words[index - 16] + sigma0 + words[index - 7] + sigma1
        ) >>> 0;
      }

      const working = hash.slice();
      for (let index = 0; index < 64; index += 1) {
        const choice =
          (working[4] & working[5]) ^ (~working[4] & working[6]);
        const majority =
          (working[0] & working[1]) ^
          (working[0] & working[2]) ^
          (working[1] & working[2]);
        const sum1 =
          rotateRight(working[4], 6) ^
          rotateRight(working[4], 11) ^
          rotateRight(working[4], 25);
        const sum0 =
          rotateRight(working[0], 2) ^
          rotateRight(working[0], 13) ^
          rotateRight(working[0], 22);
        const temporary1 = (
          working[7] + sum1 + choice + constants[index] + words[index]
        ) >>> 0;
        const temporary2 = (sum0 + majority) >>> 0;
        working[7] = working[6];
        working[6] = working[5];
        working[5] = working[4];
        working[4] = (working[3] + temporary1) >>> 0;
        working[3] = working[2];
        working[2] = working[1];
        working[1] = working[0];
        working[0] = (temporary1 + temporary2) >>> 0;
      }
      for (let index = 0; index < 8; index += 1) {
        hash[index] = (hash[index] + working[index]) >>> 0;
      }
    }
    return hash.map((word) => word.toString(16).padStart(8, "0")).join("");
  }

  async function sha256(value) {
    if (
      typeof crypto === "object" &&
      crypto &&
      typeof crypto.subtle?.digest === "function" &&
      typeof Uint8Array === "function"
    ) {
      const data =
        typeof TextEncoder === "function"
          ? new TextEncoder().encode(String(value))
          : new Uint8Array(utf8Bytes(value));
      const digest = await crypto.subtle.digest("SHA-256", data);
      return Array.from(
        new Uint8Array(digest),
        (byte) => byte.toString(16).padStart(2, "0")
      ).join("");
    }
    return sha256WithoutWebCrypto(value);
  }

  function parseVacancyIdentity(link) {
    const dataId = compactText(
      link.getAttribute("data-vacancy-id") ||
        link.closest("[data-vacancy-id]")?.getAttribute("data-vacancy-id")
    );
    const url = new URL(link.href, location.href);
    if (url.origin !== location.origin) return null;
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

  function parseSponsoredVacancyIdentity(link, url) {
    if (
      url.protocol !== "https:" ||
      url.hostname !== "adsrv.hh.ru" ||
      url.pathname !== "/click" ||
      url.searchParams.get("clickType") !== "link_to_vacancy"
    ) {
      return null;
    }
    const card = closestCard(link);
    const ids = new Set();
    const cardDataId = compactText(card?.getAttribute?.("data-vacancy-id"))
      .replace(/^hh:/i, "")
      .replace(/^0+(?=\d)/, "");
    if (/^[0-9]{1,32}$/.test(cardDataId)) ids.add(cardDataId);
    for (const candidate of card?.querySelectorAll?.('a[href*="vacancyId="]') || []) {
      if (!isVisible(candidate)) continue;
      let responseUrl;
      try {
        responseUrl = new URL(candidate.href, location.href);
      } catch (_error) {
        continue;
      }
      if (
        responseUrl.origin !== location.origin ||
        responseUrl.pathname !== "/applicant/vacancy_response"
      ) {
        continue;
      }
      const rawId = compactText(responseUrl.searchParams.get("vacancyId"))
        .replace(/^0+(?=\d)/, "");
      if (/^[0-9]{1,32}$/.test(rawId)) ids.add(rawId);
    }
    if (ids.size !== 1) return null;
    const [vacancyId] = ids;
    return {
      vacancy_id: vacancyId,
      canonical_url: `${location.protocol}//${location.host}/vacancy/${vacancyId}`,
    };
  }

  function classifyVacancyLink(link) {
    let url;
    try {
      url = new URL(link.href, location.href);
    } catch (_error) {
      return {
        kind: "malformed",
        reason: "vacancy_link_without_confirmed_numeric_identity",
      };
    }
    const semanticTitle = VACANCY_TITLE_MARKERS.has(
      compactText(link.getAttribute("data-qa"))
    );
    const vacancyContainer = Boolean(link.closest("[data-vacancy-id]"));
    const internalVacancyPath =
      url.origin === location.origin && /^\/vacancy(?:\/|$)/.test(url.pathname);

    if (
      url.origin === location.origin &&
      /^\/search\/vacancy(?:\/|$)/.test(url.pathname)
    ) {
      return { kind: "ignored", reason: "search_navigation" };
    }

    // Navigation such as /search/vacancy/map contains the word "vacancy" but
    // is not a vacancy card. A semantic title/container remains fail-closed if
    // its URL is malformed or its identity conflicts with visible card data.
    if (!semanticTitle && !vacancyContainer && !internalVacancyPath) {
      return { kind: "ignored", reason: "non_vacancy_navigation" };
    }
    const identity =
      parseVacancyIdentity(link) ||
      (semanticTitle ? parseSponsoredVacancyIdentity(link, url) : null);
    if (!identity) {
      return {
        kind: "malformed",
        reason: "vacancy_link_without_confirmed_numeric_identity",
      };
    }
    return {
      kind: "vacancy",
      identity,
      sponsored_redirect: url.hostname === "adsrv.hh.ru",
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

  function extractCards(root = document) {
    const links = allUnique(root, VACANCY_LINK_SELECTORS);
    const cards = [];
    const malformed = [];
    let vacancyPosition = 0;
    links.forEach((link) => {
      const classification = classifyVacancyLink(link);
      if (classification.kind === "ignored") return;
      vacancyPosition += 1;
      if (classification.kind === "malformed") {
        malformed.push({ position: vacancyPosition, reason: classification.reason });
        return;
      }
      const identity = classification.identity;
      const card = closestCard(link);
      const title = textFrom(card, TITLE_SELECTORS) || compactText(link.textContent);
      const company = textFrom(card, COMPANY_SELECTORS);
      const publicationNode = first(card, PUBLICATION_SELECTORS);
      const publication = compactText(
        publicationNode?.getAttribute("datetime") || publicationNode?.textContent || ""
      );
      const markerText = compactText(card.textContent).toLocaleLowerCase();
      const promoted =
        classification.sponsored_redirect ||
        card.matches('[data-promoted="true"], [data-qa*="premium"]') ||
        /(?:promoted|реклама|продвигаемая)/i.test(markerText);
      const pinned =
        card.matches('[data-pinned="true"], [data-qa*="pinned"]') ||
        /(?:pinned|закреплен)/i.test(markerText);
      cards.push({
        ...identity,
        title: title.slice(0, 1024),
        company: company.slice(0, 1024),
        position: vacancyPosition,
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
      {
        type: "error",
        selector: '[data-qa*="error"], [data-error-state="true"]',
        pattern: /(?:что-то пошло не так|произошла ошибка|something went wrong|service unavailable)/i,
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
      Array.from(document.querySelectorAll(selector)).some(isVisible)
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
    const url = new URL(urlValue, location.href);
    const raw = url.searchParams.get("page");
    const parsed = raw === null ? 0 : Number(raw);
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
  }

  function exactVisiblePaginationLink(targetPageIndex) {
    const selectors = [
      'nav[aria-label="Pagination"] a[href*="page="]',
      'nav[aria-label="Пагинация"] a[href*="page="]',
      '[data-qa="pager-block"] a[href*="page="]',
    ];
    const byUrl = new Map();
    for (const selector of selectors) {
      for (const node of Array.from(document.querySelectorAll(selector))) {
        if (!isVisible(node)) continue;
        const url = new URL(node.href, location.href).href;
        if (pageIndexFromUrl(url) === targetPageIndex) byUrl.set(url, node);
      }
    }
    return byUrl.size === 1 ? Array.from(byUrl.values())[0] : null;
  }

  function navigationEvidence(pageIndex) {
    const normalize = (node) => {
      if (!node) return { present: false, page_index: null, url: "" };
      const url = new URL(node.href, location.href).href;
      return { present: true, page_index: pageIndexFromUrl(url), url };
    };
    let previous = normalize(first(document, PREVIOUS_SELECTORS));
    const exactPrevious = normalize(exactVisiblePaginationLink(pageIndex - 1));
    if (
      !previous.present ||
      (previous.page_index !== null && previous.page_index !== pageIndex - 1)
    ) {
      if (exactPrevious.present) previous = exactPrevious;
    }
    let next = normalize(first(document, NEXT_SELECTORS));
    const exactNext = normalize(exactVisiblePaginationLink(pageIndex + 1));
    if (!next.present || (next.page_index !== null && next.page_index !== pageIndex + 1)) {
      if (exactNext.present) next = exactNext;
    }
    const visibleCurrentPageIndex = pageIndexFromUrl(location.href);
    if (!previous.present && pageIndex > 0 && visibleCurrentPageIndex === pageIndex) {
      const previousUrl = new URL(location.href);
      previousUrl.searchParams.set("page", String(pageIndex - 1));
      previous = {
        present: true,
        page_index: pageIndex - 1,
        url: previousUrl.href,
      };
    }
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
    const url = new URL(location.href);
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
      if (!node || typeof node.matches !== "function") return false;
      return node.matches?.('a[href*="/vacancy/"], [data-vacancy-id]') ||
        Boolean(node.querySelector?.('a[href*="/vacancy/"], [data-vacancy-id]'));
    });
  }

  function scrollHeightFor(root) {
    return Math.max(
      Number(root?.scrollHeight || 0),
      Number(document.documentElement?.scrollHeight || 0),
      Number(document.body?.scrollHeight || 0)
    );
  }

  function scrollPositionFor(root) {
    return Math.max(
      Number(root?.scrollTop || 0),
      Number(document.documentElement?.scrollTop || 0),
      Number(document.body?.scrollTop || 0),
      Number(typeof scrollY === "number" ? scrollY : 0)
    );
  }

  function scrollToVisibleBottom(root) {
    const top = scrollHeightFor(root);
    if (
      root &&
      root !== document.body &&
      root !== document.documentElement &&
      typeof root.scrollTo === "function"
    ) {
      root.scrollTo({ top, behavior: "auto" });
      return;
    }
    if (typeof scrollTo === "function") {
      scrollTo({ top, behavior: "auto" });
      return;
    }
    if (document.documentElement) document.documentElement.scrollTop = top;
    if (document.body) document.body.scrollTop = top;
  }

  function createUsableMutationObserver(root, onMutations) {
    if (typeof MutationObserver !== "function") return null;
    let observer = null;
    try {
      observer = new MutationObserver(onMutations);
      if (!observer || typeof observer.observe !== "function" || typeof observer.disconnect !== "function") {
        return null;
      }
      observer.observe(root, { childList: true, subtree: true, attributes: true });
      return observer;
    } catch (_error) {
      try {
        observer?.disconnect?.();
      } catch (_disconnectError) {
        // An unusable observer is treated as unavailable; timer sampling remains honest.
      }
      return null;
    }
  }

  async function visibleDomSample(root, index, startedAt, mutationCount) {
    const extracted = extractCards(root);
    if (extracted.malformed.length) {
      throw new Error(
        `Missing required vacancy identity for ${extracted.malformed.length} visible link(s)`
      );
    }
    const orderedIds = extracted.cards.map((card) => `hh:${card.vacancy_id}`);
    const uniqueIds = [...new Set(orderedIds)].sort();
    return {
      sample_index: index,
      sampled_at: new Date().toISOString(),
      relative_offset_ms: Math.max(0, Date.now() - startedAt),
      canonical_ordered_ids: orderedIds,
      canonical_ordered_id_hash: await sha256(JSON.stringify(orderedIds)),
      canonical_id_set_hash: await sha256(JSON.stringify(uniqueIds)),
      visible_card_count: extracted.cards.length,
      scroll_height: scrollHeightFor(root),
      scroll_position: scrollPositionFor(root),
      maximum_observed_card_position: extracted.cards.length
        ? Math.max(...extracted.cards.map((card) => Number(card.position) || 0))
        : null,
      loader_active: activeLoader(),
      mutation_count: mutationCount,
    };
  }

  function samplesMatch(left, right) {
    return Boolean(
      left &&
      right &&
      !left.loader_active &&
      !right.loader_active &&
      left.canonical_ordered_id_hash === right.canonical_ordered_id_hash &&
      left.canonical_id_set_hash === right.canonical_id_set_hash &&
      left.visible_card_count === right.visible_card_count &&
      left.scroll_height === right.scroll_height
    );
  }

  async function stabilityProtocol(options) {
    const requiredStableSamples = Math.max(2, Number(options.stabilitySamples || 3));
    const samplingIntervalMs = Math.max(0, Number(options.stabilityDelayMs ?? 750));
    const maxAttempts = Math.max(
      requiredStableSamples + 1,
      Number(options.maxScrollAttempts || 25)
    );
    const timeoutMs = Math.max(
      samplingIntervalMs * (requiredStableSamples + 1),
      Number(options.stabilityTimeoutMs || 30_000)
    );
    const { root, selector: resultsRootSelector } = resultsRootEvidence();
    const startedAt = Date.now();
    const samples = [];
    let stableWindow = [];
    let relevantMutationCount = 0;
    const observer = createUsableMutationObserver(root, (mutations) => {
      relevantMutationCount += mutations.filter(relevantMutation).length;
    });
    const stabilityMethod = observer
      ? "mutation_observer_visible_dom"
      : "timed_visible_dom_sampling";
    let finalVerification = {
      performed: false,
      matched: false,
      sample_index: null,
      observer_mutation_count: null,
    };
    try {
      for (let index = 0; index < maxAttempts; index += 1) {
        if (Date.now() - startedAt > timeoutMs) break;
        scrollToVisibleBottom(root);
        await wait(samplingIntervalMs);
        const sample = await visibleDomSample(
          root,
          samples.length,
          startedAt,
          observer ? relevantMutationCount : 0
        );
        samples.push(sample);
        const previous = stableWindow[stableWindow.length - 1];
        stableWindow = samplesMatch(previous, sample)
          ? [...stableWindow, sample]
          : [sample];
        if (stableWindow.length < requiredStableSamples) continue;

        if (observer) relevantMutationCount = 0;
        scrollToVisibleBottom(root);
        await wait(samplingIntervalMs);
        const finalSample = await visibleDomSample(
          root,
          samples.length,
          startedAt,
          observer ? relevantMutationCount : 0
        );
        samples.push(finalSample);
        const matched = samplesMatch(stableWindow[stableWindow.length - 1], finalSample);
        const observerClean = !observer || relevantMutationCount === 0;
        finalVerification = {
          performed: true,
          matched: matched && observerClean,
          sample_index: finalSample.sample_index,
          observer_mutation_count: observer ? relevantMutationCount : null,
        };
        if (!finalVerification.matched) {
          throw new Error("Final visible DOM stability verification differs from the stable window");
        }
        const navigation = navigationEvidence(Number(options.pageIndex));
        return {
          stability_method: stabilityMethod,
          mutation_observer_available: Boolean(observer),
          adapter_version: ADAPTER_VERSION,
          results_root_selector: resultsRootSelector,
          required_stable_sample_count: requiredStableSamples,
          actual_sample_count: samples.length,
          sampling_interval_ms: samplingIntervalMs,
          timeout_ms: timeoutMs,
          attempts: index + 1,
          max_attempts: maxAttempts,
          samples,
          stable_window_sample_indexes: stableWindow
            .slice(-requiredStableSamples)
            .map((item) => item.sample_index),
          final_verification: finalVerification,
          bottom_scroll_attempted: true,
          observer_mutation_evidence_available: Boolean(observer),
          no_relevant_dom_mutation_after_bottom: observer ? relevantMutationCount === 0 : null,
          end_of_list_evidence:
            !navigation.next.present ||
            Boolean(document.querySelector('[data-qa="search-end"], [data-end-of-list="true"]')),
        };
      }
    } finally {
      observer?.disconnect();
    }
    throw new Error(
      `Visible DOM stability timeout after ${samples.length} sample(s); ` +
        `method=${stabilityMethod}, timeout_ms=${timeoutMs}`
    );
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
      options.pageIndex === undefined ? pageIndexFromUrl(location.href) : Number(options.pageIndex);
    if (!Number.isSafeInteger(pageIndex) || pageIndex < 0) {
      throw new Error("pageIndex must be a non-negative integer");
    }
    const pageSize = Math.max(1, Math.min(100, Number(options.pageSize || 100)));
    const blocker = detectBlocker();
    if (blocker.type !== "none") {
      throw new Error(`Visible HH page is blocked: ${blocker.type}`);
    }
    const stability = await stabilityProtocol({ ...options, pageIndex });
    const { root, selector: resultsRootSelector } = resultsRootEvidence();
    if (resultsRootSelector !== stability.results_root_selector) {
      throw new Error("Visible results root changed after stability verification");
    }
    const { cards, malformed } = extractCards(root);
    if (malformed.length) {
      throw new Error(`Missing required vacancy identity for ${malformed.length} visible link(s)`);
    }
    const canonicalIds = [...new Set(cards.map((card) => `hh:${card.vacancy_id}`))].sort();
    const orderedIds = cards.map((card) => `hh:${card.vacancy_id}`);
    const finalStabilitySample = stability.samples[stability.final_verification.sample_index];
    if (
      !finalStabilitySample ||
      finalStabilitySample.canonical_ordered_id_hash !==
        (await sha256(JSON.stringify(orderedIds))) ||
      finalStabilitySample.visible_card_count !== cards.length ||
      finalStabilitySample.scroll_height !== scrollHeightFor(root) ||
      activeLoader()
    ) {
      throw new Error("Visible DOM changed after final stability verification");
    }
    const count = sourceReportedCount();
    const warnings = sourceCountDriftWarning(count, canonicalIds.length, pageIndex, pageSize);
    return {
      capture_contract: "hh_page_capture_v1",
      contract_version: CONTRACT_VERSION,
      adapter_version: ADAPTER_VERSION,
      source_kind: sourceKind,
      canonical_url: new URL(location.href).href,
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

  function unavailableLeadGenRedirectEvidence() {
    const url = new URL(location.href);
    const host = url.hostname.toLowerCase();
    const redirectIds = url.searchParams
      .getAll("utm_redirect_vacancy_id")
      .map((value) => compactText(value).replace(/^0+(?=\d)/, ""));
    const supportedLeadGenPath =
      /^\/article\/[0-9]+\/?$/.test(url.pathname) ||
      /^\/vrsurvey\/[A-Za-z0-9_-]+\/?$/.test(url.pathname);
    if (
      url.protocol !== "https:" ||
      !(host === "hh.ru" || host.endsWith(".hh.ru")) ||
      !supportedLeadGenPath ||
      redirectIds.length !== 1 ||
      !/^[0-9]{1,32}$/.test(redirectIds[0])
    ) {
      return null;
    }
    const vacancyId = redirectIds[0];
    url.hash = "";
    return {
      vacancy_id: vacancyId,
      canonical_url: `${url.protocol}//${url.host}/vacancy/${vacancyId}`,
      availability: {
        state: "unavailable",
        reason: "same_origin_lead_gen_redirect",
        observed_url: url.href,
      },
    };
  }

  async function captureVacancyDetail() {
    const blocker = detectBlocker();
    const canonical = parseVacancyIdentity({
      href: location.href,
      getAttribute: () => "",
      closest: () => null,
    });
    if (!canonical) {
      const unavailable = unavailableLeadGenRedirectEvidence();
      if (!unavailable) {
        throw new Error("The visible URL does not contain a confirmed vacancy identity");
      }
      return {
        capture_contract: "hh_detail_capture_v1",
        contract_version: CONTRACT_VERSION,
        adapter_version: ADAPTER_VERSION,
        captured_at: new Date().toISOString(),
        vacancy_id: unavailable.vacancy_id,
        canonical_url: unavailable.canonical_url,
        loader: { active: activeLoader() },
        blocker,
        availability: unavailable.availability,
        source_evidence: ["visible_url:same_origin_lead_gen_redirect"],
      };
    }
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

  return Object.freeze({
    version: ADAPTER_VERSION,
    contractVersion: CONTRACT_VERSION,
    selectors: Object.freeze({
      vacancyLinks: [...VACANCY_LINK_SELECTORS],
      cards: [...CARD_SELECTORS],
      loaders: [...LOADER_SELECTORS],
      next: [...NEXT_SELECTORS],
      previous: [...PREVIOUS_SELECTORS],
      resultsRoot: [...RESULTS_ROOT_SELECTORS],
    }),
    detectBlocker,
    classifyVacancyLink,
    captureListPage,
    capturePersonalRecommendations,
    captureVacancyDetail,
  });
})();
