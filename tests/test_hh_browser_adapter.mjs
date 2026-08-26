import assert from "node:assert/strict";
import test from "node:test";

import {
  captureDetailFixture,
  captureFixture,
  classifyFixtureEntries,
  loadFixture,
} from "./hh_browser_adapter_harness.mjs";

test("same-origin HH lead-gen redirect closes the exact vacancy as unavailable", async () => {
  const capture = await captureDetailFixture({
    baseUrl:
      "https://spb.hh.ru/article/910027?utm_source=hh_lead_gen&utm_redirect_vacancy_id=910541#apply",
  });
  assert.equal(capture.vacancy_id, "910541");
  assert.equal(capture.canonical_url, "https://spb.hh.ru/vacancy/910541");
  assert.equal(capture.availability.state, "unavailable");
  assert.equal(capture.availability.reason, "same_origin_lead_gen_redirect");
  assert.equal(
    capture.availability.observed_url,
    "https://spb.hh.ru/article/910027?utm_source=hh_lead_gen&utm_redirect_vacancy_id=910541"
  );
  assert.equal(capture.blocker.type, "none");
  assert.equal(capture.loader.active, false);
  assert.equal("fields" in capture, false);
});

test("lead-gen redirect identity remains fail closed for another host or ID", async () => {
  await assert.rejects(
    captureDetailFixture({
      baseUrl:
        "https://example.test/article/910027?utm_redirect_vacancy_id=910541",
    }),
    /visible URL does not contain a confirmed vacancy identity/
  );
  await assert.rejects(
    captureDetailFixture({
      baseUrl:
        "https://spb.hh.ru/article/910027?utm_redirect_vacancy_id=not-a-number",
    }),
    /visible URL does not contain a confirmed vacancy identity/
  );
});

test("same-origin HH vrsurvey redirect closes only its exact vacancy", async () => {
  const capture = await captureDetailFixture({
    baseUrl:
      "https://spb.hh.ru/vrsurvey/synthetic_role_survey?utm_source=hh_lead_gen&utm_redirect_vacancy_id=910729",
  });
  assert.equal(capture.vacancy_id, "910729");
  assert.equal(capture.availability.state, "unavailable");
  assert.equal(capture.availability.reason, "same_origin_lead_gen_redirect");
});

test("classifies genuine vacancy links and ignores search navigation", () => {
  const fixture = loadFixture();
  const actual = classifyFixtureEntries("success_links");
  const expected = fixture.success_links
    .filter((entry) => entry.href.includes("/vacancy"))
    .map((entry) => ({
      case: entry.case,
      kind: entry.expected_kind,
      vacancy_id: entry.expected_id || "",
    }));
  assert.deepEqual(actual, expected);
});

test(
  "ordinary capture ignores map/navigation and preserves duplicate visible evidence",
  async () => {
    const capture = await captureFixture("success_links", "ordinary_search");
    assert.equal(capture.adapter_version, "hh-dom-v1.0.2");
    assert.equal(capture.source_kind, "ordinary_search");
    assert.equal(capture.cards.length, 2);
    assert.deepEqual(
      Array.from(capture.cards, (card) => card.vacancy_id),
      ["910001", "910001"]
    );
    assert.deepEqual(Array.from(capture.cards, (card) => card.position), [1, 2]);
    assert.equal(capture.blocker.type, "none");
    assert.equal(capture.warnings.length, 0);
    assert.equal(capture.stability.stability_method, "mutation_observer_visible_dom");
    assert.equal(capture.stability.mutation_observer_available, true);
    assert.equal(capture.stability.final_verification.matched, true);
  }
);

test("timed visible-DOM sampling succeeds without MutationObserver or requestAnimationFrame", async () => {
  const capture = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
  });
  assert.equal(capture.stability.stability_method, "timed_visible_dom_sampling");
  assert.equal(capture.stability.mutation_observer_available, false);
  assert.equal(capture.stability.observer_mutation_evidence_available, false);
  assert.equal(capture.stability.no_relevant_dom_mutation_after_bottom, null);
  assert.equal(capture.stability.final_verification.matched, true);
  assert.equal(capture.stability.required_stable_sample_count, 3);
  assert.equal(capture.stability.actual_sample_count, 4);
  assert.equal(capture.stability.sampling_interval_ms, 750);
  assert.equal(capture.stability.timeout_ms, 30000);
  assert.deepEqual(
    Array.from(capture.stability.samples[0].canonical_ordered_ids),
    ["hh:910001", "hh:910001"]
  );
});

test("restricted evaluator does not require a usable globalThis", async () => {
  const capture = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
    unsuitableGlobalThis: true,
  });
  assert.equal(capture.adapter_version, "hh-dom-v1.0.2");
  assert.equal(capture.stability.final_verification.matched, true);
});

test("restricted evaluator computes stable Unicode hashes without TextEncoder", async () => {
  const reference = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
  });
  const capture = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
    textEncoder: false,
  });
  assert.equal(capture.adapter_version, "hh-dom-v1.0.2");
  assert.equal(capture.cards.length, 2);
  assert.equal(capture.stability.final_verification.matched, true);
  assert.equal(capture.canonical_id_set_hash, reference.canonical_id_set_hash);
  assert.deepEqual(
    Array.from(capture.stability.samples, (sample) => sample.canonical_ordered_id_hash),
    Array.from(reference.stability.samples, (sample) => sample.canonical_ordered_id_hash)
  );
});

test("restricted evaluator computes the same SHA-256 without WebCrypto or typed arrays", async () => {
  const reference = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
  });
  const capture = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
    restrictedPrimitives: true,
  });
  assert.equal(capture.canonical_id_set_hash, reference.canonical_id_set_hash);
  assert.deepEqual(
    Array.from(capture.stability.samples, (sample) => sample.canonical_ordered_id_hash),
    Array.from(reference.stability.samples, (sample) => sample.canonical_ordered_id_hash)
  );
});

test("page after first derives only previous evidence from the visible current URL", async () => {
  const capture = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
    baseUrl:
      "https://example.test/search/vacancy?page=1&search_session_id=synthetic",
    pageIndex: 1,
  });
  assert.equal(capture.navigation.previous.present, true);
  assert.equal(capture.navigation.previous.page_index, 0);
  assert.equal(
    capture.navigation.previous.url,
    "https://example.test/search/vacancy?page=0&search_session_id=synthetic"
  );
  assert.equal(capture.navigation.next.present, false);
  assert.equal(capture.navigation.consistent, true);
});

test("numeric pagination provides exact visible next-page evidence", async () => {
  const capture = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
    baseUrl: "https://example.test/search/vacancy?page=0&search_session_id=synthetic",
    pageIndex: 0,
    paginationUrls: [
      "/search/vacancy?page=0&search_session_id=synthetic",
      "/search/vacancy?page=1&search_session_id=synthetic",
      "/search/vacancy?page=2&search_session_id=synthetic",
    ],
  });
  assert.equal(capture.navigation.next.present, true);
  assert.equal(capture.navigation.next.page_index, 1);
  assert.equal(
    capture.navigation.next.url,
    "https://example.test/search/vacancy?page=1&search_session_id=synthetic"
  );
});

test("conflicting numeric next-page URLs fail closed", async () => {
  const capture = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
    baseUrl: "https://example.test/search/vacancy?page=0&search_session_id=synthetic",
    pageIndex: 0,
    paginationUrls: [
      "/search/vacancy?page=1&search_session_id=one",
      "/search/vacancy?page=1&search_session_id=two",
    ],
  });
  assert.equal(capture.navigation.next.present, false);
});

test("overflow terminal page prefers the exact visible target over a clamped previous control", async () => {
  const capture = await captureFixture("success_links", "ordinary_search", {
    mutationObserver: false,
    baseUrl:
      "https://example.test/search/vacancy?page=8&items_on_page=100&search_session_id=synthetic",
    pageIndex: 8,
    sourceReportedCount: 763,
    previousUrl:
      "/search/vacancy?page=6&items_on_page=100&search_session_id=synthetic",
    paginationUrls: [
      "/search/vacancy?page=5&items_on_page=100&search_session_id=synthetic",
      "/search/vacancy?page=6&items_on_page=100&search_session_id=synthetic",
      "/search/vacancy?page=7&items_on_page=100&search_session_id=synthetic",
    ],
    entrySequences: [[], [], [], []],
  });
  assert.equal(capture.cards.length, 0);
  assert.equal(capture.navigation.previous.present, true);
  assert.equal(capture.navigation.previous.page_index, 7);
  assert.equal(
    capture.navigation.previous.url,
    "https://example.test/search/vacancy?page=7&items_on_page=100&search_session_id=synthetic"
  );
  assert.equal(capture.navigation.next.present, false);
  assert.equal(capture.navigation.consistent, true);
});

test("changing ordered vacancy IDs fail closed", async () => {
  const fixture = loadFixture();
  const changed = fixture.success_links.map((entry, index) =>
    index === 0
      ? {
          ...entry,
          href: "/vacancy/910002",
          card_vacancy_id: "910002",
        }
      : entry
  );
  await assert.rejects(
    captureFixture("success_links", "ordinary_search", {
      mutationObserver: false,
      entrySequences: [fixture.success_links, changed, fixture.success_links, changed],
    }),
    /Visible DOM stability timeout/
  );
});

test("changing height fails closed when no stable window is reached", async () => {
  await assert.rejects(
    captureFixture("success_links", "ordinary_search", {
      mutationObserver: false,
      heightSequence: [1000, 1100, 1000, 1100],
    }),
    /Visible DOM stability timeout/
  );
});

test("active loader fails closed at the bounded attempt limit", async () => {
  await assert.rejects(
    captureFixture("success_links", "ordinary_search", {
      mutationObserver: false,
      loaderActive: true,
    }),
    /Visible DOM stability timeout/
  );
});

test("independent final verification mismatch fails closed", async () => {
  const fixture = loadFixture();
  const changed = fixture.success_links.map((entry, index) =>
    index === 0
      ? {
          ...entry,
          href: "/vacancy/910002",
          card_vacancy_id: "910002",
        }
      : entry
  );
  await assert.rejects(
    captureFixture("success_links", "ordinary_search", {
      mutationObserver: false,
      entrySequences: [
        fixture.success_links,
        fixture.success_links,
        fixture.success_links,
        changed,
      ],
    }),
    /Final visible DOM stability verification differs/
  );
});

test("missing or ambiguous results roots fail closed", async () => {
  await assert.rejects(
    captureFixture("success_links", "ordinary_search", {
      mutationObserver: false,
      missingResultsRoot: true,
    }),
    /results root is missing/
  );
  await assert.rejects(
    captureFixture("success_links", "ordinary_search", {
      mutationObserver: false,
      ambiguousResultsRoot: true,
    }),
    /results root is ambiguous/
  );
});

for (const blocker of ["login", "captcha", "error"]) {
  test(`${blocker} page fails closed before collection`, async () => {
    await assert.rejects(
      captureFixture("success_links", "ordinary_search", {
        mutationObserver: false,
        blockerSelector: blocker,
      }),
      new RegExp(`blocked: ${blocker}`)
    );
  });
}

test("personal recommendations use the same safe classifier", async () => {
  const capture = await captureFixture("success_links", "personal_recommendations");
  assert.equal(capture.source_kind, "personal_recommendations");
  assert.deepEqual(
    new Set(capture.cards.map((card) => card.vacancy_id)),
    new Set(["910001"])
  );
});

test("genuine malformed vacancy links still fail closed", async () => {
  const fixture = loadFixture();
  const classified = classifyFixtureEntries("malformed_links");
  assert.deepEqual(
    classified.map(({ case: caseName, kind }) => ({ case: caseName, kind })),
    fixture.malformed_links.map((entry) => ({
      case: entry.case,
      kind: entry.expected_kind,
    }))
  );
  await assert.rejects(
    captureFixture("malformed_links", "ordinary_search"),
    /Missing required vacancy identity for 3 visible link\(s\)/
  );
});

test("sponsored title uses one visible response identity from the same card", async () => {
  const capture = await captureFixture("sponsored_links", "ordinary_search", {
    mutationObserver: false,
  });
  assert.equal(capture.cards.length, 1);
  assert.equal(capture.cards[0].vacancy_id, "910101");
  assert.equal(capture.cards[0].canonical_url, "https://example.test/vacancy/910101");
});

test("sponsored title with ambiguous visible response identities fails closed", async () => {
  await assert.rejects(
    captureFixture("ambiguous_sponsored_links", "ordinary_search", {
      mutationObserver: false,
    }),
    /Missing required vacancy identity for 1 visible link\(s\)/
  );
});
