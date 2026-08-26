# Changelog

## Unreleased

- Добавлена схема v11 с append-only доказательствами повторяющихся видимых 502
  на точной хвостовой странице HH. Узкое восстановление выключено по умолчанию,
  требует не менее пяти независимых попыток и не применяется к пропущенным
  страницам, login/CAPTCHA, access denied, malformed content или незакрытой
  detail-очереди. Альтернативный полный rollover принимает только непрерывную
  новую сессию от страницы 0 и никогда не смешивает страницы разных сессий.

- Персональные рекомендации теперь можно безопасно завершить по явно сниженной
  конфигурационной границе уже внутри durable run: требуется обновлённый P1-план,
  непрерывный полностью проверенный диапазон страниц и отсутствие постороннего
  config drift; recovery сохраняется как отдельное audit-событие.

- DOM-адаптер HH обновлён до `hh-dom-v1.0.2`: observer остаётся основным
  методом, а при его реальной недоступности используется ограниченная по
  времени выборка видимого DOM с упорядоченными ID, числом карточек, высотой,
  позицией, loader-state и отдельной финальной проверкой. Адаптер возвращается
  как self-contained object и не требует записи в `globalThis`,
  а заголовок через `adsrv.hh.ru` принимает vacancy identity только из
  единственного видимого same-card response URL; отсутствие или конфликт ID
  остаются fail-closed. В ограниченном evaluator без `TextEncoder`, typed
  arrays и WebCrypto SHA-256 доказательства используют эквивалентные локальные
  UTF-8 и pure-JS SHA-256 и сверяются с обычным browser runtime,
  а count-drift recapture признаёт валидный timed-sampling fallback и может
  завершить pre-fix checkpoint только по непрерывному хвосту полностью
  эквивалентных снимков. Различающийся повтор остаётся непроверенным, но
  разрешает следующий независимый recapture; прогресс возможен только после
  непрерывного хвоста полностью эквивалентных снимков.
  Если rolling-выдача сократилась и HH зажал запрошенную страницу к новой
  последней странице, противоречивый semantic previous-control не подменяет
  точную видимую числовую ссылку на ожидаемый предыдущий индекс; отсутствие
  единственного точного URL по-прежнему остаётся fail-closed.
  Если точный `/vacancy/<id>` на HH перенаправлен на same-origin lead-gen
  `/article/<id>` или `/vrsurvey/<slug>` с единственным совпадающим
  `utm_redirect_vacancy_id`, очередь
  подробностей закрывается отдельным доказательством недоступности без
  вымышленных полей вакансии и без повышения уровня list-evidence до detail.
  Реализация не использует `requestAnimationFrame`, Chrome или скрытые API.
- Добавлена отдельная команда
  `resolve-due-followup-from-reverified-inbound`: свежая повторная проверка
  точного удалённого диалога может разрешить frozen follow-up по неизменяемому
  историческому `human_reply`, не меняя его `event_at`, не создавая дубликат и
  не смешиваясь с пользовательской отменой. Доказательно синхронизированный
  нетерминальный status отклика и вакансии сохраняется, не разрушая frozen
  identity scope.
- Zero-evidence recovery HH теперь отличает source evidence от append-only
  blocker/reopen/invalidation audit P1, сохраняет прежнюю историю и включает её
  хеши/ID в событие перепланирования; любой реальный снимок, прогресс или
  неизвестный source-bearing формат по-прежнему блокирует операцию. Более
  ранний superseded adapter blocker и известные audit-only ошибки recovery не
  подменяют последующий подтверждённый MutationObserver runtime blocker.
  Для personal recommendations совместимость audit identity сохраняется при
  чисто аддитивном расширении вложенных настроек HH: прежние значения должны
  совпасть, а изменение или удаление существующего поля остаётся fail-closed.
- Добавлена точная аудируемая `cancel-due-followup-obligation`: она очищает
  только даты follow-up, сохраняет отклик и lifecycle, идемпотентно завершает
  frozen item через `user_cancelled_followup_obligation`, переживает refresh и
  fail-closed обнаруживает повторную активацию той же обязанности.
- Исправлена классификация ссылок в DOM-адаптере HH `hh-dom-v1.0.1`: ссылки
  карты и навигации исключаются до проверки identity, а настоящая карточка без
  устойчивого числового ID по-прежнему блокирует снимок.
- Добавлен контракт счётчиков Telegram `telegram_source_units_v1`: граничные
  публикации и повторные наблюдения учитываются без подгонки значений, а
  `raw >= processed >= reconciled` проверяется до продвижения курсора.
- Повторная сверка входящих после исходящего действия теперь имеет явное
  безопасное действие и требует доказательство с `observed_at` позже последней
  попытки или видимого подтверждения, не инвалидируя независимые источники.
- План ежедневного запуска фиксирует `external_action_id_floor`: текущие
  действия и прежние неопределённые попытки обязательны, старые
  `drafted`/`authorized` остаются видимым legacy backlog. Для уже замороженного
  active plan добавлена только явная аудируемая реклассификация без изменения
  append-only истории, вывода доставки или автоматического повтора.
- Добавлена схема v10 и безопасное получение HH P2: проверяемый DOM-адаптер
  `hh-dom-v1.0.2` только для чтения обычной выдачи, персональных рекомендаций и
  страницы вакансии; доказательство стабильности DOM и независимый повторный
  снимок при расхождении счётчика; режимы `full`, `shadow`, `delta`, `resume` и
  `audit`; монотонные контрольные точки каждого потока и неизменяемая история
  аудита; программная сверка канонических ID и псевдонимов; ограниченная очередь
  подробностей для новых и изменённых вакансий; манифест v2 с совместимостью
  v1; точная интеграция с рабочими элементами P1, `lease`, отложенными записями
  и одной публикацией P0. Безопасный публичный режим — `shadow` с тремя чистыми
  полными сравнениями и периодическим полным аудитом; расхождение автоматически
  лишает контрольную точку права на `delta`.
- Добавлена схема v9: долговечные SQLite-планы ежедневного запуска,
  нормализованные шаги и элементы работы, типизированные версионированные
  манифесты, неизменяемая история переходов, компактный статус, безопасные
  приостановка и возобновление с новым `lease`, контроль изменения конфигурации,
  сверка неопределённых внешних действий и программная проверка полного объёма
  очередей, источников и дополнительных условий. Финализация по-прежнему
  публикует ровно одно атомарное поколение P0; доказательства v8 и поколения
  проекций сохраняются миграцией с резервной копией.
- Added schema v8 projection revisions and expiring daily-run leases; global
  deferred/no-render writes; bounded OS-backed writer/render locks; immutable
  staged projection generations with atomic publication; single-pass actionable
  WIP pagination; and a bounded dashboard projection. Immediate rendering stays
  the compatibility default, and v1–v7 evidence rows are preserved by migration.
- Added schema v7 append-only employer-interaction invalidations, effective
  interaction metrics, deterministic one-row-per-vacancy application state for
  follow-up projections, a fail-closed idempotent correction CLI, and backed-up
  migration from schema v6.
- Added schema v6 append-only lifecycle, current-action, and external-action
  evidence; precise interview progression; configurable decision metadata;
  multi-label source attribution; import quarantine and reprocessing; outcome,
  WIP/SLA, operational-health, account-portfolio, and false-negative-audit
  workflows; plus backed-up idempotent migrations from every supported prior
  schema.
- Added schema v5 generic source checkpoints and configurable public Telegram
  discovery with a per-channel 30-day initial backfill, post-ID deltas,
  fail-closed page/post/score/SQLite reconciliation, and cursors that advance
  only after complete coverage.
- Added schema v4 employer interactions, vacancy-level 14/30-day conversion
  cohorts, canonical source-stream aliases, downstream source-quality metrics,
  explicit employer account radar/signals/links, and evidence-backed vacancy
  factors that never alter score automatically.
- Added LinkedIn Gmail job-alert ingestion with stable LinkedIn job IDs and a
  fail-closed daily mail workflow that scores every recommended vacancy and
  archives each fully processed message only with authorization and verification.
- Added schema v3 external-ID aliases so semantic repost deduplication keeps one
  canonical vacancy while every imported source ID and URL remains resolvable.
- Added fail-closed daily search coverage with configured required streams,
  deterministic HH OR/search-period query generation, page and lazy-load
  checkpoints, SQLite history, and a generated coverage report.
- Added conservative company/title/description fingerprints for exact vacancy
  reposts with new external IDs and an explicit backed-up schema-v2 migration.
- Made repository use agent-first with a canonical `AGENTS.md`, an executable
  first-time onboarding prompt, an operational runbook, copy-paste delegation
  prompts, completion contracts, and documentation consistency tests.
- Separated the reusable engine from candidate-specific settings and data.
- Added external private workspaces through `JOB_SEARCH_HOME`.
- Added non-destructive initialization and workspace diagnostics.
- Made follow-up and automation policy configurable with safe public defaults.
- Added default-deny Git rules, a publication audit, synthetic examples, tests,
  public documentation, and MIT licensing.
- Removed one-off legacy import adapters and candidate-specific channel labels
  from the reusable engine.
- Expanded the publication audit with redacted PII findings and broader secret,
  phone, home-path, and public-IP detection.
- Updated the CI checkout action to the already validated v7 release.
- Hardened SQLite schema version checks and inline dashboard JSON escaping.
