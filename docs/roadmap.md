# Roadmap

This file records possible product directions; it is not an operating-agent
backlog. Agents should not implement roadmap items during job-search setup or a
daily run unless the user explicitly requests product development.

The first public version favors a small dependency-free engine and a strong
privacy boundary. Likely next steps, driven by real contributors and tests:

1. Extract the control plane into an installable `find_dream_job` package with
   separate schema/migrations, ingestion, domain services, and rendering modules.
2. Add forward-only, backed-up schema migrations beyond version 1.
3. Add dashboard localization and accessible empty/loading/error states.
4. Define source-adapter interfaces for official ATS APIs without bundling user
   credentials or violating site automation rules.
5. Add encrypted export/import for moving a private workspace between machines.
6. Add richer funnel analytics while keeping SQLite authoritative.

Connectors that can send applications or messages should remain explicit,
optional integrations with narrow authorization and visible evidence. They
should not become hidden side effects of scoring or ingestion.
