# SOP: Kafka Ingestion Architecture

## Purpose

Maintain and review the Splunk Kafka-ingestion architecture alternatives, their diagrams, and their reliability requirements. This SOP records the current design baseline and the checks required before accepting future changes.

## Current deliverables

- [All diagrams](./splunk-kafka-all-diagrams.drawio) — consolidated Draw.io file with one diagram per page.
- [Regional architecture options](./splunk-kafka-regional-architecture-options.drawio) — editable regional and bottom-to-top layouts.
- [Kafka ingestion architecture](./splunk-kafka-ingestion-architecture.drawio) and [standard-icon variant](./splunk-kafka-ingestion-architecture-standard-icons.drawio).
- [Kafka ingestion findings](./splunk-kafka-ingestion-findings.md) — research, assumptions, comparisons, public sources, and the detailed Cribl reliability assessment.

The consolidated diagram is the easiest entry point when reopening the work. Edit the appropriate source diagram as well as the corresponding page in the consolidated file when changing a design.

## Scope and applicability

- The findings cover Splunk Enterprise 10.x with a clustered indexer tier; the exact deployed minor/build must be checked before implementation.
- The Cribl alternative assumes customer-managed, on-prem Cribl Stream and a Kafka Source feeding a Splunk HEC Destination. Current public Cribl documentation checked on 2026-11-14 identifies version 4.20.4; verify the installed build, license, and Splunk compatibility.
- The regional diagrams show the previously requested three-region, six-worker Cribl layout and a shared indexer cluster. Treat topology and central-service placement as design assumptions until confirmed by the owning teams.
- This is an advisory design record, not evidence that any configuration was deployed, tested, or accepted.

## Architecture alternatives

1. **SOC4Kafka + Heavy Forwarders + Edge Processors:** Kafka ingestion through SOC4Kafka, Splunk parsing/routing on HFs, optional additional processing on EP, then indexing.
2. **Cribl Stream:** Kafka Source, Routes/Pipelines, and Splunk HEC Destination on Cribl Workers; the indexer cluster and Search Head remain.

Use the findings document for product/version applicability, ownership, functional boundaries, and source citations. Do not treat replacing components as a transparent configuration migration; map and validate each transformation separately.

## Diagram maintenance standard

- Use the vendor stencil or published vendor icon for each component. Use the single-node Heavy Forwarder icon for an individual HF and a group icon only when depicting a pool.
- Keep Kafka ingress and Splunk-bound event/log flows visually distinct. Preserve the existing regional color convention: teal for Kafka ingress, orange for event/log output, and dashed violet for management.
- Keep component labels concise, retain region and trust boundaries, and avoid crossing or clustered connectors.
- Preserve the separate regional and bottom-to-top views in the consolidated diagram.
- After editing, open the Draw.io file and inspect every affected page. Verify page count, component labels/icons, connector direction and colors, and that connectors terminate at the intended nodes.

## Reliability baseline: no event loss

- Keep Kafka retention and a tested replay process as the upstream recovery mechanism. Cribl documents its Kafka Source as pull-based; Source Persistent Queues are not supported for pull Sources.
- Cribl's HEC Destination documentation says not to enable Splunk Indexer Acknowledgement on the HEC token because the requests can fail with a missing channel GUID. Do not treat an HEC `2xx` as proof that an event is indexed.
- Destination Persistent Queues can reduce loss during backpressure but are finite and do not provide an absolute zero-loss guarantee. Evaluate Always On mode, configure queue-full behavior to Block rather than Drop, size for the required outage window, and select storage appropriate to worker and region failure domains.
- Verify Kafka offset-commit ordering against successful HEC delivery and durable PQ writes for the exact deployed Cribl build. The current public docs reviewed do not establish this end-to-end guarantee.
- Cribl's current HEC documentation has inconsistent retry/PQ guidance for some HTTP status codes. Confirm intended behavior with Cribl and validate it in an isolated environment; do not rely on defaults.
- Use a stable event identity and reconcile source records against searchable Splunk events. Document duplicate handling because retries and replay can produce duplicates.

The current evidence, retry caveat, source citations, validation matrix, and rollback gate are maintained in [Kafka ingestion findings](./splunk-kafka-ingestion-findings.md), under **“Delivery reliability and the ‘no event lost’ requirement.”**

## Change and acceptance procedure

1. Confirm the request, intended architecture alternative, source contract, topology, Splunk edition/build, Cribl build, and accountable owners. Record unknowns rather than assuming them.
2. Research current official product documentation for any changed capability, support boundary, retry behavior, version compatibility, or lifecycle statement. Date-check citations and retain the exact applicability.
3. Update the source diagram(s), then synchronize affected pages in the consolidated Draw.io file. Keep unrelated pages and existing source assets unchanged.
4. Review the diagram visually and validate XML integrity, unique cell IDs, and connector references.
5. Before production acceptance, run a separately authorized non-production canary on the intended topology. Exercise relevant receiver failures, timeouts, retries, queue-full behavior, and worker/storage failure and recovery.
6. Reconcile stable source IDs through to searchable Splunk data. Acceptance requires zero missing IDs, understood duplicates, no silent drops, and evidence that offsets do not advance beyond data that remains durably recoverable.
7. Record the observation window, measurable stop thresholds, evidence owners, rollback owner, safe return state, and post-rollback readback in the findings document.
8. Do not clear or disable undrained Persistent Queues as a cleanup or rollback shortcut.

## Reopening this work

Open [All diagrams](./splunk-kafka-all-diagrams.drawio) for the visual collection and [Kafka ingestion findings](./splunk-kafka-ingestion-findings.md) for the written assessment. This SOP and the deliverables are stored in the session's persistent `files` folder and are registered as session artifacts. For long-term access outside this session or on another machine, copy the files into an approved durable team repository or document store.
