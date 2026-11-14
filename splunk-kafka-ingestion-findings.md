# Kafka ingestion architecture findings

**Scope:** Splunk Enterprise 10.x, clustered indexer tier, Kafka source, Splunk OpenTelemetry Collector for Kafka (SOC4Kafka) used only for ingestion, Heavy Forwarder (HF) for Splunk parsing and routing, Edge Processor (EP) for additional processing, and Search Head for search-time extractions.

The exact Enterprise 10.x minor release, Kafka serialization, EP deployment mode, and actual list of the eight transformations are not specified. Validate release-specific behavior and event-size handling before production.

## Proposed data path

```text
Kafka
  -> Splunk OTel Collector for Kafka (SOC4Kafka; HEC delivery only)
  -> Heavy Forwarder (HEC input; event breaking, parsing, routing)
  -> Edge Processor (HF S2S input; additional SPL2 processing)
  -> Indexer cluster (indexing tier)
  -> Search Head (search-time extractions)
```

The updated editable architecture is [splunk-kafka-ingestion-architecture-standard-icons.drawio](./splunk-kafka-ingestion-architecture-standard-icons.drawio).

## Iconography

The diagram uses Splunk deployment glyphs for Heavy Forwarders, Indexers, and Search Heads from the icon sheet linked in Splunk's [Draw a diagram of your deployment](https://help.splunk.com/en/splunk-enterprise/administer/inherit-a-splunk-deployment/10.4/inherited-deployment-tasks/draw-a-diagram-of-your-deployment) guidance (page updated 2026-05-09). Kafka uses an embedded copy of the Apache Kafka [logo-only mark](https://kafka.apache.org/logos/kafka-logo-no-text.png), avoiding an unavailable remote stencil dependency. The OTel Collector now uses the official OpenTelemetry [color icon mark](https://opentelemetry.io/img/logos/opentelemetry-icon-color.png); the Edge Processor remains a distinct standard diagram symbol because the Splunk icon sheet does not provide a dedicated glyph for it. Icon artwork is embedded in the file so rendering does not depend on remote image or stencil fetches. The Splunk SVG conversion comes from the community-maintained [Splunk draw.io icon library](https://github.com/livehybrid/splunk_drawio_icons), which is based on the public icon sheet and is not a Splunk-maintained library.

## Responsibility by stage

| Stage | Responsibility in this design | Notes |
|---|---|---|
| Kafka | Provide topics and broker access to the collector. | Kafka access control and topic administration are outside the Splunk processing tiers. |
| SOC4Kafka | Consume Kafka and deliver records to the HF over HEC. | No transformations are assigned to the collector in this design. Configure its Kafka receiver and HEC exporter, including endpoint, token, TLS, and metadata as needed; omit optional processors. The [SOC4Kafka project documentation](https://github.com/splunk/splunk-opentelemetry-collector-for-kafka/tree/main?tab=readme-ov-file#how-to-start-with-soc4kafka) describes the Kafka receiver, HEC exporter, and installation options. |
| HF | Receive the HEC payload; perform event breaking, Splunk parsing, timestamp and metadata handling, and routing; forward selected data to EP. | Use the appropriate `props.conf`, `transforms.conf`, HEC input, and forwarding configuration. Splunk documents configuring a [forwarder target group](https://help.splunk.com/en/splunk-cloud-platform/process-data-at-the-edge/use-edge-processors-for-splunk-cloud-platform/get-data-into-edge-processors/get-data-from-a-forwarder-into-an-edge-processor) to send data to EP. |
| EP | Perform additional field extraction and the agreed SPL2 transformations, such as filtering, masking, and removing unneeded data. | EP has its own pipeline, source, and destination configuration. Its exact processing behavior depends on input type and output protocol. Enterprise EP is documented for [Splunk Enterprise 10.0 and higher](https://help.splunk.com/en/data-management/onboard-data-to-splunk-cloud-platform/explore-data-management-solutions/data-management-solutions). |
| Indexer cluster | Store/index the prepared events. | “Indexing only” is the intended role here, not a guarantee that no platform-side transforms run. See the protocol caveat below. |
| Search Head | Configure the search-time extractions and search knowledge objects. | These are separate from the pre-index HF and EP transformations. |

## Important event-breaking boundary

For the **HF → EP** path, Splunk's [Edge Processor transformation guide](https://help.splunk.com/en/data-management/process-data-at-the-edge/use-edge-processors-for-splunk-cloud-platform/how-the-edge-processor-solution-works/how-the-edge-processor-solution-transforms-data) states that the HF breaks data into events according to its `props.conf` configuration. EP then extracts additional fields and applies its own pipeline transformations. Therefore, for this ordering, assign the initial event breaking to the HF; do not assume EP will re-break already parsed HF S2S events.

EP can break data for other source types and paths. For example, the guide describes source-type-based breaking for raw HEC input. With the HEC event endpoint, EP treats each top-level JSON object as a distinct event; its event framing is not configurable there in the same way as raw HEC. These distinctions matter if a Kafka record contains multiple logical events or multiline content.

The detailed source/destination transformation matrix cited below is from the Splunk Cloud Platform EP manual. Splunk Enterprise documentation confirms EP availability from Enterprise 10.0+ and its Enterprise control plane, but the exact behavior should be checked against the selected Enterprise minor version and input/output mode.

## Indexer-tier caveat

Splunk's [Edge Processor transformation guide](https://help.splunk.com/en/data-management/process-data-at-the-edge/use-edge-processors-for-splunk-cloud-platform/how-the-edge-processor-solution-works/how-the-edge-processor-solution-transforms-data) documents that transformations after EP depend on how the data is sent to Splunk Platform:

- With a Splunk Platform **HEC** destination, the platform can apply `props.conf`/`transforms.conf` processing, including Ingest Actions.
- With a Splunk Platform **S2S** destination, the cited transformation guide says the platform applies Ingest Actions only.

Thus, keep the indexer tier “indexing only” by design: confirm the EP output mode and ensure no unintended indexer-side parsing, routing, or Ingest Actions are configured. The indexer cluster still needs its normal index and cluster configuration.

## Configuration count

For this design, there are **six logical configuration surfaces**, counting the Kafka side:

1. Kafka brokers, topic access, and source-side permissions.
2. SOC4Kafka deployment, Kafka receiver, and HEC delivery configuration.
3. HF HEC input, parsing/event breaking, routing, and forwarding to EP.
4. EP source/destination and transformation pipeline.
5. Indexer-cluster index and cluster-managed configuration.
6. Search Head search-time extractions.

That is **five Splunk-side surfaces** plus Kafka. It does not count every file, replica, or control-plane operation as an independent processing stage. For example, HF inputs, parsing, and forwarding can span several configuration files; EP pipeline management and indexer-cluster bundle deployment are control-plane activities.

Ingest Actions are **not included** in the baseline above. If added, treat their ruleset configuration and deployment as a distinct surface; they are not the same mechanism as an EP pipeline.

## High-volume and large-event considerations

- SOC4Kafka's project README lists several installation methods and configurable HEC sending queues and batching. It states acknowledgment support is not available, explicitly including HEC acknowledgments, and lists Protobuf encoding as unsupported. Do not infer end-to-end exactly-once delivery; validate replay, duplicate, and loss handling for the chosen topology.
- Size SOC4Kafka's queue and batching settings with representative Kafka records and the intended number of collector replicas.
- The Splunk HEC exporter documents `max_content_length_logs` with a default of `2097152` and `max_event_size` with a default of `5242880`. Treat these as exporter settings to validate, not as proof of the maximum event size accepted by the entire Collector → HF → EP → indexer path.
- The term “Magnificent 8” is treated here as the user’s eight transformations. Map each operation to one stage and verify its supported behavior on the exact Enterprise/EP release to avoid duplicate work.

## Alternative Kafka entry: HF modular-input TA vs SOC4Kafka

**Checked: 2026-11-14. Applies to the stated Splunk Enterprise 10.x clustered indexer topology; exact 10.x minor and Kafka/app versions still matter.**

The two ingress branches and their shared HF → EP → indexer flow are shown in [splunk-kafka-ingestion-options.drawio](./splunk-kafka-ingestion-options.drawio).

### Product-name distinction

“Splunk Kafka TA” is ambiguous. The current Splunkbase listing I found for a Splunk Enterprise modular input is [Kafka Messaging Modular Input](https://splunkbase.splunk.com/app/1817), by BaboonBones Ltd, not Splunk. Its listing describes a modular input that indexes messages from Kafka and says it can be installed on self-hosted Splunk forwarders. The listed version is 1.9.0 (2026-08-21), with Splunk Enterprise compatibility 9.0–10.7 and **Developer Supported** status. If a different TA is intended, its exact app ID/version is needed before making version-specific claims.

The [official Splunk Connect for Kafka (SC4Kafka) overview](https://help.splunk.com/en/splunk-cloud-platform/get-data-in/splunk-connect-for-kafka/2.2/overview/splunk-connect-for-kafka) identifies SC4Kafka as a Kafka Connect sink that consumes topics and streams data to HEC. Its [2.2 release notes](https://help.splunk.com/en/splunk-cloud-platform/get-data-in/splunk-connect-for-kafka/2.2/release-notes/release-notes-for-splunk-connect-for-kafka) list version 2.2.2, platform compatibility of Splunk 7.1 and later, and end of life on **2027-10-31**; both pages say to use SOC4Kafka instead. The overview describes SOC4Kafka as a standalone OpenTelemetry-based connector supported by Splunk. The broad SC4Kafka compatibility range includes Enterprise 10.x, but does not certify an exact 10.x minor.

The [Splunk SOC4Kafka project README](https://github.com/splunk/splunk-opentelemetry-collector-for-kafka/tree/main?tab=readme-ov-file#how-to-start-with-soc4kafka) calls SOC4Kafka the replacement for SC4Kafka. It is a standalone Splunk OpenTelemetry Collector package configured with a Kafka receiver and Splunk HEC exporter, not a Kafka Connect plugin or an HF-installed TA. The README lists Kafka 3.7.0 and later (tested versions 3.7.0, 3.8.0, 3.9.0, and 4.0.0), Splunk 9.x and later with a valid HEC token, and Apache Kafka, Amazon MSK, and Confluent support. These broad requirements include Enterprise 10.x but are not a per-build certification. The repository documents Helm, Ansible, OCI Streaming, and manual package installation. It states acknowledgment support is unavailable (HEC acknowledgments explicitly unsupported) and Protobuf encoding is unsupported. In this design, leave the optional processor list empty so SOC4Kafka only consumes and forwards data. The README does not establish co-location on HF hosts as a supported or recommended deployment.

The Splunkbase app named simply [Kafka](https://splunkbase.splunk.com/app/5906) is a Splunk SOAR connector (SOAR Cloud/On-Prem), not an Enterprise Kafka input add-on.

### Data paths

**A. Modular-input add-on installed on the HF**

```text
Kafka -> Kafka Messaging Modular Input on HF -> HF parsing / routing
      -> EP -> indexer cluster -> Search Head
```

**Inference:** Given the listing's “modular input” type and forwarder-installation claim, this option places the Kafka consumer in the HF's Splunk input runtime, without a separate Kafka Connect worker tier or a HEC hop. The HF then applies its normal parsing, timestamp/metadata, routing, and forwarding configuration; the [HF-to-EP documentation](https://help.splunk.com/en/data-management/process-data-at-the-edge/use-edge-processors-for-splunk-cloud-platform/how-the-edge-processor-solution-works/how-the-edge-processor-solution-transforms-data) establishes the HF event-breaking stage for that downstream path. The public Splunkbase listing does not document detailed consumer-group/offset behavior, event framing for every serialization, large-record limits, or a throughput benchmark. Those are material acceptance checks for the exact add-on version.

**B. SOC4Kafka with HEC to the HF**

```text
Kafka -> SOC4Kafka collector deployment (Kafka receiver; no processors)
      -> HEC / HTTPS -> HF HEC input -> HF parsing / routing
      -> EP -> indexer cluster -> Search Head
```

SOC4Kafka uses the Splunk OTel Collector package with a Kafka receiver and Splunk HEC exporter; configure the HEC destination to the HF input to preserve the requested HF → EP processing order. A destination pointing directly at indexers bypasses HF and EP. The collector can be installed through one of the repository's documented deployment methods; it is a separate collector runtime from Splunkd on the HF. The public sources checked do not establish co-location on HF hosts as supported or recommended.

The user's “Magnificent 8” transformations remain assigned to HF and EP, not SOC4Kafka. The README provides optional queue, batch, and processor settings but does not publish a maximum Kafka record/event size for the full Kafka → SOC4Kafka → HEC → HF → EP path. Because acknowledgment support is not available, validate loss, replay, and duplication behavior with representative event sizes and outages; do not assume exactly-once delivery. The README does not describe SOC4Kafka as a replacement for the downstream HF parsing or EP SPL2 pipeline.

### Logical configuration surfaces

Using the same counting convention as the baseline diagram (logical domains, not config files, hosts, or administrative teams):

| Path | Domains including Kafka | Splunk-side domains | What is configured |
|---|---:|---:|---|
| HF modular-input TA | 6 | 5 | Kafka topics/access; TA app and modular input on the HF pool; HF parsing/routing/forwarding; EP pipeline/source/destination; indexer-cluster indexes/config; Search Head search-time knowledge. |
| SOC4Kafka to HF HEC | 6 | 5 | Kafka topics/access; SOC4Kafka collector package/deployment and receiver/exporter configuration; HF HEC input plus parsing/routing/forwarding; EP; indexer cluster; Search Head. |

SOC4Kafka follows the same logical configuration count as the original OTel-ingestion design: the collector is one domain, whether configured as a manual package or deployed through another documented method. HEC input settings and HF parsing/routing are grouped in one HF domain, as in the baseline count; they may still be maintained in different files or deployment packages. Physical co-location would not merge the collector runtime with Splunkd configuration.

### Sources for this alternative

- [Kafka Messaging Modular Input on Splunkbase](https://splunkbase.splunk.com/app/1817) — app version, platform compatibility, modular-input description, forwarder installation, and Developer Supported status (listed update 2026-08-21).
- [Kafka on Splunkbase](https://splunkbase.splunk.com/app/5906) — confirms the similarly named Splunk app is a SOAR connector, with SOAR platform compatibility.
- [Splunk Connect for Kafka on Splunkbase](https://splunkbase.splunk.com/app/3862) — product description, HEC sink, Splunk Supported status, and at-least-once claim (last updated 2025-09-17).
- [Official Splunk Connect for Kafka 2.2 overview](https://help.splunk.com/en/splunk-cloud-platform/get-data-in/splunk-connect-for-kafka/2.2/overview/splunk-connect-for-kafka) — Kafka Connect sink identity, Kafka topic to HEC flow, current documentation version 2.2.2, and EOL 2027-10-31 (page updated 2026-11-09).
- [Official Splunk Connect for Kafka 2.2 release notes](https://help.splunk.com/en/splunk-cloud-platform/get-data-in/splunk-connect-for-kafka/2.2/release-notes/release-notes-for-splunk-connect-for-kafka) — version 2.2.2 and broad Splunk platform 7.1+ compatibility field; page updated 2026-11-09.
- [Splunk SOC4Kafka README](https://github.com/splunk/splunk-opentelemetry-collector-for-kafka/tree/main?tab=readme-ov-file#how-to-start-with-soc4kafka) — replacement status, Kafka/Splunk requirements, tested Kafka versions, supported platforms, acknowledgment and Protobuf limitations, configuration and installation methods (checked 2026-11-14).
- [SOC4Kafka documentation site](https://splunk.github.io/splunk-opentelemetry-collector-for-kafka/) — installation and advanced settings linked from the project README.

## Cribl Stream instead of SOC4Kafka, HFs, and EPs

**Checked: 2026-11-14.** The current Cribl Stream documentation is version 4.20.4. The target Splunk environment is Enterprise 10.x with clustered indexers; the exact deployed Cribl build and license are unknown. Per the user clarification, the assessment assumes customer-managed, distributed, on-prem Cribl Stream. Cribl's HEC documentation describes the integration generically and does not provide an explicit compatibility matrix for a specific Splunk Enterprise 10.x build, so validate the actual Cribl release and HEC endpoint configuration.

The side-by-side data path is shown in [splunk-kafka-cribl-stream-comparison.drawio](./splunk-kafka-cribl-stream-comparison.drawio).

The regional layouts for both deployment options are shown in [splunk-kafka-regional-architecture-options.drawio](./splunk-kafka-regional-architecture-options.drawio). The diagram infers three regions from the stated node counts and shows the shared indexer cluster once; central-service locations are placeholders.

```text
Kafka
  -> Cribl Stream Kafka Source
  -> Routes + Pipelines (parsing, event handling, transformations, filtering)
  -> Splunk HEC Destination
  -> HEC-enabled Splunk indexer endpoints or load balancer
  -> Indexer cluster -> Search Head
```

In this design Cribl replaces the SOC4Kafka collector, HF parsing/routing, and EP processing tiers. The indexer cluster and Search Head remain. Cribl's [Splunk HEC Destination](https://docs.cribl.io/stream/destinations-splunk-hec/) sends cooked/parsed data into Splunk's indexing segment, so HF parsing and EP transformations must be deliberately ported into Cribl; this is not a transparent component swap. Cribl's page supports multiple HEC endpoints and load balancing, but the correct HEC endpoints and token deployment for this indexer cluster must be confirmed against the Splunk environment.

### Comparative assessment

| Dimension | Cribl Stream replacing SOC4Kafka + HF + EP | SOC4Kafka + HF + EP | Assessment |
|---|---|---|---|
| **Capability and processing boundary** | One Stream control plane contains Kafka Sources, Routes, Pipelines/Functions, and the Splunk HEC Destination. Pipeline functions can filter, parse, reshape, enrich, and route data. The Kafka Source itself says **Event Breaker Support: No**; use an Event Breaker Function in a Pipeline if a Kafka record contains multiple logical events. | SOC4Kafka consumes and forwards to HEC; HF handles Splunk event breaking/parsing/routing; EP handles its pipeline transformations. | **Inference:** Cribl can consolidate the requested stages, but each of the user's eight transformations must be mapped and tested against Cribl functions. HF `props.conf`/`transforms.conf` and EP SPL2 logic may need translation, not copy/paste. |
| **Kafka onboarding** | The Kafka Source is pull-based and supports TLS, SASL (PLAIN, SCRAM, GSSAPI/Kerberos), OAuth, and Schema Registry for Avro/JSON. It automatically decompresses Gzip, Snappy, LZ4, and ZSTD. Cribl recommends one topic per Source and a unique consumer Group ID; set bootstrap servers, topic, offset/start behavior, credentials, and registry settings as needed. | SOC4Kafka's README presents a small YAML receiver/exporter config and supports Kafka 3.7+; Kafka auth, HEC details, and transformation ownership still have to be integrated with HF/EP configuration. | **Inference:** Cribl has richer onboarding controls in one UI, but more Kafka-specific choices. SOC4Kafka pass-through is simpler to start; the split HF/EP stages add work when onboarding includes parsing and routing. |
| **High volume and large records** | Kafka Source defaults include a 1 MiB per-partition fetch byte limit and a 10 MiB accumulated response byte limit; these are tunable consumer-fetch settings, **not** an end-to-end event-size guarantee. The Source has no built-in event breaker; the Event Breaker Function can split raw data up to about 128 MB. HEC supports persistent queues and backpressure policies, but Cribl's HEC docs say not to enable Splunk indexer acknowledgment. | SOC4Kafka also lacks acknowledgments; its README lists queue/batch configuration but no end-to-end maximum record size. HF, EP, and HEC limits still need alignment. | Both require end-to-end testing with representative maximum-size records and load. Cribl publishes a sizing guideline of up to 200 GB/day in+out per vCPU with hyperthreading; this is not a throughput guarantee for this Kafka/transform workload. |
| **Pipeline development** | Build ordered Functions in a visual Pipeline editor or manage the pipeline as JSON. Routes select the Pipeline and Destination. Event Breaker rules can be attached through an Event Breaker Function when the Source does not support source-level breaking. | HF parsing and routing use Splunk configuration and forwarder inputs/outputs; EP uses its own SPL2 pipeline and source/destination setup. | **Inference:** Cribl puts more of the data contract in one visible flow. The trade-off is learning Cribl's event model and function semantics; validate timestamps, field types, event boundaries, metadata, and the “Magnificent 8” one by one. |
| **Versioning and GitOps** | Every UI commit creates a local Git commit; changes can be reviewed/reverted and deployed to Worker Groups. External Git remotes and formal GitOps are documented. GitOps uses separate Dev and Production deployments/branches, keeps Production read-only, promotes through Git review, then syncs Production through CI/CD. | SOC4Kafka's YAML, HF app/config, and EP pipeline are separate change surfaces. They can each be versioned/automated with organizational tooling, but the sources checked do not document one built-in cross-component Git promotion workflow. | **Inference:** Cribl offers a more integrated change record and promotion path, at the cost of Cribl-specific setup and governance. Its GitOps documentation requires a distributed deployment and an applicable license tier; GitOps is not supported on Cribl.Cloud. The docs recommend Pack-based configuration management for a more flexible Git workflow across Worker Groups. |
| **Testing in INT** | Data Preview tests a Pipeline against sample data and shows IN/OUT results, diffs, and diagnostics. Simple Preview is capped at 10 MB. Worker Groups/Packs or a separate Dev/INT deployment can isolate configuration; GitOps explicitly documents Dev→Prod promotion. | SOC4Kafka configuration can be deployed separately for INT, while HF and EP changes must also be validated on their respective stages. No integrated SOC4Kafka→HF→EP preview/promotion workflow is documented in the sources checked. | **Inference:** Cribl offers the clearest built-in pipeline iteration loop. Preview is a sample-based logic check, not a Kafka load test or end-to-end delivery test. Use a separate INT Kafka topic or consumer group and a separate HEC token/index; a shared consumer group can rebalance when Sources or Workers change. |
| **Lifecycle and propagation** | In a distributed deployment, a Leader manages Worker Groups and distributes configuration; Workers pull bundles. Changes can be committed and deployed per Worker Group or globally. On-prem Worker Groups require manual upgrades; keep Leader/Workers version-compatible. | SOC4Kafka package/config, HF software and config, and EP pipeline each have their own deployment and maintenance steps. | **Inference:** Cribl reduces the number of processing tiers and change handoffs, but introduces a Cribl Leader/Worker fleet to size, monitor, secure, license, back up, and upgrade. Cribl docs also offer a Splunk App deployment for existing HFs; that option retains HFs and is not the replacement architecture assessed here. |
| **Configuration footprint** | In the same logical-surface counting model used above: Kafka; Cribl Stream; Splunk indexer cluster; Search Head. Within Stream, engineers still configure Kafka Sources, Routes/Pipelines, HEC Destinations, and Worker Group deployment/promotion. | Six logical surfaces: Kafka, SOC4Kafka, HF, EP, indexer cluster, and Search Head. | **Inference:** Cribl consolidates three processing products into one platform/control plane, reducing handoffs from six surfaces to four logical ownership surfaces, but not reducing the number of configuration objects to four. |

### Operational cautions for this topology

- **Kafka consumer behavior:** Offset commits default to each batch if both commit interval and threshold are unset. Deployments or Worker Process changes can trigger rebalances for Sources sharing a consumer group; Cribl recommends unique Group IDs for Sources. Stage an INT Source with isolated topic/group settings rather than experimenting against the production consumer group.
- **Event boundaries and size:** If each Kafka message is already one complete event, keep that boundary and parse the message in the Pipeline. If a single message contains multiple records, the Kafka Source's lack of source-level event-breaker support means a Pipeline Event Breaker Function is needed. Its approximately 128 MB ceiling and the Kafka fetch-byte settings are separate constraints. Validate large records through Cribl, HEC, and the Splunk receiver; no single maximum for the full path was found in the sources checked.
- **HEC delivery:** Cribl's HEC Destination supports load-balanced endpoints and persistent queues, but explicitly warns not to enable indexer acknowledgment because the required channel GUID is not sent. Persistent queues/backpressure are not equivalent to confirmed indexing. Exercise outages, retries, Kafka lag/offset behavior, and duplicate/loss acceptance in INT.
- **Splunk search/index behavior:** Because HEC output is cooked/parsed, set the intended index, source, sourcetype, host, and timestamp metadata in Cribl and verify the resulting events. Search-time knowledge on the Search Head remains separate.
- **Git workflow choice:** For self-hosted, distributed Cribl, consider Pack-based configuration management when the goal is to promote a full Source/Route/Pipeline/Destination bundle across INT and PROD Worker Groups. Use formal GitOps when separate, nearly identical Dev and Production deployments and a read-only production control plane are desired. Cribl.Cloud does not support GitOps; use its documented alternatives instead.
- **Sizing:** Cribl's published 200 GB/day in+out per vCPU figure is only an initial sizing guideline. CPU cost varies with parsing/regex/serialization and the number of destinations. Benchmark the actual record sizes, partition counts, transformations, HEC request limits, queue disk, and indexer response behavior.

### Delivery reliability and the “no event lost” requirement

**Applicability:** This assessment assumes the previously described on-prem Kafka → Cribl Stream → Splunk HEC design. Cribl's current public documentation, checked 2026-11-14, identifies the Kafka Source as a pull source; its exact installed build and the Splunk Enterprise version must still be verified.

**Decision:** Persistent Queues reduce loss risk, but the reviewed documentation does not establish a strict zero-loss guarantee for this path. Treat that requirement as **decision-blocked** until Kafka offset advancement is proven to follow durable downstream acceptance and the failure tests below pass.

#### Documented behavior and limits

- **Kafka Source PQ is not the available safeguard.** Cribl documents Kafka as a pull Source and limits Source Persistent Queues to supported push Sources; pull and collector Sources are excluded. Kafka topic retention and replay therefore remain the upstream recovery mechanism. Cribl documents that, when both Kafka offset commit interval and threshold are unset, offsets are committed after each batch. The documentation does not say that this commit waits for HEC indexing or a durable Destination-PQ write. Confirm the ordering for the deployed build with Cribl before claiming no-loss behavior. [Kafka Source](https://docs.cribl.io/stream/sources-kafka/#advanced-tab) · [Source Persistent Queues](https://docs.cribl.io/stream/persistent-queues-sources/#supported-sources)
- **HEC Destination PQ can absorb downstream backpressure.** The HEC Destination supports PQ. For a strict durability objective, evaluate **Always On** so events are written to the queue continuously; set queue-full behavior to **Block**, never **Drop**. Size the queue for peak uncompressed outbound bytes per second × the outage duration to survive, with headroom for uneven distribution. The queue is finite, and Cribl documents limitations for application failures, disk failure, and data corruption. [HEC Destination PQ settings](https://docs.cribl.io/stream/destinations-splunk-hec/#pq) · [Destination PQ guidance and limitations](https://docs.cribl.io/stream/persistent-queues-destinations/)
- **Choose storage for the worker failure domain.** A local queue on a lost Worker Node is not recoverable by another node. Cribl Stream supports shared NFS or S3-compatible storage for group-level recovery; all Workers in a group use the same storage type. Validate storage behavior, capacity, and recovery across the three-region layout, and do not clear or disable a queue before it drains. [Destination PQ storage choices](https://docs.cribl.io/stream/persistent-queues-destinations/#configure-the-storage-substrate-shared-or-local)
- **Indexer acknowledgment is not the safety net here.** Cribl's HEC Destination documentation says not to enable Indexer Acknowledgement on the Splunk token because requests can fail with a missing channel GUID. Cribl treats any HTTP `2xx` as success. **Inference:** with indexer acknowledgment disabled, a `2xx` must not be used as evidence that every event was indexed and searchable. [Cribl Splunk HEC Destination](https://docs.cribl.io/stream/destinations-splunk-hec/#indexer-acknowledgement)
- **Retry/PQ behavior needs a release-specific check.** The current HEC Destination page is inconsistent: its Retries section describes some `4xx` and `5xx` responses as retryable, while Advanced Settings says `4xx` responses are dropped by default and `5xx` retries do not trigger PQ. Confirm the intended retry list and behavior with Cribl and test it on the deployed build; do not rely on defaults or assume PQ protects every HTTP error. [HEC retries and Advanced Settings](https://docs.cribl.io/stream/destinations-splunk-hec/#retries)
- **Retries can duplicate events.** An ambiguous timeout can leave it unclear whether the receiver accepted a request; Cribl also documents that events in transit can emerge as duplicates. Use a stable event identity for reconciliation, and define duplicate handling rather than assuming exactly-once delivery. [Persistent Queue limitations](https://docs.cribl.io/stream/persistent-queues/)

#### Recommended safeguards

1. Keep Kafka records available longer than the worst planned outage plus backlog-drain and recovery time. Confirm Kafka-side durability, retention/compaction policy, consumer-group ownership, and a tested replay process with the Kafka owner.
2. Configure and size the HEC Destination PQ for the required outage window; use **Block** on queue full. Confirm that backpressure actually stops Kafka consumption without committing offsets past events not durably retained downstream.
3. Configure and verify the explicit retry policy for transient HEC failures. Treat permanent response errors as visible failures requiring replay or remediation, not as successful delivery. Alert on failed requests, drops, queue depth/age, free disk, and Kafka consumer lag.
4. Reconcile stable source event IDs (or a validated source identity) from Kafka through searchable Splunk data. Allow for duplicates caused by retry/replay; do not treat queue drain or an HEC `2xx` alone as end-to-end acceptance.
5. If zero missing events is a hard acceptance requirement, obtain written confirmation from Cribl about Kafka offset commits relative to HEC delivery and Destination-PQ persistence. If that guarantee is unavailable, retain a replayable source or durable independent archive and use a transport/path with documented end-to-end acknowledgment before making a zero-loss claim.

#### Acceptance and rollback gate

| Check | Evidence required to pass | Stop/rollback condition |
|---|---|---|
| Isolated canary on the intended Cribl build and topology | Stable IDs/counts reconciled from Kafka to indexed, searchable Splunk data; zero missing IDs; duplicates measured and explained | Any missing ID, unexplained offset advancement, silent drop, or wrong destination |
| Separately authorized failure/recovery tests | Queue and offset behavior recorded for HEC `429`, `5xx`, timeout, permanent `400`, queue-full, and Worker/storage loss; replay completes without missing IDs | Queue drops/fills without safe upstream hold, data becomes unrecoverable, or retries do not behave as approved |
| Rollback | Named owner; return to the last known-good path, preserve Kafka retention and undrained queues, and reconcile after rollback | Any missing event or unaccounted queued data; do not clear PQ as a recovery shortcut |

The Kafka owner supplies retention/replay evidence; the Cribl owner confirms offset, retry, and queue behavior; the Splunk owner validates indexed/searchable reconciliation. Cribl's documentation gaps and retry/PQ inconsistency require confirmation from Cribl before production acceptance.

**Bottom line — Inference:** Cribl Stream is a credible consolidation option when the primary goal is a single, Git-versioned processing and promotion plane for Kafka onboarding, parsing, routing, and transformations. It likely simplifies pipeline change review and INT→PROD promotion relative to coordinating SOC4Kafka, HF, and EP. It does **not** eliminate operational work: it shifts it to Cribl's own distributed runtime, and its HEC path lacks indexer acknowledgments. Whether this is simpler overall depends on Cribl deployment/licensing, the exact eight transformations, the Kafka message format/size, and ownership of the Cribl platform. No public apples-to-apples throughput or task-effort benchmark for this topology was found.

## Sources

- [Splunk Distribution of OpenTelemetry Collector for Kafka](https://help.splunk.com/en/splunk-observability-cloud/manage-data/splunk-distribution-of-the-opentelemetry-collector/get-started-with-the-splunk-distribution-of-the-opentelemetry-collector/collector-for-kafka) — Kafka consumption, Splunk HEC output, supported versions and limitations (updated 2026-10-13).
- [Configure the Collector for Kafka](https://help.splunk.com/en/splunk-observability-cloud/manage-data/splunk-distribution-of-the-opentelemetry-collector/get-started-with-the-splunk-distribution-of-the-opentelemetry-collector/collector-for-kafka/configure/overview) — receiver, exporter, queue, batching, and pipeline configuration (updated 2026-10-13).
- [Splunk HEC exporter](https://help.splunk.com/en/splunk-observability-cloud/manage-data/splunk-distribution-of-the-opentelemetry-collector/get-started-with-the-splunk-distribution-of-the-opentelemetry-collector/collector-components/exporters/splunk-hec-exporter) — HEC exporter settings, including event/content-size settings (updated 2026-09-09).
- [Cribl Stream Kafka Source](https://docs.cribl.io/stream/sources-kafka/) — pull mode, broker/topic/group settings, Kafka security and schema registry, compression, event-breaker support, fetch-byte defaults, offset commits, and rebalance behavior (Cribl Stream documentation 4.20.4; checked 2026-11-14).
- [Cribl Stream Splunk HEC Destination](https://docs.cribl.io/stream/destinations-splunk-hec/) — HEC endpoints, load balancing, backpressure, persistent queues, cooked/parsed indexing path, and indexer-acknowledgment restriction (checked 2026-11-14).
- [Cribl Persistent Queues](https://docs.cribl.io/stream/persistent-queues/) — queue behavior, finite capacity, failure limitations, and supported Sources/Destinations (checked 2026-11-14).
- [Optimize Destination Persistent Queues](https://docs.cribl.io/stream/persistent-queues-destinations/) — queue modes, queue-full behavior, storage choices, sizing, recovery, and monitoring (checked 2026-11-14).
- [Optimize Source Persistent Queues](https://docs.cribl.io/stream/persistent-queues-sources/) — Source-PQ applicability and behavior (checked 2026-11-14).
- [Destination Backpressure Triggers](https://docs.cribl.io/stream/destinations-backpressure-triggers/) — HTTP response, timeout, and backpressure conditions (checked 2026-11-14).
- [Cribl Stream Basic Concepts](https://docs.cribl.io/stream/basic-concepts/) and [Pipelines](https://docs.cribl.io/stream/pipelines/) — Sources, Routes, Pipelines, Functions, and distributed deployment model.
- [Cribl Event Breakers](https://docs.cribl.io/stream/event-breakers/) — source-level support boundary and Event Breaker Function behavior/limits.
- [Cribl Data Preview](https://docs.cribl.io/stream/data-preview/) — sample-based testing, preview cap, diagnostics, and IN/OUT views.
- [Cribl Commit and Deploy](https://docs.cribl.io/stream/commit-deploy/) — local Git commits, scoped deployment, worker bundle rollout, history, and reverts.
- [Cribl GitOps](https://docs.cribl.io/stream/gitops/) and [Pack-Based Configuration Management](https://docs.cribl.io/stream/pack-config-management-intro/) — GitOps applicability, environment promotion, licensing/deployment constraints, and Pack workflow.
- [Cribl On-Prem Deployment](https://docs.cribl.io/stream/deploy-types/) and [Distributed Deployment](https://docs.cribl.io/stream/deploy-distributed/) — Leader/Worker Groups, deployment options, and operations.
- [Cribl Upgrade Stream](https://docs.cribl.io/stream/upgrading/) and [Sizing and Scaling](https://docs.cribl.io/stream/scaling/) — version compatibility, upgrade ownership, and vendor sizing guidance.
- [Improving data ingestion using the Edge Processor solution](https://help.splunk.com/en/splunk-enterprise/get-started/get-data-in/10.0/improve-the-data-input-process/improving-data-ingestion-using-the-edge-processor-solution) — Enterprise 10.0 Edge Processor overview (updated 2026-03-19).
- [Draw a diagram of your deployment](https://help.splunk.com/en/splunk-enterprise/administer/inherit-a-splunk-deployment/10.4/inherited-deployment-tasks/draw-a-diagram-of-your-deployment) — Splunk-provided deployment icon set (updated 2026-05-09).
- [Data Management Solutions](https://help.splunk.com/en/data-management/onboard-data-to-splunk-cloud-platform/explore-data-management-solutions/data-management-solutions) — Enterprise EP availability, sources, destinations, and comparison with Ingest Actions (updated 2026-10-29).
- [How the Edge Processor solution transforms data](https://help.splunk.com/en/data-management/process-data-at-the-edge/use-edge-processors-for-splunk-cloud-platform/how-the-edge-processor-solution-works/how-the-edge-processor-solution-transforms-data) — source-specific event breaking and HF → EP transformation sequence (updated 2026-06-16; Cloud Platform manual).
- [Get data from a forwarder into an Edge Processor](https://help.splunk.com/en/splunk-cloud-platform/process-data-at-the-edge/use-edge-processors-for-splunk-cloud-platform/get-data-into-edge-processors/get-data-from-a-forwarder-into-an-edge-processor) — forwarder target-group wiring to EP (updated 2026-06-16).
- [Edge Processor Validated Architecture](https://docs.splunk.com/Documentation/SVA/latest/Architectures/EdgeProcessor) — forwarder-to-EP architecture and limitations.
- [Use Ingest Actions to improve the data input process](https://help.splunk.com/en/splunk-enterprise/forward-and-process-data/ingest-actions/use-ingest-actions-to-improve-the-data-input-process) and [Ingest Actions requirements](https://help.splunk.com/en/splunk-enterprise/forward-and-process-data/ingest-actions/requirements) — separate ruleset behavior and deployment requirements (updated 2026-03-25 and 2026-02-18).
