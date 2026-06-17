# PG Guardian Automatic Failover

## Overview

PG Guardian supports automatic PostgreSQL failover when high availability is enabled. The operator creates a primary PostgreSQL StatefulSet and a standby PostgreSQL StatefulSet. The standby is initialized from the primary using pg_basebackup and remains in streaming replication mode until a primary failure is detected.

When the primary becomes unavailable, the operator waits for a configurable detection timeout. If the primary does not recover during that period, the operator promotes the standby using PostgreSQL pg_promote(), switches the managed PostgreSQL service to the promoted standby, and keeps the old primary fenced at zero replicas to prevent split-brain.

## High-level flow

1. The operator reconciles the primary StatefulSet.
2. If high availability is enabled, the operator creates a standby StatefulSet.
3. The standby initializes itself from the primary using streaming replication.
4. The operator monitors primary readiness.
5. If the primary becomes unavailable, the operator starts the failover detection timer.
6. If the primary recovers before the timeout, no promotion happens.
7. If the timeout expires, the operator creates a promotion Job.
8. The promotion Job executes SELECT pg_promote(true, 60); against the standby.
9. The standby becomes writable and leaves recovery mode.
10. The normal PostgreSQL service is switched to the promoted standby.
11. The old primary remains fenced at zero replicas to avoid split-brain.

## Configuration example

highAvailability:
  enabled: true
  replicas: 2
  failoverMode: Automatic
  detectionTimeoutSeconds: 30

## Safety behavior

After automatic failover, the old primary is intentionally not restarted automatically. This prevents split-brain, where two PostgreSQL instances could both accept writes independently.

The old primary rejoin process is treated as a controlled manual recovery operation. In a production-grade workflow, the old primary should be rebuilt from the promoted primary or restored from a safe backup/recovery procedure before being allowed to rejoin the topology.

## Verified behavior

The implementation was validated with the following proof points:

- Primary StatefulSet scaled to zero during failure simulation.
- Standby remained running and ready.
- Operator waited for the configured detection timeout.
- Promotion Job completed successfully.
- pg_promote() returned true.
- pg_is_in_recovery() returned false on the promoted standby.
- The managed sample-postgres service switched to the promoted standby.
- Write operations succeeded through the normal sample-postgres service after failover.

## Current limitations

- The operator promotes a single standby.
- Old primary rejoin is manual.
- The promoted standby becomes the active primary, but a new replacement standby is not automatically rebuilt yet.
- This implementation is suitable for demonstration and controlled failover scenarios, while production environments should add automated rejoin/rebuild workflows.
