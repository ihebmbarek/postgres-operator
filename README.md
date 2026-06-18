# PG Guardian PostgreSQL Operator

PG Guardian is a Kubernetes/OpenShift Operator built with Kubebuilder to manage PostgreSQL clusters with backup, restore, connection pooling, monitoring, OLM packaging, and automatic failover.

The project was developed as a final-year engineering project to demonstrate how a custom Kubernetes Operator can automate the lifecycle of a production-style PostgreSQL workload on OpenShift.

---

## Project Overview

PG Guardian manages PostgreSQL as a Kubernetes-native custom resource.

Instead of manually creating StatefulSets, Services, Secrets, ConfigMaps, backup jobs, monitoring resources, and failover workflows, the user creates a single `PostgreSQLCluster` custom resource. The operator reconciles the desired state and creates all required Kubernetes/OpenShift resources automatically.

The operator supports:

* PostgreSQL StatefulSet lifecycle management
* Persistent storage through PVCs
* PostgreSQL credentials management through Secrets
* Custom PostgreSQL configuration through ConfigMaps
* Barman integration for backup and WAL archiving
* Scheduled backup CronJobs
* Point-in-Time Recovery preparation
* NodePort exposure for Barman connectivity
* HAProxy integration on the external Barman host
* PgBouncer connection pooling
* PostgreSQL exporter monitoring
* Prometheus ServiceMonitor and PrometheusRule resources
* OpenShift-compatible restricted security contexts
* OLM packaging and upgrade workflow
* Real standby PostgreSQL instance using streaming replication
* Automatic failover with standby promotion
* Service switch to the promoted standby
* Old primary fencing to prevent split-brain

---

## Current Release Status

Current main release:

```text
Operator version: v0.3.0
Operator image: quay.io/iheb_mbarek/postgres-operator:v0.3.0
Bundle image:   quay.io/iheb_mbarek/postgres-operator-bundle:v0.3.0
Catalog image:  quay.io/iheb_mbarek/postgres-operator-catalog:v0.3.3
```

The catalog image tag is `v0.3.3` because the catalog packaging was fixed after the `v0.3.0` operator release. The catalog still serves the operator CSV:

```text
postgres-operator.v0.3.0
```

Final verified OLM state:

```text
CSV: postgres-operator.v0.3.0
Phase: Succeeded
Deployment image: quay.io/iheb_mbarek/postgres-operator:v0.3.0
Controller pod: Running
```

---

## Repository Structure

Important project directories and files:

```text
api/v1/
  postgresqlcluster_types.go
  zz_generated.deepcopy.go

internal/controller/
  postgresqlcluster_controller.go

config/
  crd/
  manager/
  rbac/
  samples/

bundle/
  manifests/
  metadata/

catalog/
  index.yaml

docs/
  automatic-failover.md

Dockerfile
bundle.Dockerfile
catalog.Dockerfile
Makefile
go.mod
go.sum
README.md
```

### Key File Responsibilities

| File                                                            | Purpose                                                                              |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `api/v1/postgresqlcluster_types.go`                             | Defines the PostgreSQLCluster CRD spec and status                                    |
| `internal/controller/postgresqlcluster_controller.go`           | Main reconcile logic for PostgreSQL, backups, PgBouncer, monitoring, HA and failover |
| `config/samples/database_v1_postgresqlcluster.yaml`             | Example custom resource                                                              |
| `bundle/manifests/postgres-operator.clusterserviceversion.yaml` | OLM ClusterServiceVersion                                                            |
| `catalog/index.yaml`                                            | File-based catalog definition                                                        |
| `docs/automatic-failover.md`                                    | Detailed automatic failover documentation                                            |

---

## Custom Resource: PostgreSQLCluster

The operator introduces the following custom resource:

```text
apiVersion: database.iheb.local/v1
kind: PostgreSQLCluster
```

The CR describes the desired PostgreSQL cluster configuration.

Example:

```yaml
apiVersion: database.iheb.local/v1
kind: PostgreSQLCluster
metadata:
  name: sample-postgres
  namespace: alpha
spec:
  postgresVersion: "16"
  image: quay.io/iheb_mbarek/postgres-barman:latest

  database: appdb
  user: appuser

  storageSize: 5Gi
  storageClass: nfs-client

  resources:
    requests:
      cpu: 250m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi

  backup:
    enabled: true
    barmanHost: 192.168.180.54
    barmanUser: barman
    sshSecretName: sample-postgres-barman-ssh
    archiveTimeout: 60
    barmanServerName: sample-postgres
    exposeService: true
    nodePort: 30433
    schedule: "0 2 * * *"
    suspendScheduledBackups: false
    backupImage: quay.io/iheb_mbarek/postgres-barman:latest
    replicationAllowedCIDR: 10.128.0.0/14
    streamingUser: streaming_barman

  pgbouncer:
    enabled: true
    replicas: 1
    image: edoburu/pgbouncer:latest
    poolMode: transaction
    maxClientConn: 100
    defaultPoolSize: 20

  highAvailability:
    enabled: true
    replicas: 2
    failoverMode: Automatic
    detectionTimeoutSeconds: 30
```

---

## Main Managed Resources

For a PostgreSQLCluster named `sample-postgres`, the operator can create and manage:

```text
StatefulSet/sample-postgres
StatefulSet/sample-postgres-standby
Service/sample-postgres
Service/sample-postgres-barman
Secret/sample-postgres-credentials
Secret/sample-postgres-streaming-credentials
ConfigMap/sample-postgres-config
CronJob/sample-postgres-barman-backup
Deployment/sample-postgres-pgbouncer
Service/sample-postgres-pgbouncer
Deployment/sample-postgres-exporter
Service/sample-postgres-exporter
ServiceMonitor/sample-postgres-exporter
PrometheusRule/sample-postgres-database-alerts
Job/sample-postgres-promote-standby
```

---

## Architecture

The architecture is composed of the following main layers:

```text
User / DBA
   |
   v
PostgreSQLCluster Custom Resource
   |
   v
PG Guardian Operator Controller
   |
   +--> PostgreSQL primary StatefulSet
   +--> PostgreSQL standby StatefulSet
   +--> PostgreSQL service
   +--> PgBouncer deployment
   +--> Barman backup integration
   +--> CronJob scheduled backups
   +--> Monitoring resources
   +--> Automatic failover workflow
```

External backup architecture:

```text
PostgreSQL Pod
   |
   | WAL archive / streaming replication
   v
NodePort Service
   |
   v
OpenShift Node
   |
   v
HAProxy on Barman host
   |
   v
Barman Server
```

---

## PostgreSQL Lifecycle Management

The operator creates a PostgreSQL StatefulSet with:

* Persistent volume claim
* PostgreSQL container image
* Custom environment variables
* Credentials from Kubernetes Secret
* PostgreSQL configuration mounted from ConfigMap
* Liveness and readiness probes
* Restricted security context for OpenShift compatibility

The primary pod is normally named:

```text
sample-postgres-0
```

The primary service is:

```text
sample-postgres
```

This service is used by clients and PgBouncer to reach PostgreSQL.

---

## PostgreSQL Configuration

The operator generates PostgreSQL configuration using ConfigMaps.

Important PostgreSQL settings include:

```text
listen_addresses = '*'
wal_level = replica
archive_mode = on
archive_timeout = 60
max_wal_senders = 5
hba_file = /etc/postgresql/pg_hba.conf
```

When Barman backup is enabled, the operator configures WAL archiving using an archive command that sends WAL files to the external Barman server.

---

## Credentials

The operator manages several credentials:

### Application credentials

Stored in:

```text
Secret/sample-postgres-credentials
```

Used for:

```text
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

### Streaming replication credentials

Stored in:

```text
Secret/sample-postgres-streaming-credentials
```

Used by the standby pod to connect to the primary using streaming replication.

### SSH credentials for Barman

Stored in a Kubernetes Secret specified by:

```yaml
backup:
  sshSecretName: sample-postgres-barman-ssh
```

This secret contains the SSH private key and known hosts configuration required to connect to the external Barman server.

---

## Barman Backup Integration

Barman is used as the external PostgreSQL backup and WAL archive system.

The operator supports:

* WAL archiving
* Streaming replication user creation
* Scheduled backups using CronJobs
* Backup status reporting
* NodePort exposure for external Barman access

Barman server name example:

```text
sample-postgres
```

Typical Barman configuration on the external server:

```text
conninfo = host=127.0.0.1 port=15433 user=barman_backup dbname=appdb
streaming_conninfo = host=127.0.0.1 port=15433 user=streaming_barman
backup_method = postgres
archiver = on
streaming_archiver = on
slot_name = sample_postgres_slot
create_slot = auto
```

---

## HAProxy and NodePort

Because the Barman server is outside the OpenShift cluster, the PostgreSQL service is exposed using a NodePort.

Example NodePort:

```text
30433
```

The external Barman host uses HAProxy to forward a local port to the OpenShift NodePort.

Example flow:

```text
Barman localhost:15433
   |
   v
HAProxy
   |
   v
OpenShift node IP:30433
   |
   v
sample-postgres-barman service
   |
   v
PostgreSQL pod port 5432
```

This allows Barman to connect to PostgreSQL as if it were local while the database remains inside OpenShift.

---

## Scheduled Backups

The operator creates a CronJob when backup scheduling is enabled.

Example schedule:

```yaml
backup:
  schedule: "0 2 * * *"
```

This creates:

```text
CronJob/sample-postgres-barman-backup
```

The CronJob runs a backup image and triggers Barman backup commands.

The operator also stores backup-related status fields in the PostgreSQLCluster status.

---

## Point-in-Time Recovery

The project includes PITR preparation and validation using Barman.

The general PITR workflow is:

1. PostgreSQL archives WAL files to Barman.
2. Barman stores base backups and WAL history.
3. A restore target time can be selected.
4. Barman restores the database to the desired point.
5. The restored database is queried to prove that PITR succeeded.

PITR was validated manually during the project and treated as a controlled recovery operation.

---

## PgBouncer Connection Pooling

The operator supports optional PgBouncer deployment.

Example configuration:

```yaml
pgbouncer:
  enabled: true
  replicas: 1
  image: edoburu/pgbouncer:latest
  poolMode: transaction
  maxClientConn: 100
  defaultPoolSize: 20
```

Created resources:

```text
Deployment/sample-postgres-pgbouncer
Service/sample-postgres-pgbouncer
ConfigMap/sample-postgres-pgbouncer
```

PgBouncer connects to the PostgreSQL service and provides a pooled connection endpoint for applications.

---

## Monitoring and Alerts

The project integrates PostgreSQL monitoring using:

* PostgreSQL exporter
* ServiceMonitor
* PrometheusRule

Managed resources include:

```text
Deployment/sample-postgres-exporter
Service/sample-postgres-exporter
ServiceMonitor/sample-postgres-exporter
PrometheusRule/sample-postgres-database-alerts
```

The operator also includes monitoring for its own controller manager when installed through OLM.

Example alerts include:

```text
PGGuardianOperatorDown
PostgreSQLDown
PostgreSQLExporterDown
```

---

## OpenShift Security

The operator was adjusted to work with OpenShift restricted security requirements.

Security improvements include:

* Non-root compatible containers
* Restricted security contexts
* No unnecessary privileged permissions
* RBAC permissions for required Kubernetes resources
* Idempotent reconciliation to avoid unnecessary rollouts

The controller manager runs through OLM in the namespace:

```text
pg-guardian-olm
```

---

## Reconcile Idempotency

The controller was improved to avoid repeatedly updating resources when no meaningful change exists.

A stable pod template hash annotation is used to detect real pod template changes.

This prevents unnecessary rolling updates for:

* PostgreSQL StatefulSet
* PgBouncer Deployment
* Other managed workloads

---

## High Availability

High availability is enabled using:

```yaml
highAvailability:
  enabled: true
  replicas: 2
  failoverMode: Automatic
  detectionTimeoutSeconds: 30
```

When enabled, the operator creates:

```text
StatefulSet/sample-postgres
StatefulSet/sample-postgres-standby
```

The standby pod is:

```text
sample-postgres-standby-0
```

The standby initializes itself from the primary using:

```text
pg_basebackup
```

Then it remains in streaming replication mode.

---

## Streaming Replication

The standby connects to the primary using the streaming replication user.

The primary allows replication connections through `pg_hba.conf`.

The allowed network CIDR is configured using:

```yaml
backup:
  replicationAllowedCIDR: 10.128.0.0/14
```

This value should match the OpenShift pod network CIDR.

Validation commands:

```bash
oc exec -n alpha sample-postgres-standby-0 -- \
  psql -U appuser -d appdb -Atqc "SELECT pg_is_in_recovery();"
```

Expected while standby is still standby:

```text
t
```

Primary replication validation:

```bash
oc exec -n alpha sample-postgres-0 -- \
  psql -U appuser -d appdb -c "SELECT application_name, client_addr, state, sync_state FROM pg_stat_replication;"
```

Expected:

```text
walreceiver | standby pod IP | streaming | async
```

---

## Automatic Failover

Automatic failover is the main feature introduced in `v0.3.0`.

The workflow is:

1. Operator detects that the primary pod is unavailable.
2. Operator starts a detection timeout.
3. If the primary recovers before timeout, no promotion happens.
4. If the timeout expires, the operator creates a promotion Job.
5. The promotion Job runs `pg_promote()` against the standby.
6. The standby exits recovery mode and becomes writable.
7. The normal PostgreSQL service switches to the promoted standby.
8. The old primary remains fenced at zero replicas.

Promotion command:

```sql
SELECT pg_promote(true, 60);
```

Promotion job:

```text
Job/sample-postgres-promote-standby
```

---

## Old Primary Fencing

After failover, the original primary is intentionally kept stopped.

Example final state:

```text
sample-postgres           0/0
sample-postgres-standby   1/1
```

This avoids split-brain, where two PostgreSQL instances could both accept writes independently.

The old primary is not automatically restarted.

Old primary rejoin is intentionally manual and must be done safely by rebuilding or restoring it from the promoted primary.

---

## Service Switch After Failover

Before failover, the normal PostgreSQL service selects the primary:

```yaml
selector:
  app: sample-postgres
  app.kubernetes.io/component: postgres
  managed: postgres-operator
```

After automatic failover, the operator switches the service to the promoted standby:

```yaml
selector:
  app: sample-postgres
  app.kubernetes.io/component: postgres-standby
  database.iheb.local/role: standby
  managed: postgres-operator
```

This allows applications to continue using the normal service name:

```text
sample-postgres
```

After promotion, this service points to the promoted standby.

---

## Final Automatic Failover Proof

The final verified state was:

```text
Primary StatefulSet: sample-postgres 0/0
Standby StatefulSet: sample-postgres-standby 1/1
Service endpoint: sample-postgres -> promoted standby IP
Failover phase: Promoted
HA phase: FailoverPromoted
```

Database proof:

```sql
SELECT pg_is_in_recovery();
```

Expected after promotion:

```text
f
```

Write proof:

```sql
INSERT INTO failover_test(note)
VALUES ('managed service switched to promoted standby');

SELECT count(*) FROM failover_test;
```

Expected:

```text
2
```

---

## OLM Packaging

The operator is packaged for OpenShift Operator Lifecycle Manager.

Main OLM artifacts:

```text
bundle/
catalog/
bundle.Dockerfile
catalog.Dockerfile
```

The final OLM release uses:

```text
Operator image: quay.io/iheb_mbarek/postgres-operator:v0.3.0
Bundle image:   quay.io/iheb_mbarek/postgres-operator-bundle:v0.3.0
Catalog image:  quay.io/iheb_mbarek/postgres-operator-catalog:v0.3.3
```

The catalog exposes:

```text
stable -> postgres-operator.v0.3.0
```

The upgrade edge is:

```text
postgres-operator.v0.3.0 replaces postgres-operator.v0.2.5
```

---

## OLM Installation / Upgrade

CatalogSource example:

```yaml
apiVersion: operators.coreos.com/v1alpha1
kind: CatalogSource
metadata:
  name: pg-guardian-catalog
  namespace: openshift-marketplace
spec:
  sourceType: grpc
  image: quay.io/iheb_mbarek/postgres-operator-catalog:v0.3.3
  displayName: PG Guardian Catalog
  publisher: ihebmbarek
```

Subscription example:

```yaml
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: postgres-operator
  namespace: pg-guardian-olm
spec:
  channel: stable
  name: postgres-operator
  source: pg-guardian-catalog
  sourceNamespace: openshift-marketplace
  installPlanApproval: Automatic
```

Final verified OLM state:

```text
CSV: postgres-operator.v0.3.0
Phase: Succeeded
Deployment image: quay.io/iheb_mbarek/postgres-operator:v0.3.0
Controller pod: Running
```

---

## Build and Push Operator Image

```bash
export IMG=quay.io/iheb_mbarek/postgres-operator:v0.3.0

podman build -t $IMG .

podman push $IMG
```

---

## Build and Push Bundle Image

```bash
export VERSION=0.3.0
export IMG=quay.io/iheb_mbarek/postgres-operator:v0.3.0
export BUNDLE_IMG=quay.io/iheb_mbarek/postgres-operator-bundle:v0.3.0

make bundle IMG=$IMG VERSION=$VERSION

podman build -f bundle.Dockerfile -t $BUNDLE_IMG .

podman push $BUNDLE_IMG
```

---

## Build and Push Catalog Image

```bash
export CATALOG_IMG=quay.io/iheb_mbarek/postgres-operator-catalog:v0.3.3

podman build -f catalog.Dockerfile -t $CATALOG_IMG .

podman push $CATALOG_IMG
```

---

## Important Verification Commands

Check PostgreSQLCluster status:

```bash
oc get postgresqlcluster sample-postgres -n alpha
```

Check HA status:

```bash
oc get postgresqlcluster sample-postgres -n alpha \
  -o jsonpath='{.status.haPhase}{" | "}{.status.failoverPhase}{" | "}{.status.failoverReason}{"\n"}'
```

Check workloads:

```bash
oc get statefulset sample-postgres sample-postgres-standby -n alpha
oc get pods -n alpha | grep sample-postgres
```

Check service selector:

```bash
oc get svc sample-postgres -n alpha -o yaml | sed -n '/selector:/,/ports:/p'
```

Check service endpoint:

```bash
oc get endpoints sample-postgres -n alpha -o wide
```

Check OLM CSV:

```bash
oc get csv -n pg-guardian-olm
```

Check deployed operator image:

```bash
oc get deployment postgres-operator-controller-manager \
  -n pg-guardian-olm \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

---

## Release History

| Version         | Main Content                                                                     |
| --------------- | -------------------------------------------------------------------------------- |
| v0.1.0          | Initial OLM packaging                                                            |
| v0.2.0 - v0.2.5 | Backup, monitoring, PgBouncer, security, OLM improvements                        |
| v0.3.0          | Real standby, automatic failover, promotion, service switch, old primary fencing |
| catalog v0.3.3  | Fixed stable channel and upgrade edge for OLM upgrade to v0.3.0                  |

Important commits:

```text
e5cfc49 Add standby StatefulSet for HA replication
af72c15 Add automatic standby promotion and service switch
2f6e9de Add post-failover fencing guard
2a35ea8 Document automatic failover workflow
75a7818 Merge automatic failover feature
9f405ec Release OLM bundle and catalog v0.3.0
9cb4f16 Fix catalog image serve command
```

---

## Current Limitations

The current project is complete for demonstration and final-year project purposes, but the following limitations remain:

* Only one standby is promoted.
* Automatic old-primary rejoin is not implemented.
* After failover, rebuilding a new standby is manual.
* Multi-standby leader selection is not implemented.
* Synchronous replication is not implemented.
* Advanced backup retention policies are managed mainly through Barman.
* Production-grade disaster recovery would require additional automation and operational procedures.

---

## Future Work

Possible future improvements:

* Automatic rebuild of a new standby after failover
* Safe old-primary rejoin workflow
* Multiple standby support
* Failover candidate selection
* Synchronous replication option
* Backup retention policy automation
* Web dashboard for backup/failover status
* Alertmanager integration
* Automated restore CRD
* Grafana dashboards
* Disaster recovery runbooks

---

## Safety Notes

After automatic failover, do not manually scale the old primary back to one replica unless it has been safely rebuilt.

Unsafe command after failover:

```bash
oc scale statefulset sample-postgres -n alpha --replicas=1
```

This can cause split-brain.

Correct behavior after failover:

```text
sample-postgres primary StatefulSet remains 0/0
sample-postgres-standby remains 1/1
sample-postgres service points to promoted standby
```

---

## Project Final State

The PG Guardian Operator now provides an end-to-end PostgreSQL management solution for OpenShift:

```text
PostgreSQL lifecycle management: complete
Backup and WAL archiving: complete
Barman integration: complete
Scheduled backups: complete
PITR workflow: validated
PgBouncer pooling: complete
Monitoring and alerts: complete
OpenShift security compatibility: complete
OLM packaging and upgrade: complete
Automatic failover: complete
Service switch after promotion: complete
Split-brain protection: complete
```

The operator is technically complete for the final project demonstration.
