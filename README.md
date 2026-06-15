# PG Guardian — PostgreSQL Operator on OpenShift

PG Guardian is a custom Kubernetes/OpenShift Operator built with Kubebuilder to automate the deployment, configuration, backup, recovery, connection pooling, and monitoring of PostgreSQL workloads on OpenShift.

The project was developed as part of a PFE/internship project and focuses on building an operator-managed PostgreSQL platform with Barman backup integration, Point-in-Time Recovery, PgBouncer connection pooling, and OpenShift-native monitoring and alerting.

---

## 1. Project Objectives

The objective of this project is to design and implement a PostgreSQL Operator capable of managing the complete lifecycle of a PostgreSQL database cluster on OpenShift.

The operator automates:

* PostgreSQL deployment using StatefulSet
* Persistent storage using PVC
* Service creation and network exposure
* PostgreSQL configuration management
* Secret management
* WAL archiving with Barman
* Scheduled backups
* Point-in-Time Recovery
* PgBouncer connection pooling
* PostgreSQL database monitoring
* Operator monitoring and alerting
* Status reporting through the PostgreSQLCluster custom resource

---

## 2. Technologies Used

| Technology             | Role                                                    |
| ---------------------- | ------------------------------------------------------- |
| Go                     | Operator implementation                                 |
| Kubebuilder            | Operator framework                                      |
| Kubernetes / OpenShift | Target orchestration platform                           |
| PostgreSQL 16          | Managed database                                        |
| Barman                 | Backup, WAL archiving, and PITR source                  |
| HAProxy                | TCP bridge between Barman server and OpenShift NodePort |
| PgBouncer              | PostgreSQL connection pooling                           |
| Prometheus             | Metrics collection                                      |
| ServiceMonitor         | OpenShift monitoring integration                        |
| PrometheusRule         | Alerting rules                                          |
| GitHub Actions         | CI/CD validation                                        |
| Quay.io                | Operator image registry                                 |

---

## 3. Repository Structure

```text
postgres-operator/
├── api/
│   └── v1/
│       └── postgresqlcluster_types.go
├── cmd/
│   └── main.go
├── config/
│   ├── crd/
│   ├── default/
│   ├── manager/
│   ├── monitoring/
│   ├── rbac/
│   └── samples/
├── docs/
│   └── proofs/
├── internal/
│   └── controller/
│       └── postgresqlcluster_controller.go
├── .github/
│   └── workflows/
│       └── ci.yaml
├── Dockerfile
├── Makefile
├── go.mod
└── README.md
```

---

## 4. Custom Resource

The operator introduces the following custom resource:

```text
apiVersion: database.iheb.local/v1
kind: PostgreSQLCluster
```

The custom resource defines the desired state of a PostgreSQL cluster, including:

* PostgreSQL version and image
* Database name and user
* Storage size and storage class
* CPU and memory requests/limits
* Backup configuration
* Restore configuration
* PgBouncer configuration

The operator continuously reconciles this desired state with the actual state inside OpenShift.

---

## 5. Managed Resources

For each PostgreSQLCluster, the operator manages the following resources:

| Resource         | Purpose                             |
| ---------------- | ----------------------------------- |
| StatefulSet      | Runs the PostgreSQL pod             |
| PVC              | Stores PostgreSQL data persistently |
| Secret           | Stores database credentials         |
| ConfigMap        | Stores PostgreSQL configuration     |
| Headless Service | Internal PostgreSQL access          |
| NodePort Service | External Barman access              |
| CronJob          | Scheduled Barman backups            |
| Job              | PITR restore job                    |
| Job              | Post-restore stabilization job      |
| Deployment       | PgBouncer connection pooler         |
| Service          | PgBouncer access                    |
| ConfigMap        | PgBouncer configuration             |

---

## 6. Architecture Overview

The project architecture is divided into several layers:

```text
Users / Applications
        |
        v
PgBouncer Service :6432
        |
        v
PgBouncer Deployment
        |
        v
PostgreSQL Service :5432
        |
        v
PostgreSQL StatefulSet / Pod
        |
        v
Persistent Volume Claim
```

Backup and recovery flow:

```text
PostgreSQL Pod
        |
        | WAL archiving over SSH
        v
External Barman Server
        |
        | TCP access through HAProxy
        v
OpenShift NodePort Service
        |
        v
PostgreSQL Pod
```

Monitoring flow:

```text
Operator Metrics Service
        |
        v
ServiceMonitor
        |
        v
OpenShift Prometheus
        |
        v
PrometheusRule Alerts
```

Database monitoring flow:

```text
PostgreSQL Pod
        |
        v
postgres_exporter
        |
        v
ServiceMonitor
        |
        v
OpenShift Prometheus
```

---

## 7. PostgreSQL Deployment

The operator deploys PostgreSQL as a StatefulSet.

Main features:

* Persistent storage using PVC
* PostgreSQL configuration mounted from ConfigMap
* Credentials loaded from Secret
* Readiness and liveness probes
* Resource requests and limits
* WAL configuration
* Replication user configuration for Barman

Example managed pod:

```text
sample-postgres-0
```

Example database:

```text
Database: appdb
User: appuser
```

---

## 8. Barman Backup Integration

The operator integrates PostgreSQL with an external Barman server.

Main components:

* SSH key authentication
* Known hosts verification
* WAL archiving using `archive_command`
* NodePort Service for PostgreSQL access from the Barman host
* HAProxy TCP forwarding from the Barman server to OpenShift nodes
* Scheduled backups using CronJob

External Barman server:

```text
192.168.180.54
```

HAProxy frontend:

```text
127.0.0.1:15433
```

OpenShift NodePort:

```text
30433
```

---

## 9. Scheduled Backups

The operator creates a Kubernetes CronJob for scheduled Barman backups.

Example CronJob:

```text
sample-postgres-barman-backup
```

Example schedule:

```text
0 2 * * *
```

The backup status is exposed in the PostgreSQLCluster status fields:

```text
backupEnabled
backupPhase
backupCronJob
backupSchedule
lastBackupId
lastBackupStatus
lastBackupTime
barmanService
barmanNodePort
```

---

## 10. Point-in-Time Recovery

The operator supports Point-in-Time Recovery using Barman.

The PITR workflow is:

1. User enables restore in the PostgreSQLCluster custom resource
2. Operator validates the restore request
3. Operator scales down PostgreSQL
4. Restore Job retrieves backup data from Barman
5. PostgreSQL starts in recovery mode
6. WAL files are replayed until the target time
7. PostgreSQL promotes the restored database
8. Stabilization Job resets Barman streaming state
9. Operator updates the restore status to Completed

Restore status phases:

```text
ScalingDown
Restoring
StartingPostgreSQL
Stabilizing
Completed
Failed
Blocked
```

Clean PITR validation proved that:

```text
BEFORE_PITR_TARGET remained
AFTER_PITR_TARGET_SHOULD_DISAPPEAR disappeared
```

Proof files are stored in:

```text
docs/proofs/pitr-clean-test/
```

---

## 11. PgBouncer Connection Pooling

The operator supports PgBouncer as a connection pooling layer in front of PostgreSQL.

Managed resources:

* PgBouncer ConfigMap
* PgBouncer Deployment
* PgBouncer Service

Default configuration:

```text
pool_mode = transaction
max_client_conn = 100
default_pool_size = 20
```

PgBouncer service:

```text
sample-postgres-pgbouncer
```

Connection example:

```bash
psql -h sample-postgres-pgbouncer -p 6432 -U appuser -d appdb
```

The PgBouncer integration was validated after PITR and after the PostgreSQL Service selector fix.

Proof files are stored in:

```text
docs/proofs/selector-fix/
```

---

## 12. PostgreSQL Service Selector Fix

A critical issue was discovered and fixed.

Problem:

The PostgreSQL Service selector was too broad and selected:

```text
sample-postgres-0
sample-postgres-pgbouncer
sample-postgres-exporter
```

This caused PgBouncer and monitoring pods to appear as PostgreSQL endpoints.

Fix:

The operator now uses component-specific labels.

PostgreSQL Service selector:

```yaml
selector:
  app: sample-postgres
  app.kubernetes.io/component: postgres
  managed: postgres-operator
```

PgBouncer Service selector:

```yaml
selector:
  app: sample-postgres
  app.kubernetes.io/component: pgbouncer
  database.iheb.local/pgbouncer: sample-postgres
  managed: postgres-operator
```

The fix was released in:

```text
quay.io/iheb_mbarek/postgres-operator:v0.2.3
```

---

## 13. Monitoring and Alerting

The project integrates with OpenShift monitoring.

Operator monitoring includes:

* Secure metrics endpoint
* ServiceMonitor
* PrometheusRule
* Alert for operator availability

Database monitoring includes:

* postgres_exporter
* ServiceMonitor
* PrometheusRule
* PostgreSQL health metric

Important metrics:

```promql
pg_up
pg_database_size_bytes
controller_runtime_reconcile_total
```

Custom alerts:

```text
PGGuardianOperatorDown
PGGuardianPostgreSQLDown
```

Alerts and metrics can be viewed in the OpenShift Console:

```text
Observe → Metrics
Observe → Alerting
Observe → Targets
```

---

## 14. CI/CD

The repository includes a GitHub Actions CI workflow.

Workflow file:

```text
.github/workflows/ci.yaml
```

The CI pipeline validates:

* Go dependencies
* Unit tests
* envtest binaries
* controller-gen installation
* Kubernetes manifest generation
* Generated file consistency

The workflow runs on:

```text
push to main
pull requests to main
```

Current CI status:

```text
Passing
```

---

## 15. Build and Push Operator Image

Build the operator image:

```bash
podman build -t quay.io/iheb_mbarek/postgres-operator:v0.2.3 .
```

Push the image:

```bash
podman push quay.io/iheb_mbarek/postgres-operator:v0.2.3
```

---

## 16. Deploy the Operator

Install CRDs:

```bash
make install
```

Deploy the operator:

```bash
make deploy IMG=quay.io/iheb_mbarek/postgres-operator:v0.2.3
```

Check the operator:

```bash
oc get pods -n postgres-operator-system
```

---

## 17. Deploy a PostgreSQLCluster

Apply the sample custom resource:

```bash
oc apply -f config/samples/database_v1_postgresqlcluster.yaml
```

Check the cluster:

```bash
oc get postgresqlcluster -n alpha
oc get pods -n alpha
oc get svc -n alpha
```

---

## 18. Useful Validation Commands

Check PostgreSQLCluster status:

```bash
oc get postgresqlcluster sample-postgres -n alpha
```

Check PostgreSQL pod:

```bash
oc get pod sample-postgres-0 -n alpha
```

Check PostgreSQL Service endpoint:

```bash
oc get endpoints sample-postgres -n alpha -o yaml
```

Check PgBouncer:

```bash
oc get svc sample-postgres-pgbouncer -n alpha
oc get endpoints sample-postgres-pgbouncer -n alpha -o yaml
```

Test PgBouncer connection:

```bash
oc run pgbouncer-test \
  -n alpha \
  --rm \
  --attach=true \
  --image=postgres:16 \
  --restart=Never \
  --env PGPASSWORD=postgres \
  --command -- psql \
    -h sample-postgres-pgbouncer \
    -p 6432 \
    -U appuser \
    -d appdb \
    -c "SELECT current_database(), current_user, now();"
```

Check monitoring resources:

```bash
oc get servicemonitor -A
oc get prometheusrule -A
```

---

## 19. Proofs and Validation

Validation proofs are stored under:

```text
docs/proofs/
```

Important proof directories:

```text
docs/proofs/pitr-clean-test/
docs/proofs/selector-fix/
docs/proofs/monitoring-alerting/
```

These files document:

* PITR before/after state
* Restore status
* PostgreSQL health
* PgBouncer validation
* Clean PostgreSQL Service endpoints
* Operator image deployment
* Monitoring and alerting proof

---

## 20. Current Stable Version

Current stable operator image:

```text
quay.io/iheb_mbarek/postgres-operator:v0.2.3
```

Latest important commits:

```text
a8acfeb Install envtest binaries in CI
2ea7c9f Add GitHub Actions CI workflow
d28143e Fix PostgreSQL service selector and add PITR validation proofs
0d70051 Fix PgBouncer deployment RBAC and prove final operator image
8ff3ccb Add PostgreSQL database monitoring with postgres exporter
```

---

## 21. Known Limitations

Current limitations:

* Multi-replica PostgreSQL high availability is not implemented yet
* Automatic failover is not implemented yet
* OLM packaging is generated but not finalized
* PITR requires a valid existing Barman backup
* The current design uses an external Barman server
* Some infrastructure values such as Barman server IP and NodePort are environment-specific

---

## 22. Future Work

Planned improvements:

* Finalize OLM bundle and catalog packaging
* Add OperatorHub-compatible installation
* Implement PostgreSQL standby replicas
* Add automatic failover
* Improve restore history tracking
* Add more automated tests
* Add dashboard JSON for Grafana/OpenShift dashboards
* Add configurable TLS for PostgreSQL and PgBouncer
* Add advanced backup retention policies
* Add automated disaster recovery test workflow

---

## 23. Conclusion

PG Guardian demonstrates a production-oriented PostgreSQL Operator design on OpenShift.

The project successfully automates PostgreSQL deployment, backup, restore, PITR, monitoring, alerting, and connection pooling. It also includes real-world debugging and hardening, such as the PostgreSQL Service selector fix that prevents PgBouncer and exporter pods from being selected as database endpoints.

The current version is technically stable and ready for demonstration, documentation, and final presentation.

