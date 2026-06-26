# PG Guardian — PostgreSQL Operator for OpenShift/Kubernetes

PG Guardian is a Kubernetes/OpenShift Operator that manages PostgreSQL clusters as cloud-native workloads. It automates the creation and reconciliation of PostgreSQL resources, integrates backup and restore workflows with Barman, exposes monitoring metrics, enforces security controls, and generates SRE-oriented SLO/SLI alerts.

The project was built as a final-year engineering project to demonstrate Kubernetes Operator development, PostgreSQL administration, OpenShift deployment, backup strategy, monitoring, security compliance, and SRE practices.

---

## Table of Contents

* [Overview](#overview)
* [Main Features](#main-features)
* [Architecture](#architecture)
* [Custom Resources](#custom-resources)
* [Current Release](#current-release)
* [Container Images](#container-images)
* [Deployment Modes](#deployment-modes)
* [Example PostgreSQLCluster](#example-postgresqlcluster)
* [Backup and PITR](#backup-and-pitr)
* [TLS Security](#tls-security)
* [CIS-Aligned Compliance](#cis-aligned-compliance)
* [SLO/SLI Monitoring](#slosli-monitoring)
* [PgBouncer](#pgbouncer)
* [OLM Installation](#olm-installation)
* [Useful Verification Commands](#useful-verification-commands)
* [Proofs](#proofs)
* [Project Status](#project-status)
* [Future Work](#future-work)

---

## Overview

PG Guardian introduces a custom Kubernetes resource named `PostgreSQLCluster`.
When a `PostgreSQLCluster` object is created, the operator reconciles the required Kubernetes resources to run PostgreSQL on OpenShift/Kubernetes.

The operator manages:

* PostgreSQL `StatefulSet`
* PostgreSQL services
* Persistent storage
* Database credentials
* PostgreSQL configuration
* Barman backup integration
* Backup CronJobs
* PgBouncer connection pooling
* PostgreSQL exporter monitoring
* Prometheus alert rules
* TLS enforcement
* CIS-aligned compliance status
* SLO/SLI Prometheus rules
* OLM packaging and upgrades

---

## Main Features

### PostgreSQL Lifecycle Management

The operator creates and reconciles the core PostgreSQL workload:

* `StatefulSet`
* headless service
* optional NodePort/Barman service
* persistent volume claim
* credentials secret
* configuration configmap

### Backup Integration

PG Guardian integrates with Barman using SSH-based WAL archiving and scheduled backups.

Supported backup-related capabilities:

* Barman SSH key secret
* WAL archiving configuration
* streaming replication user
* backup CronJob generation
* backup status fields
* backup proofs
* manual PITR validation workflow

### TLS Enforcement

The operator supports PostgreSQL TLS configuration through Kubernetes secrets.

Implemented behavior:

* mounts TLS certificate and key
* prepares runtime TLS files
* enables PostgreSQL SSL
* rejects non-SSL client connections when configured
* verifies encrypted PostgreSQL sessions through `pg_stat_ssl`

### PgBouncer

The operator can deploy PgBouncer as a connection pooler for PostgreSQL.

Managed resources:

* PgBouncer Deployment
* PgBouncer Service
* PgBouncer ConfigMap
* PgBouncer status in the `PostgreSQLCluster`

### Monitoring

The project includes PostgreSQL and operator monitoring resources:

* PostgreSQL exporter
* PostgreSQL exporter ServiceMonitor
* PostgreSQL database PrometheusRule
* operator metrics ServiceMonitor
* operator health alerts

### CIS-Aligned Compliance

PG Guardian evaluates a set of CIS-aligned workload security controls and exposes the result in the custom resource status.

Implemented checks:

* PostgreSQL TLS enabled
* pod runs as non-root
* privilege escalation disabled
* containers are not privileged
* Linux capabilities dropped
* seccomp RuntimeDefault enabled
* PostgreSQL CPU and memory resources configured
* credentials stored in Kubernetes Secret

Example status:

```text
compliancePhase: Passed
complianceScore: 8/8
```

### SLO/SLI Monitoring

PG Guardian generates SRE-style PrometheusRule objects per PostgreSQL cluster.

Generated SLO/SLI alerts include:

* PostgreSQL availability SLO breach
* backup freshness SLO breach
* backup job failure
* PgBouncer availability SLO breach

Example generated rule:

```text
demo-postgres-slo-alerts
```

---

## Architecture

```text
+---------------------------+
| PostgreSQLCluster CR      |
| database.iheb.local/v1    |
+-------------+-------------+
              |
              v
+---------------------------+
| PG Guardian Operator      |
| controller-runtime / Go   |
+-------------+-------------+
              |
              +-------------------------------+
              |                               |
              v                               v
+---------------------------+     +---------------------------+
| PostgreSQL Workload       |     | Backup Integration        |
| StatefulSet / Service     |     | Barman / CronJob / SSH    |
| PVC / Secret / ConfigMap  |     | WAL Archiving / PITR      |
+-------------+-------------+     +-------------+-------------+
              |                               |
              v                               v
+---------------------------+     +---------------------------+
| PgBouncer                 |     | Monitoring                |
| Deployment / Service      |     | Exporter / ServiceMonitor |
+-------------+-------------+     | PrometheusRule            |
              |                   +-------------+-------------+
              v                                 |
+---------------------------+                   v
| Security and SRE          |     +---------------------------+
| TLS / CIS / SLO / SLI     |     | OpenShift Monitoring      |
+---------------------------+     +---------------------------+
```

---

## Custom Resources

The operator manages the following custom resources:

```text
PostgreSQLCluster
PostgreSQLBackup
PostgreSQLRestore
```

Main API group:

```text
database.iheb.local/v1
```

---

## Current Release

Current tested OLM release:

```text
postgres-operator.v0.3.4
```

Release highlights:

* PostgreSQL lifecycle reconciliation
* Barman backup integration
* PgBouncer support
* TLS enforcement
* CIS-aligned compliance status
* SLO/SLI PrometheusRule generation
* OLM packaging and upgrade path

---

## Container Images

Manager image:

```text
quay.io/iheb_mbarek/postgres-operator:v0.3.4
```

Bundle image:

```text
quay.io/iheb_mbarek/postgres-operator-bundle:v0.3.4
```

Catalog image:

```text
quay.io/iheb_mbarek/postgres-operator-catalog:v0.3.4
```

---

## Deployment Modes

PG Guardian supports two deployment modes.

### Development Deployment

Used during local development:

```bash
make deploy IMG=quay.io/iheb_mbarek/postgres-operator:<tag>
```

Development namespace:

```text
postgres-operator-system
```

### OLM Deployment

Used for production-like installation through Operator Lifecycle Manager.

OLM namespace:

```text
pg-guardian-olm
```

CatalogSource namespace:

```text
openshift-marketplace
```

---

## Example PostgreSQLCluster

```yaml
apiVersion: database.iheb.local/v1
kind: PostgreSQLCluster
metadata:
  name: demo-postgres
  namespace: alpha
spec:
  postgresVersion: "16"
  image: quay.io/iheb_mbarek/postgres-barman:latest

  database: appdb
  user: appuser

  storageSize: 5Gi
  storageClass: nfs-client

  cpuRequest: 250m
  cpuLimit: 500m
  memoryRequest: 256Mi
  memoryLimit: 512Mi

  tls:
    enabled: true
    secretName: demo-postgres-tls
    sslMode: require

  backup:
    enabled: true
    barmanHost: 192.168.180.54
    barmanUser: barman
    sshSecretName: barman-ssh-key
    archiveTimeout: "60"
    barmanServerName: demo-postgres
    exposeService: true
    nodePort: 30433
    schedule: "0 2 * * *"
    suspendScheduledBackups: false
    replicationAllowedCIDR: 0.0.0.0/0
    streamingUser: streaming_barman

  pgbouncer:
    enabled: true
    replicas: 1
    poolMode: session
    maxClientConn: 100
    defaultPoolSize: 20
```

---

## Backup and PITR

PG Guardian integrates PostgreSQL with Barman for backup and recovery workflows.

Implemented backup mechanisms:

* WAL archiving
* streaming archiver
* backup CronJob
* Barman SSH integration
* backup status in the custom resource
* proof-based backup validation

Example checks:

```bash
oc get cronjob -n alpha

oc get pods -n alpha | grep backup

oc get postgresqlcluster demo-postgres -n alpha \
  -o jsonpath='{.status.backupEnabled}{"\n"}{.status.backupPhase}{"\n"}{.status.backupCronJob}{"\n"}'
```

Manual PITR was validated as part of the project proof workflow. Full automatic PITR orchestration can be extended as future work.

---

## TLS Security

TLS is enabled through the `spec.tls` section of the `PostgreSQLCluster`.

The operator:

* mounts the Kubernetes TLS secret
* prepares runtime certificate files
* enables SSL in PostgreSQL
* updates PostgreSQL authentication rules
* enforces encrypted client connections

Verification:

```bash
oc exec -it demo-postgres-0 -n alpha -- psql -U appuser -d appdb -c "SHOW ssl;"

oc exec -it demo-postgres-0 -n alpha -- psql -U appuser -d appdb -c \
"SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid();"
```

Expected:

```text
ssl
-----
on

ssl
-----
t
```

---

## CIS-Aligned Compliance

The operator evaluates security controls and writes the result to the `PostgreSQLCluster` status.

Check compliance status:

```bash
oc get postgresqlcluster demo-postgres -n alpha \
  -o jsonpath='{.status.compliancePhase}{"\n"}{.status.complianceScore}{"\n"}'
```

Expected:

```text
Passed
8/8
```

List findings:

```bash
oc get postgresqlcluster demo-postgres -n alpha \
  -o jsonpath='{range .status.complianceFindings[*]}{.id}{" | "}{.status}{" | "}{.severity}{" | "}{.title}{"\n"}{end}'
```

Example output:

```text
CIS-PG-001 | Pass | high | PostgreSQL TLS must be enabled
CIS-K8S-001 | Pass | high | Pod must run as non-root
CIS-K8S-002 | Pass | critical | Privilege escalation must be disabled
CIS-K8S-003 | Pass | critical | Containers must not run privileged
CIS-K8S-004 | Pass | high | Linux capabilities must be dropped
CIS-K8S-005 | Pass | medium | Seccomp RuntimeDefault must be used
CIS-K8S-006 | Pass | medium | PostgreSQL container resources must be configured
CIS-PG-002 | Pass | high | Database credentials must be stored in Kubernetes Secret
```

---

## SLO/SLI Monitoring

PG Guardian generates SLO/SLI Prometheus alerts for every PostgreSQL cluster.

The generated resource name follows this pattern:

```text
<cluster-name>-slo-alerts
```

Example:

```text
demo-postgres-slo-alerts
```

Check the generated SLO rule:

```bash
oc get prometheusrule demo-postgres-slo-alerts -n alpha
```

List generated alerts:

```bash
oc get prometheusrule demo-postgres-slo-alerts -n alpha \
  -o jsonpath='{range .spec.groups[*].rules[*]}{.alert}{" | "}{.labels.sli}{" | "}{.labels.slo}{" | "}{.labels.severity}{"\n"}{end}'
```

Expected alerts:

```text
PGGuardianPostgresAvailabilitySLOBreach_DEMO_POSTGRES | postgres_availability | 99.9_percent_availability | critical
PGGuardianBackupFreshnessSLOBreach_DEMO_POSTGRES | backup_freshness | successful_backup_within_24h | warning
PGGuardianBackupJobFailed_DEMO_POSTGRES | backup_success | backup_jobs_must_succeed | warning
PGGuardianPgBouncerAvailabilitySLOBreach_DEMO_POSTGRES | pgbouncer_availability | pooler_must_be_available | warning
```

---

## PgBouncer

PgBouncer is managed by the operator when enabled in the custom resource.

Check PgBouncer resources:

```bash
oc get deploy,svc,configmap -n alpha | grep pgbouncer
```

Check status:

```bash
oc get postgresqlcluster demo-postgres -n alpha \
  -o jsonpath='{.status.pgBouncerPhase}{"\n"}'
```

Expected:

```text
Available
```

---

## OLM Installation

The operator is packaged and installed through Operator Lifecycle Manager.

Current CatalogSource:

```text
pg-guardian-catalog
```

Catalog namespace:

```text
openshift-marketplace
```

Install namespace:

```text
pg-guardian-olm
```

Patch CatalogSource to the latest catalog image:

```bash
oc patch catalogsource pg-guardian-catalog \
  -n openshift-marketplace \
  --type merge \
  -p '{"spec":{"image":"quay.io/iheb_mbarek/postgres-operator-catalog:v0.3.4"}}'
```

Check CatalogSource:

```bash
oc get catalogsource pg-guardian-catalog -n openshift-marketplace
```

Check CSV:

```bash
oc get csv -n pg-guardian-olm | grep postgres-operator
```

Expected:

```text
postgres-operator.v0.3.4   PG Guardian operator   0.3.4   postgres-operator.v0.3.3   Succeeded
```

Check running operator image:

```bash
oc get deploy postgres-operator-controller-manager \
  -n pg-guardian-olm \
  -o jsonpath='{.spec.template.spec.containers[*].image}{"\n"}'
```

Expected:

```text
quay.io/iheb_mbarek/postgres-operator:v0.3.4
```

---

## Useful Verification Commands

Check operators:

```bash
oc get deploy -A | grep -E "postgres-operator|pg-guardian"
```

Expected production-like state:

```text
pg-guardian-olm            postgres-operator-controller-manager   1/1
postgres-operator-system   postgres-operator-controller-manager   0/0
```

Check PostgreSQLCluster:

```bash
oc get postgresqlcluster -n alpha
```

Check full custom resource status:

```bash
oc get postgresqlcluster demo-postgres -n alpha -o yaml
```

Check CIS compliance:

```bash
oc get postgresqlcluster demo-postgres -n alpha \
  -o jsonpath='{.status.compliancePhase}{"\n"}{.status.complianceScore}{"\n"}'
```

Check SLO rule:

```bash
oc get prometheusrule demo-postgres-slo-alerts -n alpha
```

Check OLM RBAC for PrometheusRule:

```bash
oc auth can-i create prometheusrules.monitoring.coreos.com \
  --as=system:serviceaccount:pg-guardian-olm:postgres-operator-controller-manager \
  -n alpha
```

Expected:

```text
yes
```

---

## Proofs

The repository contains proof files collected during development and OLM validation.

Important proof directories:

```text
proofs/cis-compliance-v1/
proofs/olm-v0.3.3-cis/
proofs/slo-sli-v1/
proofs/olm-v0.3.4-slo-sli/
```

These proofs include:

* custom resource status
* CIS findings
* SLO/SLI PrometheusRule YAML
* OLM CatalogSource state
* OLM Subscription state
* ClusterServiceVersion state
* operator Deployment YAML
* RBAC validation
* final OLM upgrade validation

---

## Project Status

Completed:

* PostgreSQL Operator core reconciliation
* PostgreSQL StatefulSet and service management
* persistent storage management
* credentials and configuration management
* Barman backup integration
* manual PITR validation workflow
* PgBouncer deployment
* PostgreSQL exporter monitoring
* operator monitoring
* TLS enforcement
* CIS-aligned compliance status
* SLO/SLI PrometheusRule generation
* OLM packaging
* OLM upgrade path up to `v0.3.4`

Current validated release:

```text
v0.3.4
```

Current validated result:

```text
CIS: Passed 8/8
SLO/SLI: demo-postgres-slo-alerts generated
OLM: postgres-operator.v0.3.4 Succeeded
```

---

## Future Work

Possible future improvements:

* fully automated PITR orchestration through `PostgreSQLRestore`
* automatic failover and standby promotion
* multi-replica PostgreSQL topology
* advanced SLO burn-rate alerts
* Grafana dashboards
* backup retention policy management
* disaster recovery automation
* multi-namespace tenant management
* admission webhooks for stronger validation
* end-to-end test automation for OLM installation

---

## License

This project was developed as part of a final-year engineering project.
