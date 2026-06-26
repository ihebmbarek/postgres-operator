/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupSpec defines the Barman backup configuration for PostgreSQL.
type BackupSpec struct {
	// Enabled indicates whether Barman backup integration is enabled.
	Enabled bool `json:"enabled,omitempty"`

	// BarmanHost is the IP address or hostname of the external Barman backup server.
	BarmanHost string `json:"barmanHost,omitempty"`

	// BarmanUser is the SSH user used to connect to the Barman server.
	BarmanUser string `json:"barmanUser,omitempty"`

	// BarmanServerName is the server name configured on the Barman backup machine.
	BarmanServerName string `json:"barmanServerName,omitempty"`

	// SSHSecretName is the Kubernetes Secret containing the SSH private key for Barman access.
	SSHSecretName string `json:"sshSecretName,omitempty"`

	// ArchiveTimeout defines how often PostgreSQL should force WAL segment switching, in seconds.
	ArchiveTimeout int32 `json:"archiveTimeout,omitempty"`

	// ExposeService indicates whether the operator should create
	// an external NodePort Service for the Barman server.
	ExposeService bool `json:"exposeService,omitempty"`

	// NodePort is the external port exposed on each OpenShift node
	// for Barman database and streaming connections.
	// +kubebuilder:validation:Minimum=30000
	// +kubebuilder:validation:Maximum=32767
	NodePort int32 `json:"nodePort,omitempty"`

	// Schedule defines when the operator-managed backup CronJob runs.
	// Example: "0 2 * * *".
	Schedule string `json:"schedule,omitempty"`

	// SuspendScheduledBackups pauses the CronJob without deleting it.
	SuspendScheduledBackups bool `json:"suspendScheduledBackups,omitempty"`

	// BackupImage optionally overrides the image used by the backup CronJob.
	BackupImage string `json:"backupImage,omitempty"`

	// ReplicationAllowedCIDR restricts physical replication access to the
	// trusted Barman server network. Example: "192.168.180.54/32".
	ReplicationAllowedCIDR string `json:"replicationAllowedCIDR,omitempty"`

	// StreamingUser is the PostgreSQL role used by Barman for pg_basebackup
	// and pg_receivewal. When empty, the operator defaults to "streaming_barman".
	StreamingUser string `json:"streamingUser,omitempty"`
}

// RestoreSpec defines a requested Barman Point-in-Time Recovery operation.
type RestoreSpec struct {
	// Enabled indicates whether the operator should perform a restore operation.
	// Keep this value false during normal PostgreSQL operation.
	Enabled bool `json:"enabled,omitempty"`

	// RequestID uniquely identifies a restore operation.
	// Use a new value for each restore request so the operator does not
	// execute the same destructive restore repeatedly.
	// Example: "demo-restore-001".
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[a-z0-9][a-z0-9-]*$`
	RequestID string `json:"requestId,omitempty"`

	// Confirmation is an explicit safety acknowledgement required before the
	// operator replaces the current PostgreSQL data directory.
	// The expected format is: "RESTORE <cluster-name>".
	// Example: "RESTORE sample-postgres".
	Confirmation string `json:"confirmation,omitempty"`

	// BackupID identifies the Barman base backup used as the restore starting point.
	// Example: "20260611T203128".
	// When empty, the controller may use the latest available backup.
	BackupID string `json:"backupId,omitempty"`

	// TargetTime is the desired PITR recovery timestamp.
	// PostgreSQL replays archived WAL records until this timestamp is reached.
	// Example: "2026-06-11T16:31:35Z".
	TargetTime *metav1.Time `json:"targetTime,omitempty"`

	// TargetAction defines the action PostgreSQL performs after reaching
	// the requested recovery target.
	// +kubebuilder:validation:Enum=promote;pause;shutdown
	TargetAction string `json:"targetAction,omitempty"`

	// RestoreImage optionally overrides the image used by the restore Job.
	// When empty, the operator uses the configured PostgreSQL image.
	RestoreImage string `json:"restoreImage,omitempty"`

	// PreserveExistingData indicates whether the current PGDATA directory
	// should be preserved before restoring the selected Barman backup.
	PreserveExistingData bool `json:"preserveExistingData,omitempty"`
}

// PgBouncerSpec defines the optional PgBouncer connection pooler configuration.
type PgBouncerSpec struct {
	// Enabled controls whether the operator deploys PgBouncer.
	Enabled bool `json:"enabled,omitempty"`

	// Replicas defines the number of PgBouncer pods.
	Replicas int32 `json:"replicas,omitempty"`

	// Image is the PgBouncer container image.
	Image string `json:"image,omitempty"`

	// PoolMode defines the PgBouncer pooling mode: session, transaction, or statement.
	PoolMode string `json:"poolMode,omitempty"`

	// MaxClientConn defines the maximum number of client connections accepted by PgBouncer.
	MaxClientConn int32 `json:"maxClientConn,omitempty"`

	// DefaultPoolSize defines the default number of server connections per pool.
	DefaultPoolSize int32 `json:"defaultPoolSize,omitempty"`
}

// HighAvailabilitySpec defines the optional HA and failover-readiness configuration.
// This phase prepares the operator for a primary/standby architecture without
// performing unsafe automatic promotion by default.
type HighAvailabilitySpec struct {
	// Enabled indicates whether HA readiness mode is enabled.
	Enabled bool `json:"enabled,omitempty"`

	// Replicas is the desired PostgreSQL topology size.
	// A value of 1 means single primary.
	// A value of 2 or more represents a primary plus standby-ready architecture.
	Replicas int32 `json:"replicas,omitempty"`

	// FailoverMode defines how failover should be handled.
	// Supported initial values: Disabled, Manual, SemiAutomatic.
	FailoverMode string `json:"failoverMode,omitempty"`

	// DetectionTimeoutSeconds defines how long the operator should tolerate
	// an unhealthy primary before reporting failover as required.
	DetectionTimeoutSeconds int32 `json:"detectionTimeoutSeconds,omitempty"`
}

// PostgreSQLClusterSpec defines the desired state of PostgreSQLCluster.

// PostgreSQLTLSSpec defines PostgreSQL server-side TLS configuration.
type PostgreSQLTLSSpec struct {
	// Enabled enables PostgreSQL SSL/TLS.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// SecretName is the Kubernetes Secret containing tls.crt, tls.key, and optionally ca.crt.
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// SSLMode is the libpq sslmode used by generated clients such as Barman, standby and PgBouncer.
	// Typical values: disable, require, verify-ca, verify-full.
	// +optional
	SSLMode string `json:"sslMode,omitempty"`

	// RequireClientCert enables client certificate authentication.
	// Keep false for the first implementation.
	// +optional
	RequireClientCert bool `json:"requireClientCert,omitempty"`
}

type PostgreSQLClusterSpec struct {
	// TLS configures PostgreSQL server-side SSL/TLS.
	// +optional
	TLS PostgreSQLTLSSpec `json:"tls,omitempty"`

	PostgresVersion string `json:"postgresVersion,omitempty"`

	Image string `json:"image,omitempty"`

	Database string `json:"database,omitempty"`

	User string `json:"user,omitempty"`

	StorageSize string `json:"storageSize,omitempty"`

	StorageClassName string `json:"storageClassName,omitempty"`

	CPURequest string `json:"cpuRequest,omitempty"`

	CPULimit string `json:"cpuLimit,omitempty"`

	MemoryRequest string `json:"memoryRequest,omitempty"`

	MemoryLimit string `json:"memoryLimit,omitempty"`

	// Backup defines the optional Barman backup configuration.
	Backup BackupSpec `json:"backup,omitempty"`

	// Restore defines an optional Barman PITR request.
	// Restore must remain disabled during normal operation.
	Restore RestoreSpec `json:"restore,omitempty"`

	// PgBouncer defines the optional connection pooler configuration.
	PgBouncer PgBouncerSpec `json:"pgbouncer,omitempty"`

	// HighAvailability defines optional HA readiness and failover detection settings.
	HighAvailability HighAvailabilitySpec `json:"highAvailability,omitempty"`
}

// PostgreSQLClusterStatus defines the observed state of PostgreSQLCluster.
// ComplianceFinding describes one CIS-aligned security/compliance control result.
type ComplianceFinding struct {
	// ID is the stable identifier of the compliance control.
	ID string `json:"id,omitempty"`

	// Title is a short human-readable control name.
	Title string `json:"title,omitempty"`

	// Severity describes the risk level when the control fails.
	// Example values: critical, high, medium, low.
	Severity string `json:"severity,omitempty"`

	// Status is Pass or Fail.
	Status string `json:"status,omitempty"`

	// Message provides evidence or remediation guidance.
	Message string `json:"message,omitempty"`
}

type PostgreSQLClusterStatus struct {
	Phase string `json:"phase,omitempty"`

	Ready bool `json:"ready,omitempty"`

	// CompliancePhase describes the overall CIS-aligned compliance state.
	// Example values: Passed, Warning.
	CompliancePhase string `json:"compliancePhase,omitempty"`

	// ComplianceScore shows how many controls passed, for example "8/8".
	ComplianceScore string `json:"complianceScore,omitempty"`

	// ComplianceFindings lists CIS-aligned security control results.
	ComplianceFindings []ComplianceFinding `json:"complianceFindings,omitempty"`

	PostgresPod string `json:"postgresPod,omitempty"`

	// BackupEnabled indicates whether backup configuration is enabled.
	BackupEnabled bool `json:"backupEnabled,omitempty"`

	// BackupPhase describes the current backup configuration state.
	BackupPhase string `json:"backupPhase,omitempty"`

	BarmanService  string `json:"barmanService,omitempty"`
	BarmanNodePort int32  `json:"barmanNodePort,omitempty"`

	LastBackupStatus string       `json:"lastBackupStatus,omitempty"`
	LastBackupTime   *metav1.Time `json:"lastBackupTime,omitempty"`
	LastBackupID     string       `json:"lastBackupId,omitempty"`

	// BackupCronJob is the generated CronJob responsible for scheduled backups.
	BackupCronJob string `json:"backupCronJob,omitempty"`

	// BackupSchedule is the active CronJob schedule.
	BackupSchedule string `json:"backupSchedule,omitempty"`

	// RestoreEnabled indicates whether a restore request is active.
	RestoreEnabled bool `json:"restoreEnabled,omitempty"`

	// ObservedRestoreRequestID records the restore request currently processed
	// or most recently completed by the operator.
	ObservedRestoreRequestID string `json:"observedRestoreRequestId,omitempty"`

	// RestorePhase describes the current PITR workflow state.
	// Example values: Disabled, Pending, Preparing, Restoring, Completed, Failed.
	RestorePhase string `json:"restorePhase,omitempty"`

	// RestoreMessage provides a human-readable description of the latest
	// restore workflow event or failure.
	RestoreMessage string `json:"restoreMessage,omitempty"`

	// RestoreJob is the name of the generated Kubernetes Job responsible
	// for executing the PITR restore workflow.
	RestoreJob string `json:"restoreJob,omitempty"`

	// RestoreBackupID is the Barman base backup selected for the restore.
	RestoreBackupID string `json:"restoreBackupId,omitempty"`

	// RestoreTargetTime is the requested PostgreSQL PITR timestamp.
	RestoreTargetTime *metav1.Time `json:"restoreTargetTime,omitempty"`

	// LastRestoreTime records when the most recent successful restore completed.
	LastRestoreTime *metav1.Time `json:"lastRestoreTime,omitempty"`

	// PgBouncerEnabled indicates whether PgBouncer is enabled for this cluster.
	PgBouncerEnabled bool `json:"pgbouncerEnabled,omitempty"`

	// PgBouncerPhase describes the current PgBouncer deployment state.
	PgBouncerPhase string `json:"pgbouncerPhase,omitempty"`

	// PgBouncerService is the generated Service used by applications to connect through PgBouncer.
	PgBouncerService string `json:"pgbouncerService,omitempty"`

	// HAEnabled indicates whether high availability readiness is enabled.
	HAEnabled bool `json:"haEnabled,omitempty"`

	// HAPhase describes the current HA topology state.
	// Example values: Disabled, SinglePrimary, StandbyPlanned, Ready, Degraded.
	HAPhase string `json:"haPhase,omitempty"`

	// PrimaryPod is the current PostgreSQL primary pod observed by the operator.
	PrimaryPod string `json:"primaryPod,omitempty"`

	// StandbyPods lists standby pod names expected or observed by the operator.
	StandbyPods []string `json:"standbyPods,omitempty"`

	// FailoverPhase describes the current failover-readiness state.
	// Example values: Disabled, Healthy, PrimaryUnavailable, RecoveryRequired.
	FailoverPhase string `json:"failoverPhase,omitempty"`

	// FailoverReason provides a human-readable explanation of the latest failover state.
	FailoverReason string `json:"failoverReason,omitempty"`

	// LastPrimaryFailureTime records when the operator last detected primary unavailability.
	LastPrimaryFailureTime *metav1.Time `json:"lastPrimaryFailureTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Postgres Pod",type=string,JSONPath=`.status.postgresPod`
// +kubebuilder:printcolumn:name="Backup",type=boolean,JSONPath=`.status.backupEnabled`
// +kubebuilder:printcolumn:name="Restore",type=string,JSONPath=`.status.restorePhase`
// +kubebuilder:printcolumn:name="PgBouncer",type=string,JSONPath=`.status.pgbouncerPhase`
// +kubebuilder:printcolumn:name="HA",type=string,JSONPath=`.status.haPhase`
// +kubebuilder:printcolumn:name="Failover",type=string,JSONPath=`.status.failoverPhase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PostgreSQLCluster is the Schema for the postgresqlclusters API.
type PostgreSQLCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgreSQLClusterSpec   `json:"spec,omitempty"`
	Status PostgreSQLClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PostgreSQLClusterList contains a list of PostgreSQLCluster.
type PostgreSQLClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgreSQLCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgreSQLCluster{}, &PostgreSQLClusterList{})
}
