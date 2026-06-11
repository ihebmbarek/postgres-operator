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

	Schedule                string `json:"schedule,omitempty"`
	SuspendScheduledBackups bool   `json:"suspendScheduledBackups,omitempty"`
	BackupImage             string `json:"backupImage,omitempty"`
}

// PostgreSQLClusterSpec defines the desired state of PostgreSQLCluster.
type PostgreSQLClusterSpec struct {
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
}

// PostgreSQLClusterStatus defines the observed state of PostgreSQLCluster.
type PostgreSQLClusterStatus struct {
	Phase string `json:"phase,omitempty"`

	Ready bool `json:"ready,omitempty"`

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
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Postgres Pod",type=string,JSONPath=`.status.postgresPod`
// +kubebuilder:printcolumn:name="Backup",type=boolean,JSONPath=`.status.backupEnabled`
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
