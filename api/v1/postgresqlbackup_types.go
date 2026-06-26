package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PostgreSQLBackupSpec defines the desired state of PostgreSQLBackup
type PostgreSQLBackupSpec struct {
	// ClusterName is the name of the PostgreSQLCluster to back up.
	// +kubebuilder:validation:MinLength=1
	ClusterName string `json:"clusterName"`

	// BarmanServerName is the Barman server name used for this backup.
	// If empty, the operator can use the cluster default.
	// +optional
	BarmanServerName string `json:"barmanServerName,omitempty"`

	// BackupType defines the backup type.
	// +kubebuilder:validation:Enum=full;incremental
	// +kubebuilder:default=full
	// +optional
	BackupType string `json:"backupType,omitempty"`

	// Immediate tells the operator to start the backup immediately.
	// +kubebuilder:default=true
	// +optional
	Immediate bool `json:"immediate,omitempty"`
}

// PostgreSQLBackupStatus defines the observed state of PostgreSQLBackup
type PostgreSQLBackupStatus struct {
	// Phase shows the backup lifecycle phase.
	// Example: Pending, Running, Completed, Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// StartedAt is the backup start time.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is the backup completion time.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// BackupID is the Barman backup ID after successful creation.
	// +optional
	BackupID string `json:"backupId,omitempty"`

	// Message contains human-readable status details.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=postgresqlbackups,scope=Namespaced,shortName=pgbackup
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.backupType`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Backup ID",type=string,JSONPath=`.status.backupId`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PostgreSQLBackup is the Schema for the postgresqlbackups API
type PostgreSQLBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgreSQLBackupSpec   `json:"spec,omitempty"`
	Status PostgreSQLBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PostgreSQLBackupList contains a list of PostgreSQLBackup
type PostgreSQLBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgreSQLBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgreSQLBackup{}, &PostgreSQLBackupList{})
}
