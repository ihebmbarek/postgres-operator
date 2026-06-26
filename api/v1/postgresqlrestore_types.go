package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PostgreSQLRestoreSpec defines the desired state of PostgreSQLRestore
type PostgreSQLRestoreSpec struct {
	// ClusterName is the name of the PostgreSQLCluster to restore.
	// +kubebuilder:validation:MinLength=1
	ClusterName string `json:"clusterName"`

	// BarmanServerName is the Barman server name used for restore.
	// If empty, the operator can use the cluster default.
	// +optional
	BarmanServerName string `json:"barmanServerName,omitempty"`

	// BackupID is the Barman backup ID to restore from.
	// +optional
	BackupID string `json:"backupId,omitempty"`

	// TargetTime is the point-in-time recovery target.
	// Example: "2026-06-18 16:30:00"
	// +optional
	TargetTime string `json:"targetTime,omitempty"`

	// TargetDatabaseName is the database name after restore.
	// +optional
	TargetDatabaseName string `json:"targetDatabaseName,omitempty"`

	// RestoreMode defines the restore mode.
	// +kubebuilder:validation:Enum=latest;pitr
	// +kubebuilder:default=latest
	// +optional
	RestoreMode string `json:"restoreMode,omitempty"`
}

// PostgreSQLRestoreStatus defines the observed state of PostgreSQLRestore
type PostgreSQLRestoreStatus struct {
	// Phase shows the restore lifecycle phase.
	// Example: Pending, Running, Completed, Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// StartedAt is the restore start time.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is the restore completion time.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Message contains human-readable status details.
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=postgresqlrestores,scope=Namespaced,shortName=pgrestore
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.restoreMode`
// +kubebuilder:printcolumn:name="Backup ID",type=string,JSONPath=`.spec.backupId`
// +kubebuilder:printcolumn:name="Target Time",type=string,JSONPath=`.spec.targetTime`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PostgreSQLRestore is the Schema for the postgresqlrestores API
type PostgreSQLRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgreSQLRestoreSpec   `json:"spec,omitempty"`
	Status PostgreSQLRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PostgreSQLRestoreList contains a list of PostgreSQLRestore
type PostgreSQLRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgreSQLRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgreSQLRestore{}, &PostgreSQLRestoreList{})
}
