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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

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
}

// PostgreSQLClusterStatus defines the observed state of PostgreSQLCluster.
type PostgreSQLClusterStatus struct {
	Phase string `json:"phase,omitempty"`

	Ready bool `json:"ready,omitempty"`

	PostgresPod string `json:"postgresPod,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

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
