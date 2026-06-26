package controller

import (
	"fmt"

	databasev1 "github.com/ihebmbarek/postgres-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	complianceStatusPass = "Pass"
	complianceStatusFail = "Fail"
)

func evaluateClusterCompliance(cluster *databasev1.PostgreSQLCluster) {
	findings := make([]databasev1.ComplianceFinding, 0, 8)

	addFinding := func(id, title, severity string, pass bool, message string) {
		status := complianceStatusFail
		if pass {
			status = complianceStatusPass
		}

		findings = append(findings, databasev1.ComplianceFinding{
			ID:       id,
			Title:    title,
			Severity: severity,
			Status:   status,
			Message:  message,
		})
	}

	tlsPass := cluster.Spec.TLS.Enabled &&
		cluster.Spec.TLS.SecretName != "" &&
		cluster.Spec.TLS.SSLMode != "disable"

	addFinding(
		"CIS-PG-001",
		"PostgreSQL TLS must be enabled",
		"high",
		tlsPass,
		"PostgreSQL must enforce encrypted client connections using a Kubernetes TLS Secret.",
	)

	podSecurityContext := restrictedPodSecurityContext()
	containerSecurityContext := restrictedContainerSecurityContext()

	addFinding(
		"CIS-K8S-001",
		"Pod must run as non-root",
		"high",
		hasRunAsNonRoot(podSecurityContext),
		"Generated PostgreSQL workloads must set runAsNonRoot=true.",
	)

	addFinding(
		"CIS-K8S-002",
		"Privilege escalation must be disabled",
		"critical",
		hasPrivilegeEscalationDisabled(containerSecurityContext),
		"Generated containers must set allowPrivilegeEscalation=false.",
	)

	addFinding(
		"CIS-K8S-003",
		"Containers must not run privileged",
		"critical",
		hasPrivilegedDisabled(containerSecurityContext),
		"Generated containers must set privileged=false.",
	)

	addFinding(
		"CIS-K8S-004",
		"Linux capabilities must be dropped",
		"high",
		dropsAllCapabilities(containerSecurityContext),
		"Generated containers must drop all Linux capabilities.",
	)

	addFinding(
		"CIS-K8S-005",
		"Seccomp RuntimeDefault must be used",
		"medium",
		hasRuntimeDefaultSeccomp(podSecurityContext),
		"Generated pods must use seccompProfile.type=RuntimeDefault.",
	)

	addFinding(
		"CIS-K8S-006",
		"PostgreSQL container resources must be configured",
		"medium",
		hasPostgresResources(cluster),
		"PostgreSQL container must define CPU and memory requests and limits.",
	)

	addFinding(
		"CIS-PG-002",
		"Database credentials must be stored in Kubernetes Secret",
		"high",
		true,
		fmt.Sprintf("The operator manages database credentials in Secret %q.", cluster.Name+"-credentials"),
	)

	passed := 0
	for _, finding := range findings {
		if finding.Status == complianceStatusPass {
			passed++
		}
	}

	cluster.Status.ComplianceFindings = findings
	cluster.Status.ComplianceScore = fmt.Sprintf("%d/%d", passed, len(findings))

	if passed == len(findings) {
		cluster.Status.CompliancePhase = "Passed"
	} else {
		cluster.Status.CompliancePhase = "Warning"
	}
}

func hasRunAsNonRoot(securityContext *corev1.PodSecurityContext) bool {
	return securityContext != nil &&
		securityContext.RunAsNonRoot != nil &&
		*securityContext.RunAsNonRoot
}

func hasRuntimeDefaultSeccomp(securityContext *corev1.PodSecurityContext) bool {
	return securityContext != nil &&
		securityContext.SeccompProfile != nil &&
		securityContext.SeccompProfile.Type == corev1.SeccompProfileTypeRuntimeDefault
}

func hasPrivilegeEscalationDisabled(securityContext *corev1.SecurityContext) bool {
	return securityContext != nil &&
		securityContext.AllowPrivilegeEscalation != nil &&
		!*securityContext.AllowPrivilegeEscalation
}

func hasPrivilegedDisabled(securityContext *corev1.SecurityContext) bool {
	return securityContext != nil &&
		securityContext.Privileged != nil &&
		!*securityContext.Privileged
}

func dropsAllCapabilities(securityContext *corev1.SecurityContext) bool {
	if securityContext == nil || securityContext.Capabilities == nil {
		return false
	}

	for _, capability := range securityContext.Capabilities.Drop {
		if capability == corev1.Capability("ALL") {
			return true
		}
	}

	return false
}

func hasPostgresResources(cluster *databasev1.PostgreSQLCluster) bool {
	return cluster.Spec.CPURequest != "" &&
		cluster.Spec.CPULimit != "" &&
		cluster.Spec.MemoryRequest != "" &&
		cluster.Spec.MemoryLimit != ""
}
