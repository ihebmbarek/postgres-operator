package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/ihebmbarek/postgres-operator/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var prometheusRuleGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "PrometheusRule",
}

func (r *PostgreSQLClusterReconciler) reconcileSLOPrometheusRule(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
) error {
	desired := buildSLOPrometheusRule(cluster)

	if err := ctrl.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return fmt.Errorf("set owner reference on SLO PrometheusRule: %w", err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(prometheusRuleGVK)

	err := r.Get(ctx, client.ObjectKey{
		Name:      desired.GetName(),
		Namespace: desired.GetNamespace(),
	}, existing)

	if meta.IsNoMatchError(err) {
		// Prometheus Operator CRDs are optional in local tests or clusters without monitoring.
		// On OpenShift with monitoring enabled, the PrometheusRule CRD exists and the SLO rule is created.
		return nil
	}

	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil {
			return fmt.Errorf("create SLO PrometheusRule %s/%s: %w", desired.GetNamespace(), desired.GetName(), err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("get SLO PrometheusRule %s/%s: %w", desired.GetNamespace(), desired.GetName(), err)
	}

	existing.SetLabels(desired.GetLabels())
	existing.SetAnnotations(desired.GetAnnotations())
	existing.SetOwnerReferences(desired.GetOwnerReferences())
	existing.Object["spec"] = desired.Object["spec"]

	if err := r.Update(ctx, existing); err != nil {
		return fmt.Errorf("update SLO PrometheusRule %s/%s: %w", desired.GetNamespace(), desired.GetName(), err)
	}

	return nil
}

func buildSLOPrometheusRule(cluster *databasev1.PostgreSQLCluster) *unstructured.Unstructured {
	ruleName := fmt.Sprintf("%s-slo-alerts", cluster.Name)
	exporterService := fmt.Sprintf("%s-exporter", cluster.Name)
	backupCronJob := fmt.Sprintf("%s-barman-backup", cluster.Name)
	pgbouncerDeployment := fmt.Sprintf("%s-pgbouncer", cluster.Name)

	rule := &unstructured.Unstructured{}
	rule.SetGroupVersionKind(prometheusRuleGVK)
	rule.SetName(ruleName)
	rule.SetNamespace(cluster.Namespace)
	rule.SetLabels(map[string]string{
		"app":                          cluster.Name,
		"app.kubernetes.io/name":       cluster.Name,
		"app.kubernetes.io/component":  "slo-alerts",
		"app.kubernetes.io/managed-by": "postgres-operator",
		"pg-guardian.io/sre":           "slo-sli",
	})
	rule.SetAnnotations(map[string]string{
		"pg-guardian.io/slo-description": "Operator-generated SLO/SLI Prometheus alerts for PostgreSQL workload reliability.",
	})

	rules := []interface{}{
		map[string]interface{}{
			"alert": fmt.Sprintf("PGGuardianPostgresAvailabilitySLOBreach_%s", sanitizeAlertSuffix(cluster.Name)),
			"expr": fmt.Sprintf(
				`avg_over_time(pg_up{namespace=%q,service=%q}[5m]) < 0.999`,
				cluster.Namespace,
				exporterService,
			),
			"for": "2m",
			"labels": map[string]interface{}{
				"severity":   "critical",
				"namespace":  cluster.Namespace,
				"component":  "database",
				"sli":        "postgres_availability",
				"slo":        "99.9_percent_availability",
				"slo_target": "99.9",
			},
			"annotations": map[string]interface{}{
				"summary": "PostgreSQL availability SLO breach",
				"description": fmt.Sprintf(
					"PostgreSQL availability for cluster %s is below the 99.9%% SLO over the last 5 minutes.",
					cluster.Name,
				),
			},
		},
		map[string]interface{}{
			"alert": fmt.Sprintf("PGGuardianBackupFreshnessSLOBreach_%s", sanitizeAlertSuffix(cluster.Name)),
			"expr": fmt.Sprintf(
				`(time() - kube_cronjob_status_last_successful_time{namespace=%q,cronjob=%q}) > 86400`,
				cluster.Namespace,
				backupCronJob,
			),
			"for": "5m",
			"labels": map[string]interface{}{
				"severity":   "warning",
				"namespace":  cluster.Namespace,
				"component":  "backup",
				"sli":        "backup_freshness",
				"slo":        "successful_backup_within_24h",
				"slo_target": "24h",
			},
			"annotations": map[string]interface{}{
				"summary": "Backup freshness SLO breach",
				"description": fmt.Sprintf(
					"No successful Barman backup for PostgreSQL cluster %s has been observed within the last 24 hours.",
					cluster.Name,
				),
			},
		},
		map[string]interface{}{
			"alert": fmt.Sprintf("PGGuardianBackupJobFailed_%s", sanitizeAlertSuffix(cluster.Name)),
			"expr": fmt.Sprintf(
				`kube_job_status_failed{namespace=%q,job_name=~%q} > 0`,
				cluster.Namespace,
				backupCronJob+"-.*",
			),
			"for": "1m",
			"labels": map[string]interface{}{
				"severity":  "warning",
				"namespace": cluster.Namespace,
				"component": "backup",
				"sli":       "backup_success",
				"slo":       "backup_jobs_must_succeed",
			},
			"annotations": map[string]interface{}{
				"summary": "Backup job failed",
				"description": fmt.Sprintf(
					"At least one Barman backup job for PostgreSQL cluster %s failed.",
					cluster.Name,
				),
			},
		},
		map[string]interface{}{
			"alert": fmt.Sprintf("PGGuardianPgBouncerAvailabilitySLOBreach_%s", sanitizeAlertSuffix(cluster.Name)),
			"expr": fmt.Sprintf(
				`kube_deployment_status_replicas_available{namespace=%q,deployment=%q} < 1`,
				cluster.Namespace,
				pgbouncerDeployment,
			),
			"for": "2m",
			"labels": map[string]interface{}{
				"severity":   "warning",
				"namespace":  cluster.Namespace,
				"component":  "pgbouncer",
				"sli":        "pgbouncer_availability",
				"slo":        "pooler_must_be_available",
				"slo_target": "at_least_1_available_replica",
			},
			"annotations": map[string]interface{}{
				"summary": "PgBouncer availability SLO breach",
				"description": fmt.Sprintf(
					"PgBouncer for PostgreSQL cluster %s has no available replicas.",
					cluster.Name,
				),
			},
		},
	}

	rule.Object["spec"] = map[string]interface{}{
		"groups": []interface{}{
			map[string]interface{}{
				"name":  fmt.Sprintf("pg-guardian.%s.slo.rules", cluster.Name),
				"rules": rules,
			},
		},
	}

	return rule
}

func sanitizeAlertSuffix(value string) string {
	result := make([]rune, 0, len(value))
	for _, r := range value {
		if r >= 'a' && r <= 'z' {
			result = append(result, r-32)
			continue
		}
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			result = append(result, r)
			continue
		}
		result = append(result, '_')
	}
	return string(result)
}
