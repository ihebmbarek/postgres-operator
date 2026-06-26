package controller

import (
	"context"

	databasev1 "github.com/ihebmbarek/postgres-operator/api/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PostgreSQLBackupReconciler reconciles a PostgreSQLBackup object
type PostgreSQLBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlbackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlclusters,verbs=get;list;watch

func (r *PostgreSQLBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var backup databasev1.PostgreSQLBackup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if backup.Status.Phase == "" {
		now := metav1.Now()
		backup.Status.Phase = "Requested"
		backup.Status.StartedAt = &now
		backup.Status.Message = "PostgreSQLBackup API accepted. Barman backup execution will be handled by the operator backup workflow."

		if err := r.Status().Update(ctx, &backup); err != nil {
			return ctrl.Result{}, err
		}

		logger.Info("PostgreSQLBackup status initialized",
			"name", backup.Name,
			"namespace", backup.Namespace,
			"cluster", backup.Spec.ClusterName,
		)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgreSQLBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&databasev1.PostgreSQLBackup{}).
		Named("postgresqlbackup").
		Complete(r)
}
