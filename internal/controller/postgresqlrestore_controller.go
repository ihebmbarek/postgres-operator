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

// PostgreSQLRestoreReconciler reconciles a PostgreSQLRestore object
type PostgreSQLRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlclusters,verbs=get;list;watch

func (r *PostgreSQLRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var restore databasev1.PostgreSQLRestore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if restore.Status.Phase == "" {
		now := metav1.Now()
		restore.Status.Phase = "Requested"
		restore.Status.StartedAt = &now
		restore.Status.Message = "PostgreSQLRestore API accepted. Restore/PITR execution will be handled by the operator restore workflow."

		if err := r.Status().Update(ctx, &restore); err != nil {
			return ctrl.Result{}, err
		}

		logger.Info("PostgreSQLRestore status initialized",
			"name", restore.Name,
			"namespace", restore.Namespace,
			"cluster", restore.Spec.ClusterName,
			"mode", restore.Spec.RestoreMode,
		)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgreSQLRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&databasev1.PostgreSQLRestore{}).
		Named("postgresqlrestore").
		Complete(r)
}
