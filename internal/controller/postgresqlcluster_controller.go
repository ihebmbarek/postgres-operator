package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"regexp"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	databasev1 "github.com/ihebmbarek/postgres-operator/api/v1"
)

const (
	barmanStatusRefreshInterval = 5 * time.Minute
	barmanKnownHostsConfigMap   = "barman-known-hosts"
	barmanSSHRuntimeDirectory   = "/etc/barman-ssh"
)

var barmanServerNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type barmanShowBackupResponse map[string]barmanBackupInfo

type barmanBackupInfo struct {
	BackupID string `json:"backup_id"`
	Status   string `json:"status"`

	BaseBackupInformation struct {
		EndTime string `json:"end_time"`
	} `json:"base_backup_information"`
}

type PostgreSQLClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=database.iheb.local,resources=postgresqlclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *PostgreSQLClusterReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster databasev1.PostgreSQLCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	originalStatus := cluster.Status

	image := cluster.Spec.Image
	if image == "" {
		image = "postgres:16"
	}

	database := cluster.Spec.Database
	if database == "" {
		database = "appdb"
	}

	user := cluster.Spec.User
	if user == "" {
		user = "appuser"
	}

	storageSize := cluster.Spec.StorageSize
	if storageSize == "" {
		storageSize = "5Gi"
	}

	storageClassName := cluster.Spec.StorageClassName
	if storageClassName == "" {
		storageClassName = "nfs-client"
	}

	cpuRequest := cluster.Spec.CPURequest
	if cpuRequest == "" {
		cpuRequest = "250m"
	}

	cpuLimit := cluster.Spec.CPULimit
	if cpuLimit == "" {
		cpuLimit = "500m"
	}

	memoryRequest := cluster.Spec.MemoryRequest
	if memoryRequest == "" {
		memoryRequest = "256Mi"
	}

	memoryLimit := cluster.Spec.MemoryLimit
	if memoryLimit == "" {
		memoryLimit = "512Mi"
	}

	labels := map[string]string{
		"app":     cluster.Name,
		"managed": "postgres-operator",
	}

	if err := r.reconcileCredentialsSecret(
		ctx,
		&cluster,
		labels,
		database,
		user,
	); err != nil {
		return ctrl.Result{}, err
	}

	if cluster.Spec.Backup.Enabled {
		if err := r.validateBarmanResources(
			ctx,
			&cluster,
		); err != nil {
			log.Error(
				err,
				"Barman configuration is incomplete",
			)

			cluster.Status.BackupEnabled = true
			cluster.Status.BackupPhase = "Blocked"
			cluster.Status.LastBackupStatus = "ConfigurationError"

			if !reflect.DeepEqual(
				originalStatus,
				cluster.Status,
			) {
				if statusErr := r.Status().Update(
					ctx,
					&cluster,
				); statusErr != nil {
					return ctrl.Result{}, statusErr
				}
			}

			return ctrl.Result{
				RequeueAfter: 30 * time.Second,
			}, nil
		}
	}

	if err := r.reconcilePostgresConfigMap(
		ctx,
		&cluster,
		labels,
	); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcilePostgresService(
		ctx,
		&cluster,
		labels,
	); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileBarmanNodePortService(
		ctx,
		&cluster,
		labels,
	); err != nil {
		return ctrl.Result{}, err
	}

	statefulSet := r.buildStatefulSet(
		&cluster,
		labels,
		image,
		database,
		user,
		storageSize,
		storageClassName,
		cpuRequest,
		cpuLimit,
		memoryRequest,
		memoryLimit,
	)

	if err := ctrl.SetControllerReference(
		&cluster,
		statefulSet,
		r.Scheme,
	); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileStatefulSet(
		ctx,
		statefulSet,
	); err != nil {
		return ctrl.Result{}, err
	}

	podName := cluster.Name + "-0"

	var postgresPod corev1.Pod
	err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      podName,
			Namespace: cluster.Namespace,
		},
		&postgresPod,
	)

	if err != nil {
		cluster.Status.Phase = "Pending"
		cluster.Status.Ready = false
		cluster.Status.PostgresPod = podName
	} else {
		cluster.Status.PostgresPod = podName

		if postgresPod.Status.Phase == corev1.PodRunning {
			cluster.Status.Phase = "Running"
			cluster.Status.Ready = true
		} else {
			cluster.Status.Phase = string(postgresPod.Status.Phase)
			cluster.Status.Ready = false
		}
	}

	if cluster.Spec.Backup.Enabled {
		cluster.Status.BackupEnabled = true

		if cluster.Spec.Backup.ExposeService {
			cluster.Status.BackupPhase = "Exposed"
			cluster.Status.BarmanService = cluster.Name + "-barman"
			cluster.Status.BarmanNodePort = cluster.Spec.Backup.NodePort
		} else {
			cluster.Status.BackupPhase = "Configured"
			cluster.Status.BarmanService = ""
			cluster.Status.BarmanNodePort = 0
		}

		if err := r.updateLatestBackupStatus(
			ctx,
			&cluster,
		); err != nil {
			log.Error(
				err,
				"Failed to retrieve latest Barman backup status",
			)

			if cluster.Status.LastBackupStatus == "" {
				cluster.Status.LastBackupStatus = "NotChecked"
			}
		}
	} else {
		cluster.Status.BackupEnabled = false
		cluster.Status.BackupPhase = "Disabled"
		cluster.Status.BarmanService = ""
		cluster.Status.BarmanNodePort = 0
		cluster.Status.LastBackupStatus = "Disabled"
		cluster.Status.LastBackupTime = nil
		cluster.Status.LastBackupID = ""
	}

	if !reflect.DeepEqual(
		originalStatus,
		cluster.Status,
	) {
		if err := r.Status().Update(
			ctx,
			&cluster,
		); err != nil {
			log.Error(
				err,
				"Failed to update PostgreSQLCluster status",
			)

			return ctrl.Result{}, err
		}
	}

	if cluster.Spec.Backup.Enabled {
		return ctrl.Result{
			RequeueAfter: barmanStatusRefreshInterval,
		}, nil
	}

	return ctrl.Result{}, nil
}

func (r *PostgreSQLClusterReconciler) reconcileCredentialsSecret(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
	database string,
	user string,
) error {
	log := logf.FromContext(ctx)

	desiredSecret := r.buildSecret(
		cluster,
		labels,
		database,
		user,
	)

	if err := ctrl.SetControllerReference(
		cluster,
		desiredSecret,
		r.Scheme,
	); err != nil {
		return err
	}

	var existingSecret corev1.Secret
	err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      desiredSecret.Name,
			Namespace: desiredSecret.Namespace,
		},
		&existingSecret,
	)

	if apierrors.IsNotFound(err) {
		log.Info(
			"Creating PostgreSQL credentials Secret",
			"name",
			desiredSecret.Name,
		)

		return r.Create(
			ctx,
			desiredSecret,
		)
	}

	if err != nil {
		return err
	}

	// Preserve the existing password and credentials.
	// The Operator must not overwrite credentials after creation.
	return nil
}

func (r *PostgreSQLClusterReconciler) buildSecret(
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
	database string,
	user string,
) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-credentials",
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"POSTGRES_DB":       database,
			"POSTGRES_USER":     user,
			"POSTGRES_PASSWORD": "postgres",
		},
	}
}

func (r *PostgreSQLClusterReconciler) validateBarmanResources(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
) error {
	if cluster.Spec.Backup.BarmanHost == "" {
		return fmt.Errorf(
			"spec.backup.barmanHost must not be empty",
		)
	}

	if cluster.Spec.Backup.SSHSecretName == "" {
		return fmt.Errorf(
			"spec.backup.sshSecretName must not be empty",
		)
	}

	var sshSecret corev1.Secret
	if err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      cluster.Spec.Backup.SSHSecretName,
			Namespace: cluster.Namespace,
		},
		&sshSecret,
	); err != nil {
		return fmt.Errorf(
			"failed to retrieve SSH Secret %q: %w",
			cluster.Spec.Backup.SSHSecretName,
			err,
		)
	}

	if _, exists := sshSecret.Data["id_ed25519"]; !exists {
		return fmt.Errorf(
			"SSH Secret %q does not contain id_ed25519",
			cluster.Spec.Backup.SSHSecretName,
		)
	}

	var knownHostsConfigMap corev1.ConfigMap
	if err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      barmanKnownHostsConfigMap,
			Namespace: cluster.Namespace,
		},
		&knownHostsConfigMap,
	); err != nil {
		return fmt.Errorf(
			"failed to retrieve ConfigMap %q: %w",
			barmanKnownHostsConfigMap,
			err,
		)
	}

	if knownHostsConfigMap.Data["known_hosts"] == "" {
		return fmt.Errorf(
			"ConfigMap %q does not contain known_hosts",
			barmanKnownHostsConfigMap,
		)
	}

	return nil
}

func (r *PostgreSQLClusterReconciler) reconcilePostgresConfigMap(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
) error {
	log := logf.FromContext(ctx)

	desiredConfigMap := r.buildConfigMap(
		cluster,
		labels,
	)

	if err := ctrl.SetControllerReference(
		cluster,
		desiredConfigMap,
		r.Scheme,
	); err != nil {
		return err
	}

	var existingConfigMap corev1.ConfigMap
	err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      desiredConfigMap.Name,
			Namespace: desiredConfigMap.Namespace,
		},
		&existingConfigMap,
	)

	if apierrors.IsNotFound(err) {
		log.Info(
			"Creating PostgreSQL ConfigMap",
			"name",
			desiredConfigMap.Name,
		)

		return r.Create(
			ctx,
			desiredConfigMap,
		)
	}

	if err != nil {
		return err
	}

	if reflect.DeepEqual(
		existingConfigMap.Data,
		desiredConfigMap.Data,
	) &&
		reflect.DeepEqual(
			existingConfigMap.Labels,
			desiredConfigMap.Labels,
		) {
		return nil
	}

	existingConfigMap.Data = desiredConfigMap.Data
	existingConfigMap.Labels = desiredConfigMap.Labels

	log.Info(
		"Updating PostgreSQL ConfigMap",
		"name",
		existingConfigMap.Name,
	)

	return r.Update(
		ctx,
		&existingConfigMap,
	)
}

func (r *PostgreSQLClusterReconciler) buildConfigMap(
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-config",
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"custom.conf": buildCustomPostgresConfig(
				cluster,
			),
		},
	}
}

func buildCustomPostgresConfig(
	cluster *databasev1.PostgreSQLCluster,
) string {
	archiveTimeout := cluster.Spec.Backup.ArchiveTimeout
	if archiveTimeout == 0 {
		archiveTimeout = 60
	}

	if !cluster.Spec.Backup.Enabled {
		return `listen_addresses = '*'
wal_level = replica
archive_mode = off
max_wal_senders = 5
archive_timeout = 60
`
	}

	barmanUser := cluster.Spec.Backup.BarmanUser
	if barmanUser == "" {
		barmanUser = "barman"
	}

	barmanServerName := cluster.Spec.Backup.BarmanServerName
	if barmanServerName == "" {
		barmanServerName = cluster.Name
	}

	return fmt.Sprintf(
		`listen_addresses = '*'
wal_level = replica
archive_mode = on
max_wal_senders = 5
archive_timeout = %d
archive_command = 'PATH=/etc/barman-ssh:$PATH barman-wal-archive -U %s %s %s %%p'
`,
		archiveTimeout,
		barmanUser,
		cluster.Spec.Backup.BarmanHost,
		barmanServerName,
	)
}

func (r *PostgreSQLClusterReconciler) reconcilePostgresService(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
) error {
	log := logf.FromContext(ctx)

	desiredService := r.buildService(
		cluster,
		labels,
	)

	if err := ctrl.SetControllerReference(
		cluster,
		desiredService,
		r.Scheme,
	); err != nil {
		return err
	}

	var existingService corev1.Service
	err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      desiredService.Name,
			Namespace: desiredService.Namespace,
		},
		&existingService,
	)

	if apierrors.IsNotFound(err) {
		log.Info(
			"Creating PostgreSQL service",
			"name",
			desiredService.Name,
		)

		return r.Create(
			ctx,
			desiredService,
		)
	}

	if err != nil {
		return err
	}

	needsUpdate := false

	if !reflect.DeepEqual(
		existingService.Labels,
		desiredService.Labels,
	) {
		existingService.Labels = desiredService.Labels
		needsUpdate = true
	}

	if !reflect.DeepEqual(
		existingService.Spec.Selector,
		desiredService.Spec.Selector,
	) {
		existingService.Spec.Selector = desiredService.Spec.Selector
		needsUpdate = true
	}

	if !reflect.DeepEqual(
		existingService.Spec.Ports,
		desiredService.Spec.Ports,
	) {
		existingService.Spec.Ports = desiredService.Spec.Ports
		needsUpdate = true
	}

	if !needsUpdate {
		return nil
	}

	log.Info(
		"Updating PostgreSQL service",
		"name",
		existingService.Name,
	)

	return r.Update(
		ctx,
		&existingService,
	)
}

func (r *PostgreSQLClusterReconciler) buildService(
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{
					Name: "postgres",
					Port: 5432,
				},
			},
		},
	}
}

func (r *PostgreSQLClusterReconciler) buildStatefulSet(
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
	image string,
	database string,
	user string,
	storageSize string,
	storageClassName string,
	cpuRequest string,
	cpuLimit string,
	memoryRequest string,
	memoryLimit string,
) *appsv1.StatefulSet {
	replicas := int32(1)
	credentialsSecretName := cluster.Name + "-credentials"
	configMapName := cluster.Name + "-config"

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "postgres-storage",
			MountPath: "/var/lib/postgresql/data",
		},
		{
			Name:      "postgres-config",
			MountPath: "/etc/postgresql/custom.conf",
			SubPath:   "custom.conf",
			ReadOnly:  true,
		},
	}

	volumes := []corev1.Volume{
		{
			Name: "postgres-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMapName,
					},
				},
			},
		},
	}

	initContainers := []corev1.Container{}

	if cluster.Spec.Backup.Enabled &&
		cluster.Spec.Backup.SSHSecretName != "" {
		volumeMounts = append(
			volumeMounts,
			corev1.VolumeMount{
				Name:      "barman-ssh-runtime",
				MountPath: barmanSSHRuntimeDirectory,
				ReadOnly:  true,
			},
		)

		volumes = append(
			volumes,
			corev1.Volume{
				Name: "barman-ssh-secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: cluster.Spec.Backup.SSHSecretName,
						DefaultMode: int32Ptr(
							0440,
						),
					},
				},
			},
			corev1.Volume{
				Name: "barman-known-hosts",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: barmanKnownHostsConfigMap,
						},
					},
				},
			},
			corev1.Volume{
				Name: "barman-ssh-runtime",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		)

		initContainers = append(
			initContainers,
			corev1.Container{
				Name:  "prepare-barman-ssh",
				Image: image,
				Command: []string{
					"/bin/sh",
					"-ec",
				},
				Args: []string{
					`
cp /var/run/barman-secret/id_ed25519 /etc/barman-ssh/id_ed25519
cp /var/run/barman-known-hosts/known_hosts /etc/barman-ssh/known_hosts
chmod 600 /etc/barman-ssh/id_ed25519
chmod 644 /etc/barman-ssh/known_hosts

cat > /etc/barman-ssh/ssh <<'EOF'
#!/bin/sh
exec /usr/bin/ssh \
  -i /etc/barman-ssh/id_ed25519 \
  -o UserKnownHostsFile=/etc/barman-ssh/known_hosts \
  -o StrictHostKeyChecking=yes \
  -o IdentitiesOnly=yes \
  -o PreferredAuthentications=publickey \
  -o PasswordAuthentication=no \
  -o BatchMode=yes \
  "$@"
EOF

chmod 755 /etc/barman-ssh/ssh
`,
				},
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "barman-ssh-secret",
						MountPath: "/var/run/barman-secret",
						ReadOnly:  true,
					},
					{
						Name:      "barman-known-hosts",
						MountPath: "/var/run/barman-known-hosts",
						ReadOnly:  true,
					},
					{
						Name:      "barman-ssh-runtime",
						MountPath: barmanSSHRuntimeDirectory,
					},
				},
			},
		)
	}

	configHash := fmt.Sprintf(
		"%x",
		sha256.Sum256(
			[]byte(
				buildCustomPostgresConfig(
					cluster,
				),
			),
		),
	)

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: cluster.Name,
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"database.iheb.local/config-hash": configHash,
					},
				},
				Spec: corev1.PodSpec{
					InitContainers: initContainers,
					Containers: []corev1.Container{
						{
							Name:  "postgres",
							Image: image,
							Args: []string{
								"-c",
								"config_file=/etc/postgresql/custom.conf",
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse(
										cpuRequest,
									),
									corev1.ResourceMemory: resource.MustParse(
										memoryRequest,
									),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse(
										cpuLimit,
									),
									corev1.ResourceMemory: resource.MustParse(
										memoryLimit,
									),
								},
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "postgres",
									ContainerPort: 5432,
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"pg_isready",
											"-U",
											user,
											"-d",
											database,
										},
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       10,
								TimeoutSeconds:      5,
								FailureThreshold:    3,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"pg_isready",
											"-U",
											user,
											"-d",
											database,
										},
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       20,
								TimeoutSeconds:      5,
								FailureThreshold:    3,
							},
							Env: []corev1.EnvVar{
								{
									Name: "POSTGRES_DB",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: credentialsSecretName,
											},
											Key: "POSTGRES_DB",
										},
									},
								},
								{
									Name: "POSTGRES_USER",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: credentialsSecretName,
											},
											Key: "POSTGRES_USER",
										},
									},
								},
								{
									Name: "POSTGRES_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: credentialsSecretName,
											},
											Key: "POSTGRES_PASSWORD",
										},
									},
								},
								{
									Name:  "PGDATA",
									Value: "/var/lib/postgresql/data/pgdata",
								},
							},
							VolumeMounts: volumeMounts,
						},
					},
					Volumes: volumes,
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "postgres-storage",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: &storageClassName,
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(
									storageSize,
								),
							},
						},
					},
				},
			},
		},
	}
}

func (r *PostgreSQLClusterReconciler) reconcileStatefulSet(
	ctx context.Context,
	desiredStatefulSet *appsv1.StatefulSet,
) error {
	log := logf.FromContext(ctx)

	var existingStatefulSet appsv1.StatefulSet
	err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      desiredStatefulSet.Name,
			Namespace: desiredStatefulSet.Namespace,
		},
		&existingStatefulSet,
	)

	if apierrors.IsNotFound(err) {
		log.Info(
			"Creating PostgreSQL StatefulSet",
			"name",
			desiredStatefulSet.Name,
		)

		return r.Create(
			ctx,
			desiredStatefulSet,
		)
	}

	if err != nil {
		return err
	}

	needsUpdate := false

	if !reflect.DeepEqual(
		existingStatefulSet.Spec.Replicas,
		desiredStatefulSet.Spec.Replicas,
	) {
		existingStatefulSet.Spec.Replicas =
			desiredStatefulSet.Spec.Replicas

		needsUpdate = true
	}

	if !reflect.DeepEqual(
		existingStatefulSet.Spec.Template,
		desiredStatefulSet.Spec.Template,
	) {
		existingStatefulSet.Spec.Template =
			desiredStatefulSet.Spec.Template

		needsUpdate = true
	}

	if !needsUpdate {
		return nil
	}

	log.Info(
		"Updating PostgreSQL StatefulSet",
		"name",
		existingStatefulSet.Name,
	)

	return r.Update(
		ctx,
		&existingStatefulSet,
	)
}

func (r *PostgreSQLClusterReconciler) reconcileBarmanNodePortService(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
) error {
	log := logf.FromContext(ctx)
	serviceName := cluster.Name + "-barman"

	var existingService corev1.Service
	err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      serviceName,
			Namespace: cluster.Namespace,
		},
		&existingService,
	)

	shouldExpose := cluster.Spec.Backup.Enabled &&
		cluster.Spec.Backup.ExposeService

	if !shouldExpose {
		if apierrors.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return err
		}

		log.Info(
			"Deleting Barman NodePort service",
			"name",
			serviceName,
		)

		return r.Delete(
			ctx,
			&existingService,
		)
	}

	if cluster.Spec.Backup.NodePort < 30000 ||
		cluster.Spec.Backup.NodePort > 32767 {
		return fmt.Errorf(
			"invalid Barman NodePort %d: expected a value between 30000 and 32767",
			cluster.Spec.Backup.NodePort,
		)
	}

	desiredService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "postgres",
					Protocol:   corev1.ProtocolTCP,
					Port:       5432,
					TargetPort: intstr.FromInt32(5432),
					NodePort:   cluster.Spec.Backup.NodePort,
				},
			},
		},
	}

	if apierrors.IsNotFound(err) {
		if err := ctrl.SetControllerReference(
			cluster,
			desiredService,
			r.Scheme,
		); err != nil {
			return err
		}

		log.Info(
			"Creating Barman NodePort service",
			"name",
			desiredService.Name,
			"nodePort",
			cluster.Spec.Backup.NodePort,
		)

		return r.Create(
			ctx,
			desiredService,
		)
	}

	if err != nil {
		return err
	}

	needsUpdate := false

	if !reflect.DeepEqual(
		existingService.Labels,
		desiredService.Labels,
	) {
		existingService.Labels = desiredService.Labels
		needsUpdate = true
	}

	if existingService.Spec.Type !=
		desiredService.Spec.Type {
		existingService.Spec.Type = desiredService.Spec.Type
		needsUpdate = true
	}

	if !reflect.DeepEqual(
		existingService.Spec.Selector,
		desiredService.Spec.Selector,
	) {
		existingService.Spec.Selector = desiredService.Spec.Selector
		needsUpdate = true
	}

	if !reflect.DeepEqual(
		existingService.Spec.Ports,
		desiredService.Spec.Ports,
	) {
		existingService.Spec.Ports = desiredService.Spec.Ports
		needsUpdate = true
	}

	if !needsUpdate {
		return nil
	}

	log.Info(
		"Updating Barman NodePort service",
		"name",
		existingService.Name,
		"nodePort",
		cluster.Spec.Backup.NodePort,
	)

	return r.Update(
		ctx,
		&existingService,
	)
}

func (r *PostgreSQLClusterReconciler) updateLatestBackupStatus(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
) error {
	backupInfo, err := r.fetchLatestBarmanBackup(
		ctx,
		cluster,
	)

	if err != nil {
		return err
	}

	cluster.Status.LastBackupID = backupInfo.BackupID
	cluster.Status.LastBackupStatus = backupInfo.Status

	if backupInfo.BaseBackupInformation.EndTime == "" {
		cluster.Status.LastBackupTime = nil
		return nil
	}

	endTime, err := parseBarmanTime(
		backupInfo.BaseBackupInformation.EndTime,
	)

	if err != nil {
		return fmt.Errorf(
			"failed to parse Barman backup end time %q: %w",
			backupInfo.BaseBackupInformation.EndTime,
			err,
		)
	}

	cluster.Status.LastBackupTime = &metav1.Time{
		Time: endTime,
	}

	return nil
}

func parseBarmanTime(
	value string,
) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07:00",
		time.RFC3339Nano,
		time.RFC3339,
	}

	var lastErr error

	for _, layout := range layouts {
		parsedTime, err := time.Parse(
			layout,
			value,
		)

		if err == nil {
			return parsedTime, nil
		}

		lastErr = err
	}

	return time.Time{}, lastErr
}

func (r *PostgreSQLClusterReconciler) fetchLatestBarmanBackup(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
) (barmanBackupInfo, error) {
	var emptyBackupInfo barmanBackupInfo

	if cluster.Spec.Backup.BarmanHost == "" {
		return emptyBackupInfo, fmt.Errorf(
			"Barman host is empty",
		)
	}

	if cluster.Spec.Backup.SSHSecretName == "" {
		return emptyBackupInfo, fmt.Errorf(
			"Barman SSH Secret name is empty",
		)
	}

	barmanUser := cluster.Spec.Backup.BarmanUser
	if barmanUser == "" {
		barmanUser = "barman"
	}

	barmanServerName := cluster.Spec.Backup.BarmanServerName
	if barmanServerName == "" {
		barmanServerName = cluster.Name
	}

	if !barmanServerNamePattern.MatchString(
		barmanServerName,
	) {
		return emptyBackupInfo, fmt.Errorf(
			"invalid Barman server name %q",
			barmanServerName,
		)
	}

	var sshSecret corev1.Secret
	if err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      cluster.Spec.Backup.SSHSecretName,
			Namespace: cluster.Namespace,
		},
		&sshSecret,
	); err != nil {
		return emptyBackupInfo, fmt.Errorf(
			"failed to read Barman SSH Secret %q: %w",
			cluster.Spec.Backup.SSHSecretName,
			err,
		)
	}

	privateKey, exists := sshSecret.Data["id_ed25519"]
	if !exists {
		return emptyBackupInfo, fmt.Errorf(
			"SSH Secret %q does not contain the key id_ed25519",
			cluster.Spec.Backup.SSHSecretName,
		)
	}

	signer, err := ssh.ParsePrivateKey(
		privateKey,
	)

	if err != nil {
		return emptyBackupInfo, fmt.Errorf(
			"failed to parse the Barman SSH private key: %w",
			err,
		)
	}

	hostKeyCallback, err := r.buildBarmanHostKeyCallback(
		ctx,
		cluster.Namespace,
	)

	if err != nil {
		return emptyBackupInfo, err
	}

	sshConfig := &ssh.ClientConfig{
		User: barmanUser,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(
				signer,
			),
		},
		HostKeyCallback: hostKeyCallback,
		Timeout:         10 * time.Second,
	}

	address := net.JoinHostPort(
		cluster.Spec.Backup.BarmanHost,
		"22",
	)

	sshClient, err := ssh.Dial(
		"tcp",
		address,
		sshConfig,
	)

	if err != nil {
		return emptyBackupInfo, fmt.Errorf(
			"failed to connect to Barman over SSH at %s: %w",
			address,
			err,
		)
	}

	defer sshClient.Close()

	session, err := sshClient.NewSession()
	if err != nil {
		return emptyBackupInfo, fmt.Errorf(
			"failed to create Barman SSH session: %w",
			err,
		)
	}

	defer session.Close()

	command := fmt.Sprintf(
		"barman -f json show-backup %s latest",
		barmanServerName,
	)

	output, err := session.Output(
		command,
	)

	if err != nil {
		return emptyBackupInfo, fmt.Errorf(
			"failed to execute remote Barman command: %w",
			err,
		)
	}

	var response barmanShowBackupResponse
	if err := json.Unmarshal(
		output,
		&response,
	); err != nil {
		return emptyBackupInfo, fmt.Errorf(
			"failed to parse Barman JSON response: %w",
			err,
		)
	}

	backupInfo, exists := response[barmanServerName]
	if !exists {
		return emptyBackupInfo, fmt.Errorf(
			"Barman JSON response does not contain server %q",
			barmanServerName,
		)
	}

	if backupInfo.BackupID == "" {
		return emptyBackupInfo, fmt.Errorf(
			"Barman returned an empty backup ID for server %q",
			barmanServerName,
		)
	}

	return backupInfo, nil
}

func (r *PostgreSQLClusterReconciler) buildBarmanHostKeyCallback(
	ctx context.Context,
	namespace string,
) (ssh.HostKeyCallback, error) {
	var knownHostsConfigMap corev1.ConfigMap

	if err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      barmanKnownHostsConfigMap,
			Namespace: namespace,
		},
		&knownHostsConfigMap,
	); err != nil {
		return nil, fmt.Errorf(
			"failed to read ConfigMap %q: %w",
			barmanKnownHostsConfigMap,
			err,
		)
	}

	knownHostsContent := knownHostsConfigMap.Data["known_hosts"]
	if knownHostsContent == "" {
		return nil, fmt.Errorf(
			"ConfigMap %q does not contain known_hosts",
			barmanKnownHostsConfigMap,
		)
	}

	temporaryFile, err := os.CreateTemp(
		"",
		"barman-known-hosts-*",
	)

	if err != nil {
		return nil, fmt.Errorf(
			"failed to create a temporary known_hosts file: %w",
			err,
		)
	}

	temporaryPath := temporaryFile.Name()

	defer func() {
		temporaryFile.Close()
		os.Remove(
			temporaryPath,
		)
	}()

	if _, err := temporaryFile.WriteString(
		knownHostsContent,
	); err != nil {
		return nil, fmt.Errorf(
			"failed to write the temporary known_hosts file: %w",
			err,
		)
	}

	if err := temporaryFile.Close(); err != nil {
		return nil, fmt.Errorf(
			"failed to close the temporary known_hosts file: %w",
			err,
		)
	}

	hostKeyCallback, err := knownhosts.New(
		temporaryPath,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"failed to parse known_hosts: %w",
			err,
		)
	}

	return hostKeyCallback, nil
}

func int32Ptr(
	i int32,
) *int32 {
	return &i
}

func (r *PostgreSQLClusterReconciler) SetupWithManager(
	mgr ctrl.Manager,
) error {
	return ctrl.NewControllerManagedBy(
		mgr,
	).
		For(
			&databasev1.PostgreSQLCluster{},
		).
		Owns(
			&appsv1.StatefulSet{},
		).
		Owns(
			&corev1.Service{},
		).
		Owns(
			&corev1.Secret{},
		).
		Owns(
			&corev1.ConfigMap{},
		).
		Named(
			"postgresqlcluster",
		).
		Complete(
			r,
		)
}
