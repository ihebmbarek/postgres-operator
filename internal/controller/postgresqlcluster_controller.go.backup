package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	databasev1 "github.com/ihebmbarek/postgres-operator/api/v1"
)

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

func (r *PostgreSQLClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster databasev1.PostgreSQLCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

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

	secret := r.buildSecret(&cluster, labels, database, user)
	if err := ctrl.SetControllerReference(&cluster, secret, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingSecret corev1.Secret
	err := r.Get(ctx, client.ObjectKey{Name: secret.Name, Namespace: secret.Namespace}, &existingSecret)
	if err != nil && apierrors.IsNotFound(err) {
		log.Info("Creating PostgreSQL credentials Secret", "name", secret.Name)
		if err := r.Create(ctx, secret); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}

	configMap := r.buildConfigMap(&cluster, labels)
	if err := ctrl.SetControllerReference(&cluster, configMap, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingConfigMap corev1.ConfigMap
	err = r.Get(ctx, client.ObjectKey{Name: configMap.Name, Namespace: configMap.Namespace}, &existingConfigMap)
	if err != nil && apierrors.IsNotFound(err) {
		log.Info("Creating PostgreSQL ConfigMap", "name", configMap.Name)
		if err := r.Create(ctx, configMap); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}

	service := r.buildService(&cluster, labels)
	if err := ctrl.SetControllerReference(&cluster, service, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingService corev1.Service
	err = r.Get(ctx, client.ObjectKey{Name: service.Name, Namespace: service.Namespace}, &existingService)
	if err != nil && apierrors.IsNotFound(err) {
		log.Info("Creating PostgreSQL service", "name", service.Name)
		if err := r.Create(ctx, service); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
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
	if err := ctrl.SetControllerReference(&cluster, statefulSet, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingStatefulSet appsv1.StatefulSet
	err = r.Get(ctx, client.ObjectKey{Name: statefulSet.Name, Namespace: statefulSet.Namespace}, &existingStatefulSet)
	if err != nil && apierrors.IsNotFound(err) {
		log.Info("Creating PostgreSQL StatefulSet", "name", statefulSet.Name)
		if err := r.Create(ctx, statefulSet); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}

	podName := cluster.Name + "-0"
	var postgresPod corev1.Pod
	err = r.Get(ctx, client.ObjectKey{Name: podName, Namespace: cluster.Namespace}, &postgresPod)
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
		cluster.Status.BackupPhase = "Configured"
	} else {
		cluster.Status.BackupEnabled = false
		cluster.Status.BackupPhase = "Disabled"
	}

	if err := r.Status().Update(ctx, &cluster); err != nil {
		log.Error(err, "Failed to update PostgreSQLCluster status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
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

func (r *PostgreSQLClusterReconciler) buildConfigMap(
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
) *corev1.ConfigMap {
	archiveTimeout := cluster.Spec.Backup.ArchiveTimeout
	if archiveTimeout == 0 {
		archiveTimeout = 60
	}

	customConfig := `listen_addresses = '*'
wal_level = replica
archive_mode = off
max_wal_senders = 5
archive_timeout = 60
`

	if cluster.Spec.Backup.Enabled {
		barmanUser := cluster.Spec.Backup.BarmanUser
		if barmanUser == "" {
			barmanUser = "barman"
		}

		barmanServerName := cluster.Spec.Backup.BarmanServerName
		if barmanServerName == "" {
			barmanServerName = cluster.Name
		}

		customConfig = fmt.Sprintf(`listen_addresses = '*'
wal_level = replica
archive_mode = on
max_wal_senders = 5
archive_timeout = %d
archive_command = 'cp /tmp/.ssh/id_ed25519 /tmp/id_ed25519 && chmod 600 /tmp/id_ed25519 && BARMAN_SSH_COMMAND="ssh -i /tmp/id_ed25519 -l %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -o PreferredAuthentications=publickey -o PasswordAuthentication=no -o BatchMode=yes" barman-wal-archive %s %s %%p'
`,
			archiveTimeout,
			barmanUser,
			cluster.Spec.Backup.BarmanHost,
			barmanServerName,
		)
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-config",
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"custom.conf": customConfig,
		},
	}
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

	if cluster.Spec.Backup.Enabled && cluster.Spec.Backup.SSHSecretName != "" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "barman-ssh-key",
			MountPath: "/tmp/.ssh/id_ed25519",
			SubPath:   "id_ed25519",
			ReadOnly:  true,
		})

		volumes = append(volumes, corev1.Volume{
			Name: "barman-ssh-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  cluster.Spec.Backup.SSHSecretName,
					DefaultMode: int32Ptr(0440),
				},
			},
		})
	}

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
				},
				Spec: corev1.PodSpec{
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
									corev1.ResourceCPU:    resource.MustParse(cpuRequest),
									corev1.ResourceMemory: resource.MustParse(memoryRequest),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(cpuLimit),
									corev1.ResourceMemory: resource.MustParse(memoryLimit),
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
								corev1.ResourceStorage: resource.MustParse(storageSize),
							},
						},
					},
				},
			},
		},
	}
}

func int32Ptr(i int32) *int32 {
	return &i
}

func (r *PostgreSQLClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&databasev1.PostgreSQLCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ConfigMap{}).
		Named("postgresqlcluster").
		Complete(r)
}
