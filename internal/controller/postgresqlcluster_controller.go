package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
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

var (
	barmanServerNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	postgresRoleNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

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
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

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

	if cluster.Spec.Backup.Enabled {
		if err := r.reconcileStreamingCredentialsSecret(
			ctx,
			&cluster,
			labels,
		); err != nil {
			return ctrl.Result{}, err
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

	if err := r.reconcileBackupCronJob(
		ctx,
		&cluster,
		labels,
		image,
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

	if cluster.Spec.Restore.Enabled {
		return r.reconcileRestoreWorkflow(
			ctx,
			&cluster,
			statefulSet,
			labels,
			image,
		)
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
			cluster.Status.BarmanService = cluster.Name + "-barman"
			cluster.Status.BarmanNodePort = cluster.Spec.Backup.NodePort
		} else {
			cluster.Status.BarmanService = ""
			cluster.Status.BarmanNodePort = 0
		}

		if cluster.Spec.Backup.Schedule != "" {
			cluster.Status.BackupPhase = "Scheduled"
			cluster.Status.BackupCronJob = cluster.Name + "-barman-backup"
			cluster.Status.BackupSchedule = cluster.Spec.Backup.Schedule
		} else if cluster.Spec.Backup.ExposeService {
			cluster.Status.BackupPhase = "Exposed"
			cluster.Status.BackupCronJob = ""
			cluster.Status.BackupSchedule = ""
		} else {
			cluster.Status.BackupPhase = "Configured"
			cluster.Status.BackupCronJob = ""
			cluster.Status.BackupSchedule = ""
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
		cluster.Status.BackupCronJob = ""
		cluster.Status.BackupSchedule = ""
	}

	if !cluster.Spec.Restore.Enabled {
		cluster.Status.RestoreEnabled = false
		cluster.Status.RestorePhase = "Disabled"
		cluster.Status.RestoreMessage = "restore is disabled"
		cluster.Status.RestoreJob = ""
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

func (r *PostgreSQLClusterReconciler) reconcileStreamingCredentialsSecret(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
) error {
	log := logf.FromContext(ctx)

	secretName := cluster.Name + "-streaming-credentials"
	streamingUser := effectiveStreamingUser(cluster)

	var existingSecret corev1.Secret

	err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      secretName,
			Namespace: cluster.Namespace,
		},
		&existingSecret,
	)

	if apierrors.IsNotFound(err) {
		password, passwordErr := generateRandomPassword()
		if passwordErr != nil {
			return fmt.Errorf(
				"failed to generate streaming replication password: %w",
				passwordErr,
			)
		}

		desiredSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: cluster.Namespace,
				Labels:    labels,
			},
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"STREAMING_USER":     streamingUser,
				"STREAMING_PASSWORD": password,
			},
		}

		if err := ctrl.SetControllerReference(
			cluster,
			desiredSecret,
			r.Scheme,
		); err != nil {
			return err
		}

		log.Info(
			"Creating streaming replication credentials Secret",
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

	if existingSecret.Data == nil {
		existingSecret.Data = map[string][]byte{}
	}

	needsUpdate := false

	if !reflect.DeepEqual(
		existingSecret.Labels,
		labels,
	) {
		existingSecret.Labels = labels
		needsUpdate = true
	}

	if string(existingSecret.Data["STREAMING_USER"]) !=
		streamingUser {
		existingSecret.Data["STREAMING_USER"] =
			[]byte(streamingUser)

		needsUpdate = true
	}

	if len(existingSecret.Data["STREAMING_PASSWORD"]) == 0 {
		password, passwordErr := generateRandomPassword()
		if passwordErr != nil {
			return fmt.Errorf(
				"failed to generate streaming replication password: %w",
				passwordErr,
			)
		}

		existingSecret.Data["STREAMING_PASSWORD"] =
			[]byte(password)

		needsUpdate = true
	}

	if !needsUpdate {
		return nil
	}

	log.Info(
		"Updating streaming replication credentials Secret",
		"name",
		existingSecret.Name,
	)

	return r.Update(
		ctx,
		&existingSecret,
	)
}

func generateRandomPassword() (string, error) {
	randomBytes := make([]byte, 32)

	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(
		randomBytes,
	), nil
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

	if cluster.Spec.Backup.ReplicationAllowedCIDR == "" {
		return fmt.Errorf(
			"spec.backup.replicationAllowedCIDR must not be empty when backup is enabled",
		)
	}

	if _, _, err := net.ParseCIDR(
		cluster.Spec.Backup.ReplicationAllowedCIDR,
	); err != nil {
		return fmt.Errorf(
			"invalid spec.backup.replicationAllowedCIDR %q: %w",
			cluster.Spec.Backup.ReplicationAllowedCIDR,
			err,
		)
	}

	streamingUser := effectiveStreamingUser(cluster)
	if !postgresRoleNamePattern.MatchString(streamingUser) {
		return fmt.Errorf(
			"invalid spec.backup.streamingUser %q",
			streamingUser,
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
			"pg_hba.conf": buildManagedPostgresHBA(
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
hba_file = '/etc/postgresql/pg_hba.conf'
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
hba_file = '/etc/postgresql/pg_hba.conf'
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

func effectiveStreamingUser(
	cluster *databasev1.PostgreSQLCluster,
) string {
	streamingUser := cluster.Spec.Backup.StreamingUser
	if streamingUser == "" {
		streamingUser = "streaming_barman"
	}

	return streamingUser
}

func buildManagedPostgresHBA(
	cluster *databasev1.PostgreSQLCluster,
) string {
	baseRules := `# Managed by postgres-operator. Do not edit manually.
local   all             all                                     trust
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ::1/128                 scram-sha-256
host    all             all             0.0.0.0/0               scram-sha-256
host    all             all             ::/0                    scram-sha-256
`

	if !cluster.Spec.Backup.Enabled ||
		cluster.Spec.Backup.ReplicationAllowedCIDR == "" {
		return baseRules
	}

	return fmt.Sprintf(
		"%s\nhost    replication     %s    %s    scram-sha-256\n",
		baseRules,
		effectiveStreamingUser(cluster),
		cluster.Spec.Backup.ReplicationAllowedCIDR,
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
		{
			Name:      "postgres-config",
			MountPath: "/etc/postgresql/pg_hba.conf",
			SubPath:   "pg_hba.conf",
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
				buildCustomPostgresConfig(cluster)+
					"\n---pg_hba.conf---\n"+
					buildManagedPostgresHBA(cluster),
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
							Name:      "postgres",
							Image:     image,
							Lifecycle: buildStreamingRoleLifecycle(cluster),
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
									Name: "STREAMING_USER",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: cluster.Name + "-streaming-credentials",
											},
											Key:      "STREAMING_USER",
											Optional: boolPtr(true),
										},
									},
								},
								{
									Name: "STREAMING_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: cluster.Name + "-streaming-credentials",
											},
											Key:      "STREAMING_PASSWORD",
											Optional: boolPtr(true),
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

func buildStreamingRoleLifecycle(
	cluster *databasev1.PostgreSQLCluster,
) *corev1.Lifecycle {
	if !cluster.Spec.Backup.Enabled {
		return nil
	}

	return &corev1.Lifecycle{
		PostStart: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"/bin/sh",
					"-ec",
					`
attempt=1

while [ "${attempt}" -le 60 ]; do
  if pg_isready \
    -h 127.0.0.1 \
    -p 5432 \
    -U "${POSTGRES_USER}" \
    -d postgres \
    >/dev/null 2>&1
  then
    if psql \
      -v ON_ERROR_STOP=1 \
      -U "${POSTGRES_USER}" \
      -d postgres \
      --set=streaming_user="${STREAMING_USER}" \
      --set=streaming_password="${STREAMING_PASSWORD}" \
      <<'SQL'
SELECT format(
  'CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
  :'streaming_user',
  :'streaming_password'
)
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_roles
  WHERE rolname = :'streaming_user'
)
\gexec

SELECT format(
  'ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
  :'streaming_user',
  :'streaming_password'
)
\gexec
SQL
    then
      echo "Streaming replication role configured successfully"

      RECOVERY_STATE="$(
        psql \
          -U "${POSTGRES_USER}" \
          -d postgres \
          -Atqc "SELECT pg_is_in_recovery();"
      )"

      if [ "${RECOVERY_STATE}" = "f" ]; then
        psql \
          -v ON_ERROR_STOP=1 \
          -U "${POSTGRES_USER}" \
          -d postgres \
          -c "ALTER SYSTEM RESET restore_command;" \
          || true

        psql \
          -v ON_ERROR_STOP=1 \
          -U "${POSTGRES_USER}" \
          -d postgres \
          -c "ALTER SYSTEM RESET recovery_target_time;" \
          || true

        psql \
          -v ON_ERROR_STOP=1 \
          -U "${POSTGRES_USER}" \
          -d postgres \
          -c "ALTER SYSTEM RESET recovery_target_action;" \
          || true

        psql \
          -v ON_ERROR_STOP=1 \
          -U "${POSTGRES_USER}" \
          -d postgres \
          -c "SELECT pg_reload_conf();" \
          || true

        rm -rf \
          /var/lib/postgresql/data/barman-wal-* \
          || true

        echo "Post-PITR PostgreSQL cleanup completed successfully"
      fi

      exit 0
    fi
  fi

  attempt=$((attempt + 1))
  sleep 2
done

echo "Failed to configure the streaming replication role" >&2
exit 1
`,
				},
			},
		},
	}
}

func (r *PostgreSQLClusterReconciler) reconcileRestoreWorkflow(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	desiredStatefulSet *appsv1.StatefulSet,
	labels map[string]string,
	defaultImage string,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if err := validateRestoreRequest(cluster); err != nil {
		if statusErr := r.updateRestoreStatus(
			ctx,
			cluster,
			"Blocked",
			err.Error(),
			"",
		); statusErr != nil {
			return ctrl.Result{}, statusErr
		}

		return ctrl.Result{
			RequeueAfter: 30 * time.Second,
		}, nil
	}

	jobName := restoreJobName(cluster)

	if cluster.Status.ObservedRestoreRequestID ==
		cluster.Spec.Restore.RequestID &&
		cluster.Status.RestorePhase == "Completed" {
		return ctrl.Result{}, nil
	}

	pvcName := postgresPVCName(cluster)

	var postgresPVC corev1.PersistentVolumeClaim
	if err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      pvcName,
			Namespace: cluster.Namespace,
		},
		&postgresPVC,
	); err != nil {
		message := fmt.Sprintf(
			"failed to retrieve PostgreSQL PVC %q: %v",
			pvcName,
			err,
		)

		if statusErr := r.updateRestoreStatus(
			ctx,
			cluster,
			"Blocked",
			message,
			jobName,
		); statusErr != nil {
			return ctrl.Result{}, statusErr
		}

		return ctrl.Result{
			RequeueAfter: 30 * time.Second,
		}, nil
	}

	var restoreJob batchv1.Job
	jobErr := r.Get(
		ctx,
		client.ObjectKey{
			Name:      jobName,
			Namespace: cluster.Namespace,
		},
		&restoreJob,
	)

	if jobErr == nil {
		if restoreJob.Status.Failed > 0 {
			message := fmt.Sprintf(
				"restore Job %q failed; inspect its logs",
				jobName,
			)

			if err := r.updateRestoreStatus(
				ctx,
				cluster,
				"Failed",
				message,
				jobName,
			); err != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, nil
		}

		if restoreJob.Status.Succeeded > 0 {
			if err := r.ensureStatefulSetReplicas(
				ctx,
				desiredStatefulSet,
				1,
			); err != nil {
				return ctrl.Result{}, err
			}

			var postgresPod corev1.Pod
			podErr := r.Get(
				ctx,
				client.ObjectKey{
					Name:      cluster.Name + "-0",
					Namespace: cluster.Namespace,
				},
				&postgresPod,
			)

			if apierrors.IsNotFound(podErr) ||
				(podErr == nil &&
					!isPodReady(&postgresPod)) {
				if err := r.updateRestoreStatus(
					ctx,
					cluster,
					"StartingPostgreSQL",
					"restore Job completed; waiting for PostgreSQL to become Ready",
					jobName,
				); err != nil {
					return ctrl.Result{}, err
				}

				return ctrl.Result{
					RequeueAfter: 5 * time.Second,
				}, nil
			}

			if podErr != nil {
				return ctrl.Result{}, podErr
			}

			return r.reconcileRestoreStabilizationJob(
				ctx,
				cluster,
				labels,
				defaultImage,
			)
		}

		if err := r.ensureStatefulSetReplicas(
			ctx,
			desiredStatefulSet,
			0,
		); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.updateRestoreStatus(
			ctx,
			cluster,
			"Restoring",
			"restore Job is running",
			jobName,
		); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, nil
	}

	if !apierrors.IsNotFound(jobErr) {
		return ctrl.Result{}, jobErr
	}

	if err := r.ensureStatefulSetReplicas(
		ctx,
		desiredStatefulSet,
		0,
	); err != nil {
		return ctrl.Result{}, err
	}

	var postgresPod corev1.Pod
	podErr := r.Get(
		ctx,
		client.ObjectKey{
			Name:      cluster.Name + "-0",
			Namespace: cluster.Namespace,
		},
		&postgresPod,
	)

	if podErr == nil {
		if err := r.updateRestoreStatus(
			ctx,
			cluster,
			"ScalingDown",
			"waiting for the active PostgreSQL Pod to terminate before restoring the PVC",
			jobName,
		); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, nil
	}

	if !apierrors.IsNotFound(podErr) {
		return ctrl.Result{}, podErr
	}

	desiredRestoreJob, err := r.buildRestoreJob(
		cluster,
		labels,
		defaultImage,
	)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := ctrl.SetControllerReference(
		cluster,
		desiredRestoreJob,
		r.Scheme,
	); err != nil {
		return ctrl.Result{}, err
	}

	log.Info(
		"Creating PostgreSQL PITR restore Job",
		"name",
		desiredRestoreJob.Name,
		"requestId",
		cluster.Spec.Restore.RequestID,
		"backupId",
		cluster.Spec.Restore.BackupID,
	)

	if err := r.Create(
		ctx,
		desiredRestoreJob,
	); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.updateRestoreStatus(
		ctx,
		cluster,
		"Restoring",
		"restore Job created",
		jobName,
	); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{
		RequeueAfter: 5 * time.Second,
	}, nil
}

func validateRestoreRequest(
	cluster *databasev1.PostgreSQLCluster,
) error {
	if !cluster.Spec.Backup.Enabled {
		return fmt.Errorf(
			"spec.backup.enabled must be true before requesting a restore",
		)
	}

	if cluster.Spec.Restore.RequestID == "" {
		return fmt.Errorf(
			"spec.restore.requestId must not be empty",
		)
	}

	if !regexp.MustCompile(
		`^[a-z0-9][a-z0-9-]*$`,
	).MatchString(
		cluster.Spec.Restore.RequestID,
	) {
		return fmt.Errorf(
			"invalid spec.restore.requestId %q",
			cluster.Spec.Restore.RequestID,
		)
	}

	expectedConfirmation := "RESTORE " + cluster.Name
	if cluster.Spec.Restore.Confirmation !=
		expectedConfirmation {
		return fmt.Errorf(
			"spec.restore.confirmation must equal %q",
			expectedConfirmation,
		)
	}

	if cluster.Spec.Restore.BackupID == "" {
		return fmt.Errorf(
			"spec.restore.backupId must not be empty",
		)
	}

	if !barmanServerNamePattern.MatchString(
		cluster.Spec.Restore.BackupID,
	) {
		return fmt.Errorf(
			"invalid spec.restore.backupId %q",
			cluster.Spec.Restore.BackupID,
		)
	}

	if cluster.Spec.Restore.TargetTime == nil {
		return fmt.Errorf(
			"spec.restore.targetTime must not be empty",
		)
	}

	targetAction := effectiveRestoreTargetAction(
		cluster,
	)

	if targetAction != "promote" {
		return fmt.Errorf(
			"only spec.restore.targetAction=promote is supported by the current automated workflow",
		)
	}

	if cluster.Spec.Backup.SSHSecretName == "" {
		return fmt.Errorf(
			"spec.backup.sshSecretName must not be empty",
		)
	}

	return nil
}

func effectiveRestoreTargetAction(
	cluster *databasev1.PostgreSQLCluster,
) string {
	targetAction := cluster.Spec.Restore.TargetAction
	if targetAction == "" {
		targetAction = "promote"
	}

	return targetAction
}

func postgresPVCName(
	cluster *databasev1.PostgreSQLCluster,
) string {
	return "postgres-storage-" + cluster.Name + "-0"
}

func restoreJobName(
	cluster *databasev1.PostgreSQLCluster,
) string {
	name := cluster.Name +
		"-restore-" +
		cluster.Spec.Restore.RequestID

	if len(name) <= 63 {
		return name
	}

	name = strings.TrimRight(
		name[:63],
		"-",
	)

	return name
}

func isPodReady(
	pod *corev1.Pod,
) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady &&
			condition.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

func (r *PostgreSQLClusterReconciler) ensureStatefulSetReplicas(
	ctx context.Context,
	desiredStatefulSet *appsv1.StatefulSet,
	replicas int32,
) error {
	statefulSet := desiredStatefulSet.DeepCopy()
	statefulSet.Spec.Replicas = int32Ptr(
		replicas,
	)

	return r.reconcileStatefulSet(
		ctx,
		statefulSet,
	)
}

func (r *PostgreSQLClusterReconciler) updateRestoreStatus(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	phase string,
	message string,
	jobName string,
) error {
	originalStatus := cluster.Status

	cluster.Status.RestoreEnabled =
		cluster.Spec.Restore.Enabled

	cluster.Status.ObservedRestoreRequestID =
		cluster.Spec.Restore.RequestID

	cluster.Status.RestorePhase = phase
	cluster.Status.RestoreMessage = message
	cluster.Status.RestoreJob = jobName
	cluster.Status.RestoreBackupID =
		cluster.Spec.Restore.BackupID

	cluster.Status.RestoreTargetTime =
		cluster.Spec.Restore.TargetTime

	if reflect.DeepEqual(
		originalStatus,
		cluster.Status,
	) {
		return nil
	}

	return r.Status().Update(
		ctx,
		cluster,
	)
}

func (r *PostgreSQLClusterReconciler) buildRestoreJob(
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
	defaultImage string,
) (*batchv1.Job, error) {
	restoreImage := cluster.Spec.Restore.RestoreImage
	if restoreImage == "" {
		restoreImage = cluster.Spec.Backup.BackupImage
	}

	if restoreImage == "" {
		restoreImage = defaultImage
	}

	barmanUser := cluster.Spec.Backup.BarmanUser
	if barmanUser == "" {
		barmanUser = "barman"
	}

	barmanServerName := cluster.Spec.Backup.BarmanServerName
	if barmanServerName == "" {
		barmanServerName = cluster.Name
	}

	targetAction := effectiveRestoreTargetAction(
		cluster,
	)

	targetTime := cluster.Spec.Restore.TargetTime.
		Time.
		Format(
			time.RFC3339Nano,
		)

	preserveExistingData := "false"
	if cluster.Spec.Restore.PreserveExistingData {
		preserveExistingData = "true"
	}

	remoteCommand := fmt.Sprintf(
		`set -e
DEST="$(mktemp -d /tmp/%s-restore.XXXXXX)"
trap 'rm -rf "$DEST"' EXIT
barman recover %s %s "$DEST" \
  --target-time %s \
  --target-action %s >&2
tar -C "$DEST" -cf - .`,
		cluster.Name,
		shellQuote(barmanServerName),
		shellQuote(cluster.Spec.Restore.BackupID),
		shellQuote(targetTime),
		shellQuote(targetAction),
	)

	remoteCommandEncoded := base64.StdEncoding.EncodeToString(
		[]byte(remoteCommand),
	)

	remoteInvocation := "printf %s " +
		remoteCommandEncoded +
		" | base64 -d | bash"

	command := fmt.Sprintf(
		`set -euo pipefail

echo "Starting operator-managed PostgreSQL PITR restore"
echo "Runtime identity:"
id

PGROOT="/var/lib/postgresql/data"
PGDATA="${PGROOT}/pgdata"
REQUEST_ID=%s
STAGED_WAL="${PGROOT}/barman-wal-${REQUEST_ID}"
PRESERVE_EXISTING_DATA=%s

if [ -d "${PGDATA}" ]; then
  if [ "${PRESERVE_EXISTING_DATA}" = "true" ]; then
    PRESERVED="${PGROOT}/pgdata.before-${REQUEST_ID}"
    rm -rf "${PRESERVED}"
    mv "${PGDATA}" "${PRESERVED}"
    echo "Existing PGDATA preserved at ${PRESERVED}"
  else
    rm -rf "${PGDATA}"
  fi
fi

mkdir -p "${PGDATA}"
rm -rf "${STAGED_WAL}"

ssh \
  -i /etc/barman-ssh/id_ed25519 \
  -l %s \
  -o UserKnownHostsFile=/etc/barman-ssh/known_hosts \
  -o StrictHostKeyChecking=yes \
  -o IdentitiesOnly=yes \
  -o PreferredAuthentications=publickey \
  -o PasswordAuthentication=no \
  -o BatchMode=yes \
  %s \
  %s \
| tar \
    -C "${PGDATA}" \
    --no-same-owner \
    --no-same-permissions \
    --touch \
    --no-overwrite-dir \
    --strip-components=1 \
    -xf -

test -f "${PGDATA}/PG_VERSION"

if [ -d "${PGDATA}/barman_wal" ]; then
  mv "${PGDATA}/barman_wal" "${STAGED_WAL}"
fi

test -d "${STAGED_WAL}"

sed -i \
  "s#^restore_command = .*#restore_command = 'cp ${STAGED_WAL}/%%f %%p'#" \
  "${PGDATA}/postgresql.auto.conf"

touch "${PGDATA}/recovery.signal"

chmod -R u+rwX "${PGDATA}"
chmod 0700 "${PGDATA}"

echo
echo "Recovery configuration:"
grep -nE \
  'restore_command|recovery_target_time|recovery_target_action' \
  "${PGDATA}/postgresql.auto.conf"

echo
echo "RESTORE_JOB_OK"
`,
		shellQuote(cluster.Spec.Restore.RequestID),
		preserveExistingData,
		shellQuote(barmanUser),
		shellQuote(cluster.Spec.Backup.BarmanHost),
		shellQuote(remoteInvocation),
	)

	restoreLabels := map[string]string{}
	for key, value := range labels {
		restoreLabels[key] = value
	}

	restoreLabels["app.kubernetes.io/component"] =
		"restore"

	restoreLabels["database.iheb.local/restore-request"] =
		cluster.Spec.Restore.RequestID

	ttlSecondsAfterFinished := int32(
		3600,
	)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreJobName(cluster),
			Namespace: cluster.Namespace,
			Labels:    restoreLabels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(
				0,
			),
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: restoreLabels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "restore",
							Image:           restoreImage,
							ImagePullPolicy: corev1.PullAlways,
							Command: []string{
								"/bin/bash",
								"-lc",
							},
							Args: []string{
								command,
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse(
										"50m",
									),
									corev1.ResourceMemory: resource.MustParse(
										"64Mi",
									),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse(
										"500m",
									),
									corev1.ResourceMemory: resource.MustParse(
										"512Mi",
									),
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "postgres-storage",
									MountPath: "/var/lib/postgresql/data",
								},
								{
									Name:      "barman-ssh",
									MountPath: barmanSSHRuntimeDirectory,
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "postgres-storage",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: postgresPVCName(
										cluster,
									),
								},
							},
						},
						{
							Name: "barman-ssh",
							VolumeSource: corev1.VolumeSource{
								Projected: &corev1.ProjectedVolumeSource{
									DefaultMode: int32Ptr(
										0400,
									),
									Sources: []corev1.VolumeProjection{
										{
											Secret: &corev1.SecretProjection{
												LocalObjectReference: corev1.LocalObjectReference{
													Name: cluster.Spec.Backup.SSHSecretName,
												},
												Items: []corev1.KeyToPath{
													{
														Key:  "id_ed25519",
														Path: "id_ed25519",
													},
												},
											},
										},
										{
											ConfigMap: &corev1.ConfigMapProjection{
												LocalObjectReference: corev1.LocalObjectReference{
													Name: barmanKnownHostsConfigMap,
												},
												Items: []corev1.KeyToPath{
													{
														Key:  "known_hosts",
														Path: "known_hosts",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func restoreStabilizationJobName(
	cluster *databasev1.PostgreSQLCluster,
) string {
	name := cluster.Name +
		"-restore-stabilize-" +
		cluster.Spec.Restore.RequestID

	if len(name) <= 63 {
		return name
	}

	return strings.TrimRight(
		name[:63],
		"-",
	)
}

func (r *PostgreSQLClusterReconciler) reconcileRestoreStabilizationJob(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
	defaultImage string,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	jobName := restoreStabilizationJobName(
		cluster,
	)

	var stabilizationJob batchv1.Job
	err := r.Get(
		ctx,
		client.ObjectKey{
			Name:      jobName,
			Namespace: cluster.Namespace,
		},
		&stabilizationJob,
	)

	if apierrors.IsNotFound(err) {
		desiredJob, buildErr := r.buildRestoreStabilizationJob(
			cluster,
			labels,
			defaultImage,
		)
		if buildErr != nil {
			return ctrl.Result{}, buildErr
		}

		if referenceErr := ctrl.SetControllerReference(
			cluster,
			desiredJob,
			r.Scheme,
		); referenceErr != nil {
			return ctrl.Result{}, referenceErr
		}

		log.Info(
			"Creating post-PITR Barman stabilization Job",
			"name",
			desiredJob.Name,
			"requestId",
			cluster.Spec.Restore.RequestID,
		)

		if createErr := r.Create(
			ctx,
			desiredJob,
		); createErr != nil {
			return ctrl.Result{}, createErr
		}

		if statusErr := r.updateRestoreStatus(
			ctx,
			cluster,
			"Stabilizing",
			"PostgreSQL promoted successfully; waiting for Barman streaming reset",
			jobName,
		); statusErr != nil {
			return ctrl.Result{}, statusErr
		}

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, nil
	}

	if err != nil {
		return ctrl.Result{}, err
	}

	if stabilizationJob.Status.Failed > 0 {
		message := fmt.Sprintf(
			"post-PITR stabilization Job %q failed; inspect its logs",
			jobName,
		)

		if statusErr := r.updateRestoreStatus(
			ctx,
			cluster,
			"Failed",
			message,
			jobName,
		); statusErr != nil {
			return ctrl.Result{}, statusErr
		}

		return ctrl.Result{}, nil
	}

	if stabilizationJob.Status.Succeeded == 0 {
		if statusErr := r.updateRestoreStatus(
			ctx,
			cluster,
			"Stabilizing",
			"waiting for Barman receive-wal to restart on the promoted timeline",
			jobName,
		); statusErr != nil {
			return ctrl.Result{}, statusErr
		}

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, nil
	}

	now := metav1.Now()
	cluster.Status.LastRestoreTime = &now

	if statusErr := r.updateRestoreStatus(
		ctx,
		cluster,
		"Completed",
		"restore and post-PITR streaming stabilization completed successfully; disable spec.restore.enabled to resume normal operation",
		jobName,
	); statusErr != nil {
		return ctrl.Result{}, statusErr
	}

	log.Info(
		"PostgreSQL PITR restore and Barman stabilization completed successfully",
		"requestId",
		cluster.Spec.Restore.RequestID,
		"job",
		jobName,
	)

	return ctrl.Result{}, nil
}

func (r *PostgreSQLClusterReconciler) buildRestoreStabilizationJob(
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
	defaultImage string,
) (*batchv1.Job, error) {
	image := cluster.Spec.Restore.RestoreImage
	if image == "" {
		image = cluster.Spec.Backup.BackupImage
	}

	if image == "" {
		image = defaultImage
	}

	barmanUser := cluster.Spec.Backup.BarmanUser
	if barmanUser == "" {
		barmanUser = "barman"
	}

	barmanServerName := cluster.Spec.Backup.BarmanServerName
	if barmanServerName == "" {
		barmanServerName = cluster.Name
	}

	remoteCommand := fmt.Sprintf(
		`set -euo pipefail

echo "Resetting Barman receive-wal state after PostgreSQL PITR promotion"

barman receive-wal \
  --reset \
  %s

barman cron

attempt=1

while [ "${attempt}" -le 12 ]; do
  echo
  echo "Barman health-check attempt ${attempt}"

  CHECK_OUTPUT="$(
    barman check %s 2>&1 || true
  )"

  printf '%%s\n' "${CHECK_OUTPUT}"

  if printf '%%s\n' "${CHECK_OUTPUT}" |
      grep -q "replication slot: OK" &&
    printf '%%s\n' "${CHECK_OUTPUT}" |
      grep -q "receive-wal running: OK"
  then
    echo
    echo "BARMAN_STREAMING_STABILIZATION_OK"
    exit 0
  fi

  barman cron
  attempt=$((attempt + 1))
  sleep 5
done

echo "Barman streaming stabilization failed" >&2
exit 1`,
		shellQuote(barmanServerName),
		shellQuote(barmanServerName),
	)

	encodedRemoteCommand := base64.StdEncoding.EncodeToString(
		[]byte(remoteCommand),
	)

	remoteInvocation := "printf %s " +
		encodedRemoteCommand +
		" | base64 -d | bash"

	command := fmt.Sprintf(
		`set -euo pipefail

echo "Starting post-PITR Barman stabilization"

ssh \
  -i /etc/barman-ssh/id_ed25519 \
  -l %s \
  -o UserKnownHostsFile=/etc/barman-ssh/known_hosts \
  -o StrictHostKeyChecking=yes \
  -o IdentitiesOnly=yes \
  -o PreferredAuthentications=publickey \
  -o PasswordAuthentication=no \
  -o BatchMode=yes \
  %s \
  %s

echo
echo "POST_RESTORE_STABILIZATION_JOB_OK"
`,
		shellQuote(barmanUser),
		shellQuote(cluster.Spec.Backup.BarmanHost),
		shellQuote(remoteInvocation),
	)

	stabilizationLabels := map[string]string{}
	for key, value := range labels {
		stabilizationLabels[key] = value
	}

	stabilizationLabels["app.kubernetes.io/component"] =
		"restore-stabilization"

	stabilizationLabels["database.iheb.local/restore-request"] =
		cluster.Spec.Restore.RequestID

	ttlSecondsAfterFinished := int32(
		3600,
	)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: restoreStabilizationJobName(
				cluster,
			),
			Namespace: cluster.Namespace,
			Labels:    stabilizationLabels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(
				0,
			),
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: stabilizationLabels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "stabilize",
							Image:           image,
							ImagePullPolicy: corev1.PullAlways,
							Command: []string{
								"/bin/bash",
								"-lc",
							},
							Args: []string{
								command,
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse(
										"25m",
									),
									corev1.ResourceMemory: resource.MustParse(
										"32Mi",
									),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU: resource.MustParse(
										"250m",
									),
									corev1.ResourceMemory: resource.MustParse(
										"256Mi",
									),
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "barman-ssh",
									MountPath: barmanSSHRuntimeDirectory,
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "barman-ssh",
							VolumeSource: corev1.VolumeSource{
								Projected: &corev1.ProjectedVolumeSource{
									DefaultMode: int32Ptr(
										0400,
									),
									Sources: []corev1.VolumeProjection{
										{
											Secret: &corev1.SecretProjection{
												LocalObjectReference: corev1.LocalObjectReference{
													Name: cluster.Spec.Backup.SSHSecretName,
												},
												Items: []corev1.KeyToPath{
													{
														Key:  "id_ed25519",
														Path: "id_ed25519",
													},
												},
											},
										},
										{
											ConfigMap: &corev1.ConfigMapProjection{
												LocalObjectReference: corev1.LocalObjectReference{
													Name: barmanKnownHostsConfigMap,
												},
												Items: []corev1.KeyToPath{
													{
														Key:  "known_hosts",
														Path: "known_hosts",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}, nil
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
			Type:                  corev1.ServiceTypeNodePort,
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
			Selector:              labels,
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

	if existingService.Spec.ExternalTrafficPolicy !=
		desiredService.Spec.ExternalTrafficPolicy {
		existingService.Spec.ExternalTrafficPolicy =
			desiredService.Spec.ExternalTrafficPolicy
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

func (r *PostgreSQLClusterReconciler) reconcileBackupCronJob(
	ctx context.Context,
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
	defaultImage string,
) error {
	log := logf.FromContext(ctx)
	cronJobName := cluster.Name + "-barman-backup"

	var existingCronJob batchv1.CronJob
	getErr := r.Get(
		ctx,
		client.ObjectKey{
			Name:      cronJobName,
			Namespace: cluster.Namespace,
		},
		&existingCronJob,
	)

	shouldSchedule := cluster.Spec.Backup.Enabled &&
		cluster.Spec.Backup.Schedule != ""

	if !shouldSchedule {
		if apierrors.IsNotFound(getErr) {
			return nil
		}

		if getErr != nil {
			return getErr
		}

		log.Info(
			"Deleting scheduled Barman backup CronJob",
			"name",
			cronJobName,
		)

		return r.Delete(
			ctx,
			&existingCronJob,
		)
	}

	desiredCronJob, err := r.buildBackupCronJob(
		cluster,
		labels,
		defaultImage,
	)
	if err != nil {
		return err
	}

	if apierrors.IsNotFound(getErr) {
		if err := ctrl.SetControllerReference(
			cluster,
			desiredCronJob,
			r.Scheme,
		); err != nil {
			return err
		}

		log.Info(
			"Creating scheduled Barman backup CronJob",
			"name",
			desiredCronJob.Name,
			"schedule",
			desiredCronJob.Spec.Schedule,
		)

		return r.Create(
			ctx,
			desiredCronJob,
		)
	}

	if getErr != nil {
		return getErr
	}

	needsUpdate := false

	if !reflect.DeepEqual(
		existingCronJob.Labels,
		desiredCronJob.Labels,
	) {
		existingCronJob.Labels = desiredCronJob.Labels
		needsUpdate = true
	}

	if !reflect.DeepEqual(
		existingCronJob.Spec,
		desiredCronJob.Spec,
	) {
		existingCronJob.Spec = desiredCronJob.Spec
		needsUpdate = true
	}

	if !needsUpdate {
		return nil
	}

	log.Info(
		"Updating scheduled Barman backup CronJob",
		"name",
		existingCronJob.Name,
		"schedule",
		desiredCronJob.Spec.Schedule,
	)

	return r.Update(
		ctx,
		&existingCronJob,
	)
}

func (r *PostgreSQLClusterReconciler) buildBackupCronJob(
	cluster *databasev1.PostgreSQLCluster,
	labels map[string]string,
	defaultImage string,
) (*batchv1.CronJob, error) {
	if cluster.Spec.Backup.Schedule == "" {
		return nil, fmt.Errorf(
			"spec.backup.schedule must not be empty when scheduled backups are enabled",
		)
	}

	if cluster.Spec.Backup.BarmanHost == "" {
		return nil, fmt.Errorf(
			"spec.backup.barmanHost must not be empty",
		)
	}

	if cluster.Spec.Backup.SSHSecretName == "" {
		return nil, fmt.Errorf(
			"spec.backup.sshSecretName must not be empty",
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
		return nil, fmt.Errorf(
			"invalid Barman server name %q",
			barmanServerName,
		)
	}

	backupImage := cluster.Spec.Backup.BackupImage
	if backupImage == "" {
		backupImage = defaultImage
	}

	cronJobLabels := map[string]string{}
	for key, value := range labels {
		cronJobLabels[key] = value
	}
	cronJobLabels["app.kubernetes.io/component"] = "backup"

	command := fmt.Sprintf(
		`set -euo pipefail

echo "Starting scheduled Barman backup for %s"

for attempt in 1 2 3; do
  echo "Backup attempt ${attempt}/3"

  if ssh \
    -i /etc/barman-ssh/id_ed25519 \
    -l %s \
    -o UserKnownHostsFile=/etc/barman-ssh/known_hosts \
    -o StrictHostKeyChecking=yes \
    -o IdentitiesOnly=yes \
    -o PreferredAuthentications=publickey \
    -o PasswordAuthentication=no \
    -o BatchMode=yes \
    %s \
    "barman backup %s --wait"
  then
    echo "Scheduled Barman backup completed successfully"
    exit 0
  fi

  if [ "${attempt}" -lt 3 ]; then
    delay=$((attempt * 20))
    echo "Backup attempt failed; retrying in ${delay}s"
    sleep "${delay}"
  fi
done

echo "Scheduled Barman backup failed after 3 attempts" >&2
exit 1
`,
		shellQuote(barmanServerName),
		shellQuote(barmanUser),
		shellQuote(cluster.Spec.Backup.BarmanHost),
		shellQuote(barmanServerName),
	)

	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-barman-backup",
			Namespace: cluster.Namespace,
			Labels:    cronJobLabels,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:          cluster.Spec.Backup.Schedule,
			ConcurrencyPolicy: batchv1.ForbidConcurrent,
			Suspend: boolPtr(
				cluster.Spec.Backup.SuspendScheduledBackups ||
					cluster.Spec.Restore.Enabled,
			),
			SuccessfulJobsHistoryLimit: int32Ptr(
				3,
			),
			FailedJobsHistoryLimit: int32Ptr(
				3,
			),
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					BackoffLimit: int32Ptr(
						0,
					),
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: cronJobLabels,
						},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Name:            "barman-backup",
									Image:           backupImage,
									ImagePullPolicy: corev1.PullAlways,
									Command: []string{
										"/bin/bash",
										"-lc",
									},
									Args: []string{
										command,
									},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse(
												"50m",
											),
											corev1.ResourceMemory: resource.MustParse(
												"64Mi",
											),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse(
												"250m",
											),
											corev1.ResourceMemory: resource.MustParse(
												"256Mi",
											),
										},
									},
									VolumeMounts: []corev1.VolumeMount{
										{
											Name:      "barman-ssh",
											MountPath: barmanSSHRuntimeDirectory,
											ReadOnly:  true,
										},
									},
								},
							},
							Volumes: []corev1.Volume{
								{
									Name: "barman-ssh",
									VolumeSource: corev1.VolumeSource{
										Projected: &corev1.ProjectedVolumeSource{
											DefaultMode: int32Ptr(
												0400,
											),
											Sources: []corev1.VolumeProjection{
												{
													Secret: &corev1.SecretProjection{
														LocalObjectReference: corev1.LocalObjectReference{
															Name: cluster.Spec.Backup.SSHSecretName,
														},
														Items: []corev1.KeyToPath{
															{
																Key:  "id_ed25519",
																Path: "id_ed25519",
															},
														},
													},
												},
												{
													ConfigMap: &corev1.ConfigMapProjection{
														LocalObjectReference: corev1.LocalObjectReference{
															Name: barmanKnownHostsConfigMap,
														},
														Items: []corev1.KeyToPath{
															{
																Key:  "known_hosts",
																Path: "known_hosts",
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}, nil
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

func shellQuote(
	value string,
) string {
	return "'" + strings.ReplaceAll(
		value,
		"'",
		"'\\\"'\\\"'",
	) + "'"
}

func boolPtr(
	value bool,
) *bool {
	return &value
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
		Owns(
			&batchv1.CronJob{},
		).
		Owns(
			&batchv1.Job{},
		).
		Named(
			"postgresqlcluster",
		).
		Complete(
			r,
		)
}
