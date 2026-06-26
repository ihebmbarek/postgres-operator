package controller

import (
	"context"

	databasev1 "github.com/ihebmbarek/postgres-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("PostgreSQLBackup Controller", func() {
	Context("When creating a PostgreSQLBackup resource", func() {
		It("should create the resource with a valid clusterName", func() {
			ctx := context.Background()

			backup := &databasev1.PostgreSQLBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-backup",
					Namespace: "default",
				},
				Spec: databasev1.PostgreSQLBackupSpec{
					ClusterName:      "sample-postgres",
					BarmanServerName: "sample-postgres",
					BackupType:       "full",
					Immediate:        true,
				},
			}

			Expect(k8sClient.Create(ctx, backup)).To(Succeed())

			created := &databasev1.PostgreSQLBackup{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "test-backup",
				Namespace: "default",
			}, created)).To(Succeed())

			Expect(created.Spec.ClusterName).To(Equal("sample-postgres"))
			Expect(created.Spec.BackupType).To(Equal("full"))
		})
	})
})
