package controller

import (
	"context"

	databasev1 "github.com/ihebmbarek/postgres-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("PostgreSQLRestore Controller", func() {
	Context("When creating a PostgreSQLRestore resource", func() {
		It("should create the resource with a valid clusterName", func() {
			ctx := context.Background()

			restore := &databasev1.PostgreSQLRestore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-restore",
					Namespace: "default",
				},
				Spec: databasev1.PostgreSQLRestoreSpec{
					ClusterName:        "sample-postgres",
					BarmanServerName:   "sample-postgres",
					RestoreMode:        "pitr",
					TargetTime:         "2026-06-18 16:30:00",
					TargetDatabaseName: "sample-postgres-restored",
				},
			}

			Expect(k8sClient.Create(ctx, restore)).To(Succeed())

			created := &databasev1.PostgreSQLRestore{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "test-restore",
				Namespace: "default",
			}, created)).To(Succeed())

			Expect(created.Spec.ClusterName).To(Equal("sample-postgres"))
			Expect(created.Spec.RestoreMode).To(Equal("pitr"))
		})
	})
})
