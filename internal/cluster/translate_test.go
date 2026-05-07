// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package cluster

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
	"github.com/carlos-loya/archive-purge-restore/internal/controller"
)

func TestTranslateArchive_HappyPath(t *testing.T) {
	t.Setenv(controller.EnvDatabaseUsername, "admin")
	t.Setenv(controller.EnvDatabasePassword, "hunter2")

	rule := newRule()
	dbc := newDBC(aprv1alpha1.EnginePostgres, 0)
	sb := newSB(aprv1alpha1.StorageS3)

	ec, err := TranslateArchive(rule, dbc, sb)
	if err != nil {
		t.Fatalf("TranslateArchive: %v", err)
	}
	if ec.Rule.Name != rule.Name {
		t.Errorf("rule name = %q, want %q", ec.Rule.Name, rule.Name)
	}
	if got, want := ec.Rule.Source.Engine, "postgres"; got != want {
		t.Errorf("engine = %q, want %q", got, want)
	}
	if ec.Rule.Source.Port != 5432 {
		t.Errorf("postgres port should default to 5432, got %d", ec.Rule.Source.Port)
	}
	if ec.Rule.Source.SSLMode != "prefer" {
		t.Errorf("ssl_mode should default to prefer, got %q", ec.Rule.Source.SSLMode)
	}
	if ec.Rule.Source.Credentials.Username != "admin" || ec.Rule.Source.Credentials.Password != "hunter2" {
		t.Errorf("credentials not propagated: %+v", ec.Rule.Source.Credentials)
	}
	if len(ec.Rule.Tables) != 1 || ec.Rule.Tables[0].Name != rule.Spec.Table {
		t.Errorf("table not propagated: %+v", ec.Rule.Tables)
	}
	if ec.Config.Storage.Type != "s3" || ec.Config.Storage.S3 == nil {
		t.Errorf("storage.type = %q, S3 nil? %v", ec.Config.Storage.Type, ec.Config.Storage.S3 == nil)
	}
}

func TestTranslateArchive_MissingCredentials(t *testing.T) {
	t.Setenv(controller.EnvDatabaseUsername, "")
	t.Setenv(controller.EnvDatabasePassword, "")

	_, err := TranslateArchive(newRule(), newDBC(aprv1alpha1.EnginePostgres, 0), newSB(aprv1alpha1.StorageFilesystem))
	if err == nil {
		t.Fatal("expected error when credentials env vars are unset")
	}
	if !strings.Contains(err.Error(), controller.EnvDatabaseUsername) {
		t.Errorf("error should mention env var name, got %q", err.Error())
	}
}

func TestTranslateArchive_PortDefaulting(t *testing.T) {
	t.Setenv(controller.EnvDatabaseUsername, "u")
	t.Setenv(controller.EnvDatabasePassword, "p")

	cases := []struct {
		engine   aprv1alpha1.DatabaseEngine
		wantPort int
	}{
		{aprv1alpha1.EnginePostgres, 5432},
		{aprv1alpha1.EngineTimescaleDB, 5432},
		{aprv1alpha1.EngineMySQL, 3306},
	}
	for _, tc := range cases {
		t.Run(string(tc.engine), func(t *testing.T) {
			ec, err := TranslateArchive(newRule(), newDBC(tc.engine, 0), newSB(aprv1alpha1.StorageFilesystem))
			if err != nil {
				t.Fatal(err)
			}
			if ec.Rule.Source.Port != tc.wantPort {
				t.Errorf("port = %d, want %d", ec.Rule.Source.Port, tc.wantPort)
			}
		})
	}
}

func TestTranslateArchive_ExplicitPortRespected(t *testing.T) {
	t.Setenv(controller.EnvDatabaseUsername, "u")
	t.Setenv(controller.EnvDatabasePassword, "p")

	dbc := newDBC(aprv1alpha1.EnginePostgres, 6543)
	ec, err := TranslateArchive(newRule(), dbc, newSB(aprv1alpha1.StorageFilesystem))
	if err != nil {
		t.Fatal(err)
	}
	if ec.Rule.Source.Port != 6543 {
		t.Errorf("explicit port not honored: got %d", ec.Rule.Source.Port)
	}
}

func TestTranslateArchive_StorageTypes(t *testing.T) {
	t.Setenv(controller.EnvDatabaseUsername, "u")
	t.Setenv(controller.EnvDatabasePassword, "p")

	cases := []struct {
		typ  aprv1alpha1.StorageType
		want string
	}{
		{aprv1alpha1.StorageS3, "s3"},
		{aprv1alpha1.StorageR2, "r2"},
		{aprv1alpha1.StorageGCS, "gcs"},
		{aprv1alpha1.StorageFilesystem, "filesystem"},
	}
	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			ec, err := TranslateArchive(newRule(), newDBC(aprv1alpha1.EnginePostgres, 0), newSB(tc.typ))
			if err != nil {
				t.Fatal(err)
			}
			if ec.Config.Storage.Type != tc.want {
				t.Errorf("storage.type = %q, want %q", ec.Config.Storage.Type, tc.want)
			}
		})
	}
}

func TestParseRef(t *testing.T) {
	cases := []struct {
		in       string
		wantNS   string
		wantName string
		wantErr  bool
	}{
		{"default/orders", "default", "orders", false},
		{"a/b/c", "a", "b/c", false},
		{"justaname", "", "", true},
		{"/no-namespace", "", "", true},
		{"no-name/", "", "", true},
		{"", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			ns, name, err := ParseRef(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if ns != tc.wantNS || name != tc.wantName {
				t.Errorf("got (%q, %q), want (%q, %q)", ns, name, tc.wantNS, tc.wantName)
			}
		})
	}
}

// --- helpers ---

func newRule() *aprv1alpha1.ArchiveRule {
	return &aprv1alpha1.ArchiveRule{
		ObjectMeta: metav1.ObjectMeta{Name: "rule1", Namespace: "default"},
		Spec: aprv1alpha1.ArchiveRuleSpec{
			DatabaseRef: corev1.LocalObjectReference{Name: "dbc1"},
			StorageRef:  corev1.LocalObjectReference{Name: "sb1"},
			Table:       "orders",
			DateColumn:  "created_at",
			DaysOnline:  90,
			Schedule:    "0 2 * * *",
		},
	}
}

func newDBC(engine aprv1alpha1.DatabaseEngine, port int32) *aprv1alpha1.DatabaseConnection {
	return &aprv1alpha1.DatabaseConnection{
		ObjectMeta: metav1.ObjectMeta{Name: "dbc1", Namespace: "default"},
		Spec: aprv1alpha1.DatabaseConnectionSpec{
			Engine:               engine,
			Host:                 "pg.example.com",
			Port:                 port,
			Database:             "orders",
			CredentialsSecretRef: corev1.LocalObjectReference{Name: "dbc1-creds"},
		},
	}
}

func newSB(typ aprv1alpha1.StorageType) *aprv1alpha1.StorageBackend {
	sb := &aprv1alpha1.StorageBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "sb1", Namespace: "default"},
		Spec: aprv1alpha1.StorageBackendSpec{
			Type:   typ,
			Bucket: "test-archive",
			Region: "us-west-2",
		},
	}
	if typ == aprv1alpha1.StorageR2 {
		sb.Spec.AccountID = "abc123"
	}
	if typ == aprv1alpha1.StorageFilesystem {
		sb.Spec.Bucket = "/tmp/apr-archives"
	}
	return sb
}
