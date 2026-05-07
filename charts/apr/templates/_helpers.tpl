{{/* vim: set filetype=mustache: */}}

{{/*
Expand the name of the chart.
*/}}
{{- define "apr.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully-qualified release-scoped name (used for the manager Deployment etc).
*/}}
{{- define "apr.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "apr.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "apr.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{/*
Selector labels (subset of common labels — must be stable for Deployment
selectors, which is why we exclude version/managed-by/etc).
*/}}
{{- define "apr.selectorLabels" -}}
app.kubernetes.io/name: {{ include "apr.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Resolved image reference. Falls back to .Chart.AppVersion when
.Values.image.tag is empty.
*/}}
{{- define "apr.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Manager ServiceAccount name. Always release-scoped.
*/}}
{{- define "apr.managerServiceAccountName" -}}
{{- printf "%s-manager" (include "apr.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Runner ServiceAccount name. Configurable via .Values.runner.serviceAccount.name
because the value is also used as the --archive-runner-service-account flag
on the manager. The default ("apr-runner") is *not* release-scoped so that
multiple installs of this chart in the same cluster will share the runner
SA — which is fine since archive Jobs land in user namespaces, not the
operator's.
*/}}
{{- define "apr.runnerServiceAccountName" -}}
{{- .Values.runner.serviceAccount.name | default "apr-runner" -}}
{{- end -}}
