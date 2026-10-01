{{/*
Validated credential mode: none | single_tenant | multi_tenant (see values.yaml config.credentialMode).
Fails the render on an unknown mode; on single_tenant without a UUID tenantId or without the
ConfigMap that carries it; and on multi_tenant WITH a tenantId (multi_tenant serves each live
deployment's OWN tenant from the database, so a process-wide tenant makes no sense and would
only invite the two modes being confused). A live-capable release can never be rendered by
accident with a missing/garbled tenant, and multi_tenant is never implied: it must be named.
*/}}
{{- define "signal-engine-helm.credentialMode" -}}
{{- $mode := default "none" .Values.config.credentialMode | toString | lower -}}
{{- if not (has $mode (list "none" "single_tenant" "multi_tenant")) -}}
{{- fail (printf "config.credentialMode must be 'none', 'single_tenant' or 'multi_tenant', got '%s'." $mode) -}}
{{- end -}}
{{- if eq $mode "multi_tenant" -}}
{{- $mtTenant := default "" .Values.config.tenantId | toString -}}
{{- if ne $mtTenant "" -}}
{{- fail (printf "config.credentialMode=multi_tenant serves every tenant from the database (each live deployment's own tenant); config.tenantId must be empty, got '%s'. Use single_tenant to bind one tenant." $mtTenant) -}}
{{- end -}}
{{- end -}}
{{- if eq $mode "single_tenant" -}}
{{- $tenant := default "" .Values.config.tenantId | toString -}}
{{- if not (regexMatch "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$" $tenant) -}}
{{- fail (printf "config.credentialMode=single_tenant requires config.tenantId to be a UUID, got '%s'" $tenant) -}}
{{- end -}}
{{- if eq (lower $tenant) "00000000-0000-0000-0000-000000000000" -}}
{{- fail "config.credentialMode=single_tenant requires a real config.tenantId, not the nil UUID" -}}
{{- end -}}
{{- if not .Values.configMap.enabled -}}
{{- fail "config.credentialMode=single_tenant needs configMap.enabled=true (TENANT_ID is read from the chart ConfigMap)" -}}
{{- end -}}
{{- end -}}
{{- $mode -}}
{{- end }}

{{/*
Expand the name of the chart.
*/}}
{{- define "signal-engine-helm.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "signal-engine-helm.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "signal-engine-helm.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "signal-engine-helm.labels" -}}
helm.sh/chart: {{ include "signal-engine-helm.chart" . }}
{{ include "signal-engine-helm.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "signal-engine-helm.selectorLabels" -}}
app.kubernetes.io/name: {{ include "signal-engine-helm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "signal-engine-helm.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "signal-engine-helm.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
