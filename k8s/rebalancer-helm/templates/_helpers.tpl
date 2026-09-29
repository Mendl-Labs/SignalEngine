{{/*
Expand the name of the chart.
*/}}
{{- define "rebalancer-helm.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name. Truncated at 63 chars (Kubernetes DNS naming limit).
*/}}
{{- define "rebalancer-helm.fullname" -}}
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
{{- define "rebalancer-helm.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "rebalancer-helm.labels" -}}
helm.sh/chart: {{ include "rebalancer-helm.chart" . }}
{{ include "rebalancer-helm.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "rebalancer-helm.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rebalancer-helm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "rebalancer-helm.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "rebalancer-helm.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
A validated UUID from .Values.pilot.tenantId / .Values.pilot.accountId. Fails the render on
anything that isn't a real (non-nil) UUID -- this chart serves exactly ONE allow-listed
(tenant, account) pair (PilotAllowList::parse refuses an empty list or the nil UUID at the
application layer too; this is the same check at the chart layer, so a bad values file fails
`helm template`/`helm install` immediately instead of producing a pod that will exit(1) on its
first PilotConfig::from_env() call).
*/}}
{{- define "rebalancer-helm.validatedUuid" -}}
{{- $label := index . 0 -}}
{{- $value := index . 1 | toString -}}
{{- if not (regexMatch "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$" $value) -}}
{{- fail (printf "%s must be a UUID, got '%s'. This chart serves exactly one allow-listed (tenant, account) pair -- see values.yaml's pilot.tenantId/pilot.accountId." $label $value) -}}
{{- end -}}
{{- if eq (lower $value) "00000000-0000-0000-0000-000000000000" -}}
{{- fail (printf "%s must not be the nil UUID (PilotAllowList::parse refuses it too)." $label) -}}
{{- end -}}
{{- $value -}}
{{- end }}

{{/*
Fails the render if image.tag is empty. The build workflow never pushes :latest (immutable
short-SHA tags only, matching k8s/signal-engine-helm's own ci-cd.yml convention) and this chart's
own values.yaml default is deliberately blank, so an operator must name a real, pushed tag.
*/}}
{{- define "rebalancer-helm.validatedImageTag" -}}
{{- $tag := .Values.image.tag | toString -}}
{{- if eq $tag "" -}}
{{- fail "image.tag must be set to an immutable tag pushed by .github/workflows/rebalancer-service-image.yml (e.g. a short git SHA) -- :latest is never pushed, and this chart has no default tag on purpose." -}}
{{- end -}}
{{- $tag -}}
{{- end }}

{{/*
Fails the render if .Values.extraEnv names the one environment variable this binary must never
see: CREDENTIALS_ENCRYPTION_KEY (the tenant-credential master key). rebalancer-service's own
startup check (PilotConfig::from_lookup, crates/rebalancer-service/src/pilot.rs) already refuses
to start if this variable is present in its process environment at all -- this is the same rule
enforced one layer earlier, at chart-render time, so a values.yaml mistake never even reaches a
running (and then immediately exiting) pod.
*/}}
{{- define "rebalancer-helm.validatedExtraEnv" -}}
{{- range .Values.extraEnv }}
{{- if eq .name "CREDENTIALS_ENCRYPTION_KEY" }}
{{- fail "extraEnv must not set CREDENTIALS_ENCRYPTION_KEY: rebalancer-service refuses to start if this variable is present in its environment at all (see PilotConfig::from_lookup's FORBIDDEN_ENV check) -- this chart deliberately never wires the tenant-credential master key to the pilot rebalancer." }}
{{- end }}
{{- end }}
{{- end }}
