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
{{- if eq .name "REBALANCER_SERVE_PROPOSAL_PLANS" }}
{{- fail "extraEnv must not set REBALANCER_SERVE_PROPOSAL_PLANS: use the serveProposalPlans value, which also enforces the credential-broker requirement." }}
{{- end }}
{{- if eq .name "CREDENTIALS_ENCRYPTION_KEY" }}
{{- fail "extraEnv must not set CREDENTIALS_ENCRYPTION_KEY: rebalancer-service refuses to start if this variable is present in its environment at all (see PilotConfig::from_lookup's FORBIDDEN_ENV check) -- this chart deliberately never wires the tenant-credential master key to the pilot rebalancer." }}
{{- end }}
{{- end }}
{{- end }}

{{/*
"true" when the owner pilot (tenant, account) pair is served, else empty. With serveProposalPlans off the
pair is always served (and validatedUuid fails on a bad/nil id, as before). With it on, both ids unset
(empty or the nil UUID) means no pilot pair; both set means the pair is served; exactly one is a render
error (mirrors PilotConfig::from_lookup: a half-configured pair is a mistake, never "no pilot").
*/}}
{{- define "rebalancer-helm.pilotPresent" -}}
{{- if eq (toString .Values.serveProposalPlans) "true" -}}
{{- $nil := "00000000-0000-0000-0000-000000000000" -}}
{{- $t := .Values.pilot.tenantId | toString | trim | lower -}}
{{- $a := .Values.pilot.accountId | toString | trim | lower -}}
{{- $tSet := and (ne $t "") (ne $t $nil) -}}
{{- $aSet := and (ne $a "") (ne $a $nil) -}}
{{- if and $tSet $aSet -}}true
{{- else if or $tSet $aSet -}}
{{- fail "serveProposalPlans: set BOTH pilot.tenantId and pilot.accountId (owner pilot alongside proposal plans) or NEITHER (proposal plans only); exactly one is a half-configured pilot pair." -}}
{{- end -}}
{{- else -}}true
{{- end -}}
{{- end }}

{{/*
Fails the render when serveProposalPlans is on without the credential broker: with
REBALANCER_SERVE_PROPOSAL_PLANS=true the service cannot decrypt tenant credentials itself
(CREDENTIALS_ENCRYPTION_KEY stays forbidden) and refuses to start without the broker URL and token.
*/}}
{{- define "rebalancer-helm.validatedProposalPlans" -}}
{{- if eq (toString .Values.serveProposalPlans) "true" -}}
{{- if not .Values.credentialBroker.enabled -}}
{{- fail "serveProposalPlans=true requires credentialBroker.enabled=true: this pod cannot decrypt tenant credentials (CREDENTIALS_ENCRYPTION_KEY is forbidden here) and fetches each paper key from the Engine credential broker. Create Secret rebalancer-credential-broker with keys url and token, then set credentialBroker.enabled=true." -}}
{{- end -}}
{{- if or (not .Values.credentialBroker.secretName) (not .Values.credentialBroker.urlKey) (not .Values.credentialBroker.tokenKey) -}}
{{- fail "serveProposalPlans=true requires credentialBroker.secretName, credentialBroker.urlKey and credentialBroker.tokenKey (the Secret holding the broker URL and bearer token)." -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Validates the Secret-backed optional env: extraEnvFromSecrets (generic), alerts, heartbeat and dataGate. Renders nothing;
only fails. Rules: every extraEnvFromSecrets entry needs name, secretName and key; the name may not be the tenant
master key, the proposal-plans flag, the paper-only interlock, CREDENTIAL_MODE, a live-venue key (ALPACA_LIVE*, OANDA_*,
KRAKEN_*), nor any env this chart already manages itself (use the dedicated value instead). alerts.enabled=true needs
alerts.secretName (ALERTS_ENABLED=true without a RESEND_API_KEY/ALERT_FROM_EMAIL source only records sender_disabled).
*/}}
{{- define "rebalancer-helm.validatedSecretEnv" -}}
{{- $reserved := list "CREDENTIALS_ENCRYPTION_KEY" "REBALANCER_SERVE_PROPOSAL_PLANS" "REBALANCER_PAPER_ONLY" "CREDENTIAL_MODE" "DATABASE_URL" "MASSIVE_API_KEY" "PILOT_TENANT_ID" "PILOT_ACCOUNT_ID" "PILOT_ALPACA_KEY_ID" "PILOT_ALPACA_KEY_SECRET" "REBALANCER_CREDENTIAL_BROKER_URL" "CREDENTIAL_BROKER_SERVICE_TOKEN" "REBALANCER_TICK_SECS" "ALERTS_ENABLED" "RESEND_API_KEY" "ALERT_FROM_EMAIL" "HEARTBEAT_URL" "ALPACA_DATA_KEY_ID" "ALPACA_DATA_KEY_SECRET" "POD_NAME" "POD_NAMESPACE" -}}
{{- range $i, $e := .Values.extraEnvFromSecrets }}
{{- if or (not $e.name) (not $e.secretName) (not $e.key) }}
{{- fail (printf "extraEnvFromSecrets[%d] needs name, secretName and key." $i) }}
{{- end }}
{{- if eq $e.name "CREDENTIALS_ENCRYPTION_KEY" }}
{{- fail "extraEnvFromSecrets must not set CREDENTIALS_ENCRYPTION_KEY: rebalancer-service refuses to start if this variable is present in its environment at all (see PilotConfig::from_lookup's FORBIDDEN_ENV check)." }}
{{- end }}
{{- if eq $e.name "REBALANCER_SERVE_PROPOSAL_PLANS" }}
{{- fail "extraEnvFromSecrets must not set REBALANCER_SERVE_PROPOSAL_PLANS: use the serveProposalPlans value, which also enforces the credential-broker requirement." }}
{{- end }}
{{- if or (has $e.name $reserved) (regexMatch "^(ALPACA_LIVE|OANDA_|KRAKEN_)" $e.name) }}
{{- fail (printf "extraEnvFromSecrets[%d]: %s is reserved (managed by this chart, a safety interlock, or a live-venue credential this pod must never hold); use the dedicated value (alerts, heartbeat, dataGate, secrets, credentialBroker) where one exists." $i $e.name) }}
{{- end }}
{{- end }}
{{- if and .Values.alerts.enabled (not .Values.alerts.secretName) }}
{{- fail "alerts.enabled=true requires alerts.secretName (a Secret holding the Resend API key and the from-address; see values.yaml)." }}
{{- end }}
{{- if and .Values.alerts.secretName (or (not .Values.alerts.resendApiKeyKey) (not .Values.alerts.fromEmailKey)) }}
{{- fail "alerts.secretName requires alerts.resendApiKeyKey and alerts.fromEmailKey." }}
{{- end }}
{{- if and .Values.heartbeat.secretName (not .Values.heartbeat.urlKey) }}
{{- fail "heartbeat.secretName requires heartbeat.urlKey." }}
{{- end }}
{{- if and .Values.dataGate.secretName (or (not .Values.dataGate.keyIdKey) (not .Values.dataGate.keySecretKey)) }}
{{- fail "dataGate.secretName requires dataGate.keyIdKey and dataGate.keySecretKey." }}
{{- end }}
{{- end }}
