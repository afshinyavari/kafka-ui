{{- define "kafka-ui.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kafka-ui.fullname" -}}
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

{{- define "kafka-ui.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kafka-ui.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafka-ui.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "kafka-ui.labels" -}}
helm.sh/chart: {{ include "kafka-ui.chart" . }}
{{ include "kafka-ui.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "kafka-ui.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kafka-ui.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "kafka-ui.image" -}}
{{- $repo := required "image.repository is required (no default registry)" .Values.image.repository -}}
{{- printf "%s:%v" $repo (default .Chart.AppVersion (.Values.image.tag | toString | default "")) -}}
{{- end }}

{{- define "kafka-ui.isMtls" -}}
{{- if eq .Values.mode "mtls" }}true{{ end -}}
{{- end }}

{{- define "kafka-ui.exposed" -}}
{{- if or .Values.route.enabled .Values.ingress.enabled }}true{{ end -}}
{{- end }}

{{/* Name of the secret holding the client certificate (mtls mode). Fails when none is configured. */}}
{{- define "kafka-ui.clientSecretName" -}}
{{- if .Values.certManager.enabled -}}
{{- default (printf "%s-client" (include "kafka-ui.fullname" .)) .Values.certManager.secretName -}}
{{- else -}}
{{- required "mode=mtls needs a client certificate: set tls.client.existingSecret or certManager.enabled=true" .Values.tls.client.existingSecret -}}
{{- end -}}
{{- end }}

{{/* Validation shared by all templates; included once from the ConfigMap. */}}
{{- define "kafka-ui.validate" -}}
{{- if not (has .Values.mode (list "oidc" "mtls")) }}{{ fail "mode must be oidc or mtls" }}{{ end }}
{{- $_ := include "kafka-ui.image" . }}
{{- $_ := required "kafka.bootstrapServers is required" .Values.kafka.bootstrapServers }}
{{- $_ := required "registry.url is required" .Values.registry.url }}
{{- $_ := required "tls.ca.existingSecret is required (secret with the PEM CA bundle)" .Values.tls.ca.existingSecret }}
{{- if eq .Values.mode "mtls" }}
{{- $_ := include "kafka-ui.clientSecretName" . }}
{{- else }}
{{- $_ := required "oidc.authServerUrl is required in oidc mode" .Values.oidc.authServerUrl }}
{{- $_ := required "oidc.clientId is required in oidc mode" .Values.oidc.clientId }}
{{- if and (gt (int .Values.replicaCount) 1) (not .Values.oidc.stateSecret.existingSecret) }}
{{- fail "replicaCount > 1 requires oidc.stateSecret.existingSecret (min 32 chars), otherwise logins break when the callback hits another pod" }}
{{- end }}
{{- end }}
{{- end }}
