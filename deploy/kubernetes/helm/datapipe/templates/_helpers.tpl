{{/*
Expand the name of the chart.
*/}}
{{- define "datapipe.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "datapipe.fullname" -}}
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
{{- define "datapipe.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "datapipe.labels" -}}
helm.sh/chart: {{ include "datapipe.chart" . }}
{{ include "datapipe.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "datapipe.selectorLabels" -}}
app.kubernetes.io/name: {{ include "datapipe.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "datapipe.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "datapipe.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Master image full path
*/}}
{{- define "datapipe.master.image" -}}
{{- if .Values.master.image.repository }}
{{- printf "%s/%s:%s" .Values.registry .Values.master.image.repository .Values.master.image.tag }}
{{- else }}
{{- printf "%s/%s-master:%s" .Values.registry .Values.repository .Values.master.image.tag }}
{{- end }}
{{- end }}

{{/*
Worker image full path
*/}}
{{- define "datapipe.worker.image" -}}
{{- if .Values.worker.image.repository }}
{{- printf "%s/%s:%s" .Values.registry .Values.worker.image.repository .Values.worker.image.tag }}
{{- else }}
{{- printf "%s/%s-worker:%s" .Values.registry .Values.repository .Values.worker.image.tag }}
{{- end }}
{{- end }}

{{/*
Database connection string
*/}}
{{- define "datapipe.database.url" -}}
{{- printf "postgresql://%s:%s@%s:%d/%s?sslmode=%s" .Values.database.user .Values.database.password .Values.database.host (int .Values.database.port) .Values.database.name .Values.database.sslmode }}
{{- end }}

{{/*
Redis connection string
*/}}
{{- define "datapipe.redis.url" -}}
{{- $password := .Values.redis.password | default "" -}}
{{- if $password }}
{{- printf "redis://:%s@%s:%d/%d" $password .Values.redis.host (int .Values.redis.port) .Values.redis.db }}
{{- else }}
{{- printf "redis://%s:%d/%d" .Values.redis.host (int .Values.redis.port) .Values.redis.db }}
{{- end }}
{{- end }}

{{/*
Create the master fully qualified name
*/}}
{{- define "datapipe.master.fullname" -}}
{{- printf "%s-master" (include "datapipe.fullname" .) }}
{{- end }}

{{/*
Create the worker fully qualified name
*/}}
{{- define "datapipe.worker.fullname" -}}
{{- printf "%s-worker" (include "datapipe.fullname" .) }}
{{- end }}
