{{/*
Expand the name of the chart.
*/}}
{{- define "gecko-hc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "gecko-hc.fullname" -}}
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
{{- define "gecko-hc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "gecko-hc.labels" -}}
helm.sh/chart: {{ include "gecko-hc.chart" . }}
{{ include "gecko-hc.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "gecko-hc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gecko-hc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "gecko-hc.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "gecko-hc.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Validate required values.
*/}}
{{- define "gecko-hc.validateValues" -}}
{{- $registry := trim (toString .Values.image.registry) -}}
{{- if or (not $registry) (eq $registry "CHANGE_ME") -}}
{{- fail "image.registry must be set (e.g. --set image.registry=us-docker.pkg.dev)" -}}
{{- end -}}
{{- $repository := trim (toString .Values.image.repository) -}}
{{- if or (not $repository) (eq $repository "CHANGE_ME") -}}
{{- fail "image.repository must be set (e.g. --set image.repository=gcp-hcp-commons/gcp-hcp-images/gecko-controllers)" -}}
{{- end -}}
{{- if not (trim (toString .Values.image.tag)) -}}
{{- fail "image.tag must be set (e.g. --set image.tag=abc1234)" -}}
{{- end -}}
{{- end }}
