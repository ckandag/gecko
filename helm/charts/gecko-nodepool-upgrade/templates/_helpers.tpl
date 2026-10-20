{{- define "gecko-nodepool-upgrade.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "gecko-nodepool-upgrade.fullname" -}}
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

{{- define "gecko-nodepool-upgrade.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "gecko-nodepool-upgrade.labels" -}}
helm.sh/chart: {{ include "gecko-nodepool-upgrade.chart" . }}
{{ include "gecko-nodepool-upgrade.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "gecko-nodepool-upgrade.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gecko-nodepool-upgrade.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "gecko-nodepool-upgrade.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "gecko-nodepool-upgrade.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "gecko-nodepool-upgrade.validateValues" -}}
{{- if not (trim (toString .Values.image.registry)) -}}
{{- fail "image.registry must be set" -}}
{{- end -}}
{{- if not (trim (toString .Values.image.repository)) -}}
{{- fail "image.repository must be set" -}}
{{- end -}}
{{- if not (trim (toString .Values.image.tag)) -}}
{{- fail "image.tag must be set" -}}
{{- end -}}
{{- end }}
