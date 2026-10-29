{{- define "gecko-replication.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "gecko-replication.serviceAccountName" -}}
{{- default (include "gecko-replication.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{- define "gecko-replication.gcpServiceAccount" -}}
{{- printf "%s@%s.iam.gserviceaccount.com" .Values.identity.accountId .Values.identity.projectId -}}
{{- end -}}

{{- define "gecko-replication.labels" -}}
app.kubernetes.io/name: gecko-replication
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "gecko-replication.validateValues" -}}
{{- if .Values.enabled -}}
{{- $_ := required "region is required" .Values.region -}}
{{- $_ := required "leaderRegion is required" .Values.leaderRegion -}}
{{- if lt (int .Values.leaderGeneration) 1 -}}{{- fail "leaderGeneration must be positive" -}}{{- end -}}
{{- if ne (int .Values.replicaCount) 1 -}}{{- fail "replicaCount must be 1 until durable per-key ordering is implemented" -}}{{- end -}}
{{- $_ := required "replicate is required" .Values.replicate -}}
{{- $_ := required "pubsub.projectId is required" .Values.pubsub.projectId -}}
{{- $_ := required "pubsub.dataTopic is required" .Values.pubsub.dataTopic -}}
{{- $_ := required "pubsub.controlTopic is required" .Values.pubsub.controlTopic -}}
{{- $_ := required "pubsub.dataSubscription is required" .Values.pubsub.dataSubscription -}}
{{- $_ := required "pubsub.controlSubscription is required" .Values.pubsub.controlSubscription -}}
{{- $_ := required "identity.projectId is required" .Values.identity.projectId -}}
{{- $_ := required "identity.accountId is required" .Values.identity.accountId -}}
{{- if lt (len .Values.identity.accountId) 6 -}}{{- fail "identity.accountId must be at least 6 characters" -}}{{- end -}}
{{- if gt (len .Values.identity.accountId) 30 -}}{{- fail "identity.accountId must be at most 30 characters" -}}{{- end -}}
{{- $_ := required "image.registry is required" .Values.image.registry -}}
{{- $_ := required "image.repository is required" .Values.image.repository -}}
{{- $_ := required "image.tag is required" .Values.image.tag -}}
{{- end -}}
{{- end -}}
