{{/*
Expand the name of the chart.
*/}}
{{- define "objectio.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "objectio.fullname" -}}
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
Common labels.
*/}}
{{- define "objectio.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "objectio.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.global.imageTag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "objectio.selectorLabels" -}}
app.kubernetes.io/name: {{ include "objectio.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Gateway fullname.
*/}}
{{- define "objectio.gateway.fullname" -}}
{{- printf "%s-gateway" (include "objectio.fullname" .) }}
{{- end }}

{{/*
Meta fullname.
*/}}
{{- define "objectio.meta.fullname" -}}
{{- printf "%s-meta" (include "objectio.fullname" .) }}
{{- end }}

{{/*
Meta headless service name.
*/}}
{{- define "objectio.meta.headless" -}}
{{- printf "%s-meta-headless" (include "objectio.fullname" .) }}
{{- end }}

{{/*
OSD fullname.
*/}}
{{- define "objectio.osd.fullname" -}}
{{- printf "%s-osd" (include "objectio.fullname" .) }}
{{- end }}

{{/*
OSD headless service name.
*/}}
{{- define "objectio.osd.headless" -}}
{{- printf "%s-osd-headless" (include "objectio.fullname" .) }}
{{- end }}

{{/*
Block Gateway fullname.
*/}}
{{- define "objectio.blockGateway.fullname" -}}
{{- printf "%s-block-gateway" (include "objectio.fullname" .) }}
{{- end }}

{{/*
Prometheus fullname.
*/}}
{{- define "objectio.prometheus.fullname" -}}
{{- printf "%s-prometheus" (include "objectio.fullname" .) }}
{{- end }}

{{/*
Grafana fullname.
*/}}
{{- define "objectio.grafana.fullname" -}}
{{- printf "%s-grafana" (include "objectio.fullname" .) }}
{{- end }}

{{/*
Generate comma-separated meta peer list for Raft.
Format: meta-0.HEADLESS:9100,meta-1.HEADLESS:9100,...
*/}}
{{- define "objectio.metaPeers" -}}
{{- $headless := include "objectio.meta.headless" . }}
{{- $fullname := include "objectio.meta.fullname" . }}
{{- $port := .Values.meta.service.port | int }}
{{- $peers := list }}
{{- range $i := until (.Values.meta.replicas | int) }}
{{- $peers = append $peers (printf "%s-%d.%s:%d" $fullname $i $headless $port) }}
{{- end }}
{{- join "," $peers }}
{{- end }}
