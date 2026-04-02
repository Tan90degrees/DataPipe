package logging

import (
	"bytes"
	"fmt"
	"text/template"
)

type FilebeatConfig struct {
	Inputs    []FilebeatInput
	OutputLogstash HostConfig
	OutputElasticsearch HostConfig
	Processors []map[string]interface{}
}

type FilebeatInput struct {
	Type        string
	Paths       []string
	Fields      map[string]interface{}
	FieldsUnderRoot bool
	ScanFrequency string
	CloseTimeout string
	CleanInactive string
	IgnoreOlder string
	HarvesterBufferSize int
	MaxBytes int
	Backoff string
	MaxBackoff string
	SourceProcessor string
}

type HostConfig struct {
	Hosts    []string
	Host     string
	Port     int
	Username string
	Password string
	Protocol string
	Index    string
}

func NewFilebeatConfig() *FilebeatConfig {
	return &FilebeatConfig{
		Inputs:    make([]FilebeatInput, 0),
		Processors: make([]map[string]interface{}, 0),
	}
}

func (c *FilebeatConfig) AddInput(input FilebeatInput) {
	c.Inputs = append(c.Inputs, input)
}

func (c *FilebeatConfig) SetLogstashOutput(hosts []string) {
	c.OutputLogstash = HostConfig{
		Hosts: hosts,
	}
}

func (c *FilebeatConfig) SetElasticsearchOutput(hosts []string, index string) {
	c.OutputElasticsearch = HostConfig{
		Hosts:    hosts,
		Protocol: "http",
		Index:    index,
	}
}

func (c *FilebeatConfig) AddProcessor(name string, params map[string]interface{}) {
	c.Processors = append(c.Processors, map[string]interface{}{
		name: params,
	})
}

func (c *FilebeatConfig) Generate() (string, error) {
	tmpl := `filebeat.inputs:
{{- range .Inputs }}
- type: {{ .Type }}
  paths:
    {{- range .Paths }}
    - {{ . }}
    {{- end }}
  fields:
    {{- range $k, $v := .Fields }}
    {{ $k }}: {{ $v }}
    {{- end }}
  fields_under_root: {{ .FieldsUnderRoot }}
  scan_frequency: {{ .ScanFrequency }}
  close_timeout: {{ .CloseTimeout }}
  clean_inactive: {{ .CleanInactive }}
  ignore_older: {{ .IgnoreOlder }}
  harvester_buffer_size: {{ .HarvesterBufferSize }}
  max_bytes: {{ .MaxBytes }}
  backoff: {{ .Backoff }}
  max_backoff: {{ .MaxBackoff }}
{{- end }}

{{- if .OutputLogstash.Hosts }}
output.logstash:
  hosts:
    {{- range .OutputLogstash.Hosts }}
    - {{ . }}
    {{- end }}
{{- end }}

{{- if .OutputElasticsearch.Hosts }}
output.elasticsearch:
  hosts:
    {{- range .OutputElasticsearch.Hosts }}
    - {{ . }}
    {{- end }}
  index: "{{ .OutputElasticsearch.Index }}"
{{- end }}

{{- if .Processors }}
processors:
  {{- range .Processors }}
  {{- range $k, $v := . }}
  - {{ $k }}:
    {{- range $pk, $pv := $v }}
    {{ $pk }}: {{ $pv }}
    {{- end }}
  {{- end }}
  {{- end }}
{{- end }}

logging.level: info
logging.to_files: true
logging.files:
  path: /var/log/filebeat
  name: filebeat
  keepfiles: 7
  permissions: 0640
`

	var buf bytes.Buffer
	t, err := template.New("filebeat").Parse(tmpl)
	if err != nil {
		return "", err
	}

	if err := t.Execute(&buf, c); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func GenerateDataPipeFilebeatConfig(serviceType string) (*FilebeatConfig, error) {
	config := NewFilebeatConfig()

	switch serviceType {
	case "master":
		config.AddInput(FilebeatInput{
			Type:             "log",
			Paths:            []string{"/var/log/datapipe/master/*.log"},
			Fields:           map[string]interface{}{"service": "master", "datapipe": "true"},
			FieldsUnderRoot:  true,
			ScanFrequency:    "10s",
			CloseTimeout:     "5m",
			CleanInactive:    "24h",
			IgnoreOlder:      "12h",
			HarvesterBufferSize: 16384,
			MaxBytes:         10485760,
			Backoff:          "1s",
			MaxBackoff:       "10s",
		})
		config.SetElasticsearchOutput([]string{"elasticsearch:9200"}, "datapipe-master-%{+yyyy.MM.dd}")

	case "worker":
		config.AddInput(FilebeatInput{
			Type:             "log",
			Paths:            []string{"/var/log/datapipe/worker/*.log"},
			Fields:           map[string]interface{}{"service": "worker", "datapipe": "true"},
			FieldsUnderRoot:  true,
			ScanFrequency:    "10s",
			CloseTimeout:     "5m",
			CleanInactive:    "24h",
			IgnoreOlder:      "12h",
			HarvesterBufferSize: 16384,
			MaxBytes:         10485760,
			Backoff:          "1s",
			MaxBackoff:       "10s",
		})
		config.SetElasticsearchOutput([]string{"elasticsearch:9200"}, "datapipe-worker-%{+yyyy.MM.dd}")

	case "api":
		config.AddInput(FilebeatInput{
			Type:             "log",
			Paths:            []string{"/var/log/datapipe/api/*.log"},
			Fields:           map[string]interface{}{"service": "api", "datapipe": "true"},
			FieldsUnderRoot:  true,
			ScanFrequency:    "10s",
			CloseTimeout:     "5m",
			CleanInactive:    "24h",
			IgnoreOlder:      "12h",
			HarvesterBufferSize: 16384,
			MaxBytes:         10485760,
			Backoff:          "1s",
			MaxBackoff:       "10s",
		})
		config.SetElasticsearchOutput([]string{"elasticsearch:9200"}, "datapipe-api-%{+yyyy.MM.dd}")

	default:
		config.AddInput(FilebeatInput{
			Type:             "log",
			Paths:            []string{"/var/log/datapipe/*.log"},
			Fields:           map[string]interface{}{"service": "datapipe", "datapipe": "true"},
			FieldsUnderRoot:  true,
			ScanFrequency:    "10s",
			CloseTimeout:     "5m",
			CleanInactive:    "24h",
			IgnoreOlder:      "12h",
			HarvesterBufferSize: 16384,
			MaxBytes:         10485760,
			Backoff:          "1s",
			MaxBackoff:       "10s",
		})
		config.SetElasticsearchOutput([]string{"elasticsearch:9200"}, "datapipe-%{+yyyy.MM.dd}")
	}

	config.AddProcessor("add_host_metadata", map[string]interface{}{})
	config.AddProcessor("add_docker_metadata", map[string]interface{}{"host": "unix:///var/run/docker.sock"})
	config.AddProcessor("add_fields", map[string]interface{}{
		"fields": map[string]interface{}{
			"environment": "production",
			"cluster":     "datapipe",
		},
	})

	return config, nil
}

func GenerateMasterFilebeatConfig() (string, error) {
	config, err := GenerateDataPipeFilebeatConfig("master")
	if err != nil {
		return "", err
	}
	return config.Generate()
}

func GenerateWorkerFilebeatConfig() (string, error) {
	config, err := GenerateDataPipeFilebeatConfig("worker")
	if err != nil {
		return "", err
	}
	return config.Generate()
}

func GenerateAPIServerFilebeatConfig() (string, error) {
	config, err := GenerateDataPipeFilebeatConfig("api")
	if err != nil {
		return "", err
	}
	return config.Generate()
}

func GenerateAllInOneFilebeatConfig() (string, error) {
	config, err := GenerateDataPipeFilebeatConfig("")
	if err != nil {
		return "", err
	}
	return config.Generate()
}

type FilebeatDockerConfig struct {
	Version string
	Services []DockerServiceConfig
}

type DockerServiceConfig struct {
	Name   string
	Type   string
	LogsPath string
}

func GenerateDockerComposeFilebeatConfig() string {
	return `version: '3.8'

services:
  filebeat-master:
    image: docker.elastic.co/beats/filebeat:8.11.0
    user: root
    volumes:
      - ./master-logs:/var/log/datapipe/master:ro
      - ./filebeat-master.yml:/usr/share/filebeat/filebeat.yml:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks:
      - datapipe
    depends_on:
      - elasticsearch
    restart: unless-stopped

  filebeat-worker:
    image: docker.elastic.co/beats/filebeat:8.11.0
    user: root
    volumes:
      - ./worker-logs:/var/log/datapipe/worker:ro
      - ./filebeat-worker.yml:/usr/share/filebeat/filebeat.yml:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks:
      - datapipe
    depends_on:
      - elasticsearch
    restart: unless-stopped

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    volumes:
      - es-data:/usr/share/elasticsearch/data
    networks:
      - datapipe
    restart: unless-stopped

  kibana:
    image: docker.elastic.co/kibana/kibana:8.11.0
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
    ports:
      - "5601:5601"
    networks:
      - datapipe
    depends_on:
      - elasticsearch
    restart: unless-stopped

volumes:
  es-data:

networks:
  datapipe:
    driver: bridge
`
}

func GenerateFilebeatKubernetesConfig() string {
	return `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: filebeat-config
  namespace: datapipe
data:
  filebeat.yml: |
    filebeat.inputs:
    - type: log
      paths:
        - /var/log/datapipe/*.log
      fields:
        service: datapipe
        datapipe: "true"
      fields_under_root: true
      scan_frequency: 10s
      close_timeout: 5m
      clean_inactive: 24h
      ignore_older: 12h

    output.elasticsearch:
      hosts: ["elasticsearch.datapipe.svc.cluster.local:9200"]
      index: "datapipe-%{+yyyy.MM.dd}"

    processors:
      - add_host_metadata:
          fields:
            cluster: datapipe
            environment: kubernetes
      - add_kubernetes_metadata:
          host: ${NODE_NAME}
          matchers:
            - logs_path:
                logs_path: "/var/log/datapipe/"
      - add_docker_metadata:
          host: "unix:///var/run/docker.sock"

---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: filebeat
  namespace: datapipe
spec:
  selector:
    matchLabels:
      app: filebeat
  template:
    metadata:
      labels:
        app: filebeat
    spec:
      serviceAccountName: filebeat
      terminationGracePeriodSeconds: 30
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      containers:
      - name: filebeat
        image: docker.elastic.co/beats/filebeat:8.11.0
        args:
          - "-c"
          - /usr/share/filebeat/filebeat.yml
          - "-e"
        env:
          - name: NODE_NAME
            valueFrom:
              fieldRef:
                fieldPath: spec.nodeName
        securityContext:
          runAsUser: 0
        resources:
          limits:
            memory: 200Mi
          requests:
            cpu: 100m
            memory: 100Mi
        volumeMounts:
        - name: datapipe-logs
          mountPath: /var/log/datapipe
          readOnly: true
        - name: filebeat-config
          mountPath: /usr/share/filebeat/filebeat.yml
          readOnly: true
          subPath: filebeat.yml
        - name: dockersock
          mountPath: /var/run/docker.sock
      volumes:
      - name: datapipe-logs
        hostPath:
          path: /var/log/datapipe
      - name: filebeat-config
        configMap:
          name: filebeat-config
      - name: dockersock
        hostPath:
          path: /var/run/docker.sock
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: filebeat
  namespace: datapipe
`
}

func GenerateFilebeatIndexTemplate() string {
	return `{
  "index_patterns": ["datapipe-*"],
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "index.lifecycle.name": "datapipe-logs-policy",
    "index.lifecycle.rollover_alias": "datapipe"
  },
  "mappings": {
    "properties": {
      "timestamp": {
        "type": "date"
      },
      "level": {
        "type": "keyword"
      },
      "service": {
        "type": "keyword"
      },
      "task_id": {
        "type": "keyword"
      },
      "pipeline_id": {
        "type": "keyword"
      },
      "node_id": {
        "type": "keyword"
      },
      "message": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "caller": {
        "type": "keyword"
      },
      "context": {
        "type": "object",
        "enabled": false
      },
      "error": {
        "type": "text"
      },
      "duration_ms": {
        "type": "float"
      }
    }
  }
}
`
}

func GenerateILMPolicy() string {
	return `{
  "policy": {
    "phases": {
      "hot": {
        "min_age": "0ms",
        "actions": {
          "rollover": {
            "max_age": "1d",
            "max_size": "50gb"
          },
          "set_priority": 100
        }
      },
      "warm": {
        "min_age": "7d",
        "actions": {
          "shrink": {
            "number_of_shards": 1
          },
          "forcemerge": {
            "max_num_segments": 1
          },
          "set_priority": 50
        }
      },
      "delete": {
        "min_age": "30d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}
`
}

type ElasticsearchIndexTemplate struct {
	Name       string
	IndexPattern string
	Shards     int
	Replicas   int
	ILMPolicy  string
}

func NewElasticsearchIndexTemplate(name string) *ElasticsearchIndexTemplate {
	return &ElasticsearchIndexTemplate{
		Name:         name,
		IndexPattern: fmt.Sprintf("%s-*", name),
		Shards:       1,
		Replicas:     1,
		ILMPolicy:    "datapipe-logs-policy",
	}
}

func (t *ElasticsearchIndexTemplate) Generate() (string, error) {
	templateStr := fmt.Sprintf(`{
  "index_patterns": ["%s"],
  "settings": {
    "number_of_shards": %d,
    "number_of_replicas": %d,
    "index.lifecycle.name": "%s"
  },
  "mappings": %s
}`, t.IndexPattern, t.Shards, t.Replicas, t.ILMPolicy, GenerateFilebeatIndexTemplate())

	return templateStr, nil
}
