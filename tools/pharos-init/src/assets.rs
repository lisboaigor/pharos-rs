//! Infrastructure files shipped verbatim into a generated project.
//!
//! These are configuration, not code templates: roughly 1600 lines of YAML,
//! JSON and Alloy syntax that would be unreadable — and unlintable — squeezed
//! through `formatdoc!` with every brace escaped. They live as real files under
//! `assets/`, where an editor highlights them and a diff is legible, and are
//! embedded at compile time.
//!
//! # Why no substitution
//!
//! Every one of them is byte-identical for every project, which holds because
//! the application's compose service is **always named `app`**. That single
//! convention is what makes `job_name: app`, `{service="app"}` and the
//! dashboards' queries constants rather than templates.
//!
//! Anything that genuinely needs the project's name — the compose file, the
//! Dockerfile, the Grafana folder — is generated in `generator.rs` instead. The
//! split is deliberate: assets are the framework's to update, generated files
//! are the application's to edit.

/// One embedded file and where it lands in the project.
pub struct Asset {
    pub rel_path: &'static str,
    pub contents: &'static str,
}

/// The observability stack's configuration.
pub const OBSERVABILITY: &[Asset] = &[
    Asset {
        rel_path: "docker/prometheus/prometheus.yml",
        contents: include_str!("../assets/docker/prometheus/prometheus.yml"),
    },
    Asset {
        rel_path: "docker/loki/loki.yml",
        contents: include_str!("../assets/docker/loki/loki.yml"),
    },
    Asset {
        rel_path: "docker/tempo/tempo.yml",
        contents: include_str!("../assets/docker/tempo/tempo.yml"),
    },
    Asset {
        rel_path: "docker/alloy/config.alloy",
        contents: include_str!("../assets/docker/alloy/config.alloy"),
    },
    Asset {
        rel_path: "docker/telegraf/telegraf.conf",
        contents: include_str!("../assets/docker/telegraf/telegraf.conf"),
    },
    Asset {
        rel_path: "docker/grafana/provisioning/datasources/prometheus.yml",
        contents: include_str!("../assets/docker/grafana/provisioning/datasources/prometheus.yml"),
    },
    Asset {
        rel_path: "docker/grafana/provisioning/datasources/loki.yml",
        contents: include_str!("../assets/docker/grafana/provisioning/datasources/loki.yml"),
    },
    Asset {
        rel_path: "docker/grafana/provisioning/datasources/tempo.yml",
        contents: include_str!("../assets/docker/grafana/provisioning/datasources/tempo.yml"),
    },
    Asset {
        rel_path: "docker/grafana/dashboards/http.json",
        contents: include_str!("../assets/docker/grafana/dashboards/http.json"),
    },
    Asset {
        rel_path: "docker/grafana/dashboards/logs.json",
        contents: include_str!("../assets/docker/grafana/dashboards/logs.json"),
    },
];

/// The seven observability services, already indented to sit under `services:`.
///
/// Kept as a string rather than an asset file because the compose document is
/// assembled from parts that depend on the project's answers (which broker, which
/// database), and this block is the invariant one.
pub const COMPOSE_SERVICES: &str = r#"
  # Observability. None of these publish a port: they are reachable only from
  # inside the compose network, except Grafana, which is the way in.
  prometheus:
    image: prom/prometheus:v3.4.1
    # Overriding `command` discards the image's default, so its flags have to be
    # repeated here.
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus"
      - "--web.console.libraries=/usr/share/prometheus/console_libraries"
      - "--web.console.templates=/usr/share/prometheus/consoles"
      # Receives the RED metrics and service graph Tempo derives from spans.
      - "--web.enable-remote-write-receiver"
      # Keeps the exemplars that arrive with them; without it they are parsed
      # and thrown away, and clicking a latency spike leads nowhere.
      - "--enable-feature=exemplar-storage"
    volumes:
      - ./docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    restart: unless-stopped

  # Host CPU, memory, disk and network.
  node-exporter:
    image: prom/node-exporter:v1.9.1
    command: ["--path.rootfs=/host"]
    volumes:
      - /:/host:ro,rslave
    restart: unless-stopped

  # Per-container resource usage. `user: root` with the entrypoint called
  # directly: the image's default entrypoint drops privileges and loses access
  # to the Docker socket.
  telegraf:
    image: telegraf:1.35-alpine
    user: root
    entrypoint: ["telegraf"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./docker/telegraf/telegraf.conf:/etc/telegraf/telegraf.conf:ro
    restart: unless-stopped

  # Logs. Prometheus stores numeric series only; log text does not fit in it.
  loki:
    image: grafana/loki:3.7.6
    command: ["-config.file=/etc/loki/loki.yml"]
    volumes:
      - ./docker/loki/loki.yml:/etc/loki/loki.yml:ro
      - loki_data:/loki
    restart: unless-stopped

  # Reads every container's stdout through the Docker API and pushes it to Loki.
  alloy:
    image: grafana/alloy:v1.18.1
    user: root
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./docker/alloy/config.alloy:/etc/alloy/config.alloy:ro
      # Read positions per container. Without persisting them, a restart would
      # re-send every log from the beginning and duplicate everything in Loki.
      - alloy_data:/var/lib/alloy/data
    depends_on: [loki]
    restart: unless-stopped

  # Traces, over OTLP.
  tempo:
    image: grafana/tempo:2.9.4
    command: ["-config.file=/etc/tempo/tempo.yml"]
    volumes:
      - ./docker/tempo/tempo.yml:/etc/tempo/tempo.yml:ro
      - tempo_data:/var/tempo
    restart: unless-stopped

  grafana:
    image: grafana/grafana:11.6.1
    ports:
      - "3002:3000"
    environment:
      GF_SECURITY_ADMIN_USER: ${GRAFANA_ADMIN_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - ./docker/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./docker/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - grafana_data:/var/lib/grafana
    # Datasources are read once, at boot: without recreating Grafana, a newly
    # provisioned one never appears.
    depends_on: [prometheus, loki, tempo]
    restart: unless-stopped
"#;

/// Volumes the observability services need.
pub const COMPOSE_VOLUMES: &str = r#"  prometheus_data:
  grafana_data:
  loki_data:
  alloy_data:
  tempo_data:
"#;
