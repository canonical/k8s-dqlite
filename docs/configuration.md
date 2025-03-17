# K8s-dqlite Configuration

This document describes the configuration options available for k8s-dqlite.

## Configuration

The following configuration options are available listed in a table format:

| Option | Description | Default |
|--------|-------------|---------|
| `--storage-dir` | The directory to store the Dqlite data | `/var/tmp/k8s-dqlite/` |
| `--listen` | The endpoint where Dqlite should listen to | `tcp://127.0.0.1:12379` |
| `--enable-tls` | Enable TLS | `true` |
| `--debug` | Enable debug logs | `false` |
| `--profiling` | Enable debug pprof endpoint | `false` |
| `--profiling-dir` | Directory to use for profiling data | - |
| `--profiling-listen` | The address to listen for pprof endpoint | `127.0.0.1:4000` |
| `--disk-mode` | (Experimental) Run Dqlite store in disk mode | `false` |
| `--tls-client-session-cache-size` | ClientCacheSession size for dial TLS config | `0` |
| `--min-tls-version` | Minimum TLS version for Dqlite endpoint supported values: (tls10, tls11, tls12, tls13) | `tls12` |
| `--metrics` | Enable metrics endpoint | `false` |
| `--otel` | Enable traces endpoint | `false` |
| `--otel-listen` | The address to listen for OpenTelemetry endpoint | `127.0.0.1:4317` |
| `--otel-dir` | Dump OpenTelemetry metrics in the specified directory | - |
| `--otel-span-name-filter` |drop OpenTelemetry trace spans that do not match the specified regex filter | - |
| `--otel-span-min-duration-filter` | Drop OpenTelemetry trace spans below the specified time interval (e.g. 10ms) | - |
| `--metrics-listen` | The address to listen for metrics endpoint | `127.0.0.1:9042` |
| `--datastore-max-idle-connections` | Maximum number of idle connections retained by datastore | `5` |
| `--datastore-max-open-connections` | Maximum number of open connections used by datastore | `5` |
| `--datastore-connection-max-lifetime` | Maximum amount of time a connection may be reused | `60s` |
| `--datastore-connection-max-idle-time` | Maximum amount of time a connection may be idle before being closed | `0s` |
| `--watch-storage-available-size-interval` | Interval to check if the disk is running low on space | `5s` |
| `--watch-storage-available-size-min-bytes` | Minimum required available disk size (in bytes) to continue operation | `10*1024*1024`|
| `--low-available-storage-action` | Action to perform in case the available storage is low | `none` |
| ~~`--admission-control-policy`~~ | `REMOVED` | - |
| ~~`--admission-control-policy-limit`~~ | `REMOVED` | - |
| ~~`--admission-control-only-for-write-queries`~~ | `REMOVED` | - |
| `--watch-query-timeout` | Timeout for querying events in the watch poll loop | `20s` |
| `--watch-progress-notify-interval` | Interval between periodic watch progress notifications. Default is 5s to ensure support for watch progress notifications. | `5s` |

## Observability

The `metrics` endpoint allows you to view the metrics of the k8s-dqlite layer with [Prometheus](https://prometheus.io/).
With k8s-dqlite `v1.2.0` you will need to enable the `metrics` endpoint first before scraping the metrics.

Starting with k8s-dqlite `v1.2.0`, [Otel](https://opentelemetry.io/) can be used to gather insights on
traces on queries to Dqlite using a tool like Jaeger.
To gather insights on traces and metrics locally, run `docker-compose up` in the `./hack/otel` directory.
This sets up the Otel collector, Jaeger, and Prometheus. Navigate to `http://localhost:16686` to view the traces
in Jaeger and to `http://localhost:9090` to view the metrics in Prometheus.

## Connection Pool Configuration

The connection pool configuration options are available to control the connections to Dqlite:

- [SetMaxOpenConns](https://pkg.go.dev/database/sql#DB.SetMaxOpenConns)
- [SetMaxIdleConns](https://pkg.go.dev/database/sql#DB.SetMaxIdleConns)
- [SetConnMaxIdleTime](https://pkg.go.dev/database/sql#DB.SetConnMaxIdleTime)
- [SetConnMaxLifetime](https://pkg.go.dev/database/sql#DB.SetConnMaxLifetime)

We recommend allowing at least two maximum open connections to Dqlite. For larger clusters,
you may find it advantageous to increase the maximum open connections to Dqlite.

## Changing the Default Configuration

It is possible to change the default configuration of k8s-dqlite by editing the configuration file and restarting the service.

### MicroK8s

To change the default configuration in MicroK8s, you can edit the file
`/var/snap/microk8s/current/args/k8s-dqlite` and restart k8s-dqlite by running:

```
sudo snap restart microk8s.daemon-k8s-dqlite
```

### Canonical Kubernetes

For Canonical Kubernetes, you can edit `/var/snap/k8s/common/args/k8s-dqlite` and restart k8s-dqlite via:

```
sudo snap restart snap.k8s.k8s-dqlite
```
