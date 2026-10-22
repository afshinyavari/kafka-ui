# Kafka UI (Quarkus + htmx)

A stand-alone web UI for a Strimzi-based Kafka cluster, in the style of Kafbat/kafka-ui, but with **no permission model of its own**: what a user may do is decided by Kafka (Keycloak Authorization Services via Strimzi OAuth). One installation serves one cluster.

Features: cluster overview, topics (partitions, config, consumers), message browsing with live tail and regexp search on key/value within a time range, automatic deserialization (Apicurio headers, magic byte, Avro, JSON Schema, JSON, XML, text, hex), consumer groups, Kafka ACLs, Apicurio Registry 3 schemas (read-only), produce, and reset offsets. Produce and reset offsets are the only write operations.

## Authentication modes

Set with `KAFKA_UI_MODE`.

### `oidc` (default, for production)
The user logs in against Keycloak (authorization code flow with PKCE). The user's access token is passed unchanged to Kafka (SASL OAUTHBEARER) and to the Apicurio proxy (Bearer); there is no token exchange. Tokens are renewed with the refresh token.

Required settings:

| Variable | Meaning |
|---|---|
| `QUARKUS_OIDC_AUTH_SERVER_URL` | Keycloak realm URL (the issuer) |
| `QUARKUS_OIDC_CLIENT_ID` | OIDC client of the UI |
| `QUARKUS_OIDC_CREDENTIALS_SECRET` | client secret |
| `QUARKUS_OIDC_AUTHENTICATION_STATE_SECRET` | **required with more than one replica** (min 32 chars); otherwise every pod generates its own random secret and logins break when the callback hits another pod |

Keycloak requirements:
- The UI client and Kafka must be in the **same realm/issuer**.
- If the Kafka listener has `checkAudience=true`, add an **audience mapper** to the UI client that puts the Kafka client in `aud`.
- If the listener uses `userNameClaim` / `customClaimCheck`, the UI client's token must contain those claims.
- The Apicurio proxy's principal/role claim (`PRINCIPAL_CLAIM`) must be present in the UI client's token (add a mapper).

Schema lookups are cached per user in this mode, since the proxy decides read access per artifact and role.

### `mtls` (test environments only)
**No login.** Everyone who can reach the UI gets the rights of the UI's own client certificate, including produce and reset offsets. The UI shows a banner saying so. The same certificate is used towards the Apicurio proxy. Do not use this in production.

Any value other than `mtls` is treated as `oidc` (fails closed).

## Configuration

Quarkus maps `kafka-ui.some-name` to `KAFKA_UI_SOME_NAME`.

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_UI_MODE` | `oidc` | `oidc` or `mtls` |
| `KAFKA_UI_BOOTSTRAP_SERVERS` | (required) | Kafka bootstrap servers |
| `KAFKA_UI_REGISTRY_URL` | (required) | Apicurio Registry v3 API URL (the proxy), e.g. `https://proxy/apis/registry/v3` |
| `KAFKA_UI_TLS_CA_FILE` | (required) | PEM CA bundle (may contain several certificates); trust for Kafka and the registry proxy |
| `KAFKA_UI_TLS_CERT_FILE` | none | client certificate, PEM. `mtls` mode only: required there (startup fails without it), ignored in `oidc` mode |
| `KAFKA_UI_TLS_KEY_FILE` | none | client key, PEM, must be **PKCS#8** (`BEGIN PRIVATE KEY`). `mtls` mode only, like the certificate |
| `KAFKA_UI_SEARCH_MAX_SCANNED` | `100000` | max messages scanned per search |
| `KAFKA_UI_SEARCH_MAX_MATCHES` | `500` | max hits per search |
| `KAFKA_UI_SEARCH_REGEX_TIMEOUT` | `100ms` | time limit per regexp match (protects against catastrophic backtracking) |
| `KAFKA_UI_SEARCH_DEFAULT_RANGE` | `1h` | default time range in the search form |
| `KAFKA_UI_TAIL_MAX_ROWS` | `1000` | max rows kept in the tail view |
| `KAFKA_UI_STREAMS_MAX_CONCURRENT` | `32` | max searches + live tails running at once (dedicated thread pool) |
| `KAFKA_UI_STREAMS_MAX_PER_USER` | `3` | max searches + live tails per user at once (`oidc` mode only; in `mtls` mode only `MAX_CONCURRENT` applies) |
| `KAFKA_UI_PRODUCE_ID_LOCATION` | `PAYLOAD` | where produced Avro records carry the schema id: `PAYLOAD` (magic byte + id) or `HEADERS` |
| `KAFKA_UI_PRODUCE_ID_KIND` | `CONTENT_ID` | `CONTENT_ID` or `GLOBAL_ID` |
| `KAFKA_UI_PRODUCE_VALUE_HEADER_PREFIX` | `apicurio.value.` | header name prefix for the value schema id (when `HEADERS`) |
| `KAFKA_UI_PRODUCE_KEY_HEADER_PREFIX` | `apicurio.key.` | header name prefix for the key schema id |

The defaults match Apicurio 3's serializer defaults. Reading tries all variants regardless of these settings.

Certificate files are reloaded when they change (cert-manager rotation); Kafka clients are recreated.

Other useful Quarkus settings:

| Variable | Meaning |
|---|---|
| `QUARKUS_HTTP_PROXY_PROXY_ADDRESS_FORWARDING=true` and `QUARKUS_HTTP_PROXY_ENABLE_FORWARDED_HOST=true` | **Needed behind a TLS-terminating proxy/ingress** (or make the proxy preserve `Host`). Otherwise the CSRF Origin check rejects POSTs. |
| `QUARKUS_HTTP_PORT` | HTTP port (default 8080) |

### CSRF
Writes (produce, reset offsets) are only accepted as htmx requests (`HX-Request`) from the same origin (`Origin` must match the request's host). Plain cross-site form posts are rejected.

## OpenShift / Kubernetes

The recommended way to deploy (OpenShift Route, restricted-v2 SCC, optional cert-manager client certificate) is the
Helm chart in `deploy/helm/kafka-ui`, see its [README](deploy/helm/kafka-ui/README.md):

```
helm install kafka-ui deploy/helm/kafka-ui -f my-values.yaml   # scenario examples: deploy/helm/ci-values/
```

### Plain manifests

Alternatively, plain manifests are in `deploy/kubernetes` (Deployment, Service, ConfigMap, kustomization):

```
kubectl apply -k deploy/kubernetes
```

- Certificates come from Kubernetes secrets mounted as volumes (no Kubernetes client in the app). The Deployment maps secret fields to fixed paths with `items`, so secret names and field names are configurable: edit `secretName` and `items[].key` (defaults: `kafka-ui-client-cert` with `tls.crt`/`tls.key`, `ca-bundle` with `ca-bundle.pem`). The ConfigMap's `KAFKA_UI_TLS_*_FILE` values point at the fixed paths under `/etc/kafka-ui/client` and `/etc/kafka-ui/ca`. `KAFKA_UI_TLS_CERT_FILE` / `KAFKA_UI_TLS_KEY_FILE` are commented out: they are only used in `mtls` mode (uncomment them when you set `KAFKA_UI_MODE: mtls`); in `oidc` mode the UI never presents a client certificate.
- Probes use `/q/health/live` and `/q/health/ready` (the app includes `quarkus-smallrye-health`; `/q/health/*` is public).
- Create secret `kafka-ui-oidc` with `client-secret` (and `state-secret` if you run several replicas).

## Build and run

```
./mvnw quarkus:dev                 # dev profile defaults: Kafka on localhost:9092, registry on localhost:8081
./mvnw test                        # fast unit/component tests
./mvnw verify                      # also integration tests; needs Docker (Apicurio 3 + Kafka containers)
./mvnw package
docker build -f src/main/docker/Dockerfile.jvm -t kafka-ui .
```

### Styles (Tailwind CSS)

`/css/app.css` is generated, not checked in. Its source is `src/main/tailwind/app.css` (Tailwind CSS v4: theme
colours as `@theme` variables, shared component classes) and Tailwind picks up the classes used in
`src/main/resources/templates`. The build needs no Node: in `generate-resources` Maven downloads the pinned
Tailwind standalone CLI (`tailwind.version` in `pom.xml`, checksum-verified, one OS profile each for linux-x64,
linux-arm64, macos-x64 and macos-arm64) and writes the minified result to
`target/classes/META-INF/resources/css/app.css`. The binary is cached in
`~/.m2/repository/.cache/download-maven-plugin`, so builds work offline after the first one.

In dev mode, rebuild the CSS on template changes by running the CLI in watch mode next to `./mvnw quarkus:dev`
(after one `./mvnw generate-resources` has fetched it):

```
target/tailwind/tailwindcss-4.3.3 --input src/main/tailwind/app.css \
  --output target/classes/META-INF/resources/css/app.css --watch
```

Icons are inline SVGs (Lucide shapes) in `src/main/resources/templates/icons/`, included with
`{#include icons/<name> /}`; there are no runtime CDNs.

### Container image pipeline

`.github/workflows/image.yml` runs `./mvnw verify` and builds the UBI 9 image (`linux/amd64` and
`linux/arm64`) to `docker.io/alfeenz/kafka-ui` (`IMAGE` in the workflow):

| Trigger | Tags pushed |
|---------|-------------|
| push to `main` | `main`, `sha-<short>` |
| tag `v1.2.3` | `1.2.3`, `latest`, `sha-<short>` |
| pull request | none (build and test only) |

Releasing: set `appVersion` in `deploy/helm/kafka-ui/Chart.yaml` to `1.2.3`, then push tag `v1.2.3`. The pom stays
on the next `-SNAPSHOT`; the tag build sets the Maven version to `1.2.3` before building.

Setup in the GitHub repository (Settings → Secrets and variables → Actions): secret `DOCKERHUB_TOKEN`, a Docker
Hub access token with Read & Write (`gh secret set DOCKERHUB_TOKEN`).

## Known limitations

- **Avro produce:** schemas containing `fixed` fields cannot be produced (messages with such fields still decode fine).
- No Protobuf, no Apicurio v2, no topic/ACL/schema administration.
