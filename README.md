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
| `KAFKA_UI_TLS_CERT_FILE` | none | client certificate, PEM (required in `mtls` mode) |
| `KAFKA_UI_TLS_KEY_FILE` | none | client key, PEM, must be **PKCS#8** (`BEGIN PRIVATE KEY`) (required in `mtls` mode) |
| `KAFKA_UI_SEARCH_MAX_SCANNED` | `100000` | max messages scanned per search |
| `KAFKA_UI_SEARCH_MAX_MATCHES` | `500` | max hits per search |
| `KAFKA_UI_SEARCH_REGEX_TIMEOUT` | `100ms` | time limit per regexp match (protects against catastrophic backtracking) |
| `KAFKA_UI_SEARCH_DEFAULT_RANGE` | `1h` | default time range in the search form |
| `KAFKA_UI_TAIL_MAX_ROWS` | `1000` | max rows kept in the tail view |
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

## Kubernetes

Manifests are in `deploy/kubernetes` (Deployment, Service, ConfigMap, kustomization):

```
kubectl apply -k deploy/kubernetes
```

- Certificates come from Kubernetes secrets mounted as volumes (no Kubernetes client in the app). The Deployment maps secret fields to fixed paths with `items`, so secret names and field names are configurable: edit `secretName` and `items[].key` (defaults: `kafka-ui-client-cert` with `tls.crt`/`tls.key`, `ca-bundle` with `ca-bundle.pem`). The ConfigMap's `KAFKA_UI_TLS_*_FILE` values point at the fixed paths under `/etc/kafka-ui/client` and `/etc/kafka-ui/ca`.
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

## Known limitations

- **Avro produce:** schemas containing `fixed` fields cannot be produced (messages with such fields still decode fine).
- No Protobuf, no Apicurio v2, no topic/ACL/schema administration.
