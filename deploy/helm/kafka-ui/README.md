# kafka-ui Helm chart

Deploys the Kafka UI to OpenShift (works on plain Kubernetes with `route.enabled=false` and an Ingress).
Compatible with the `restricted-v2` SCC: no `runAsUser`/`fsGroup` is set, the pod runs `runAsNonRoot` with the
`RuntimeDefault` seccomp profile, privilege escalation is off and all capabilities are dropped.

## Install

```
helm install kafka-ui deploy/helm/kafka-ui \
  --set image.repository=image-registry.openshift-image-registry.svc:5000/<ns>/kafka-ui \
  --set kafka.bootstrapServers=my-cluster-kafka-bootstrap.kafka.svc:9093 \
  --set registry.url=https://apicurio-proxy.kafka.svc/apis/registry/v3 \
  --set tls.ca.existingSecret=ca-bundle --set tls.ca.key=ca-bundle.pem \
  --set oidc.authServerUrl=https://keycloak.example.com/realms/my-realm \
  --set oidc.clientId=kafka-ui \
  --set oidc.clientSecret.existingSecret=kafka-ui-oidc
```

Ready-made scenarios are in `deploy/helm/ci-values/` (oidc, mtls with existing secret, mtls with cert-manager,
ingress). The chart creates no secrets: CA bundle, OIDC secrets and (mtls) client certificate are referenced by name.

## Values

| Value | Default | Meaning |
|---|---|---|
| `mode` | `oidc` | `oidc` or `mtls` (mtls = test environments only, no login) |
| `image.repository` | **required** | no default registry |
| `image.tag` | chart `appVersion` | |
| `replicaCount` | `1` | `> 1` requires `oidc.stateSecret.existingSecret` |
| `kafka.bootstrapServers` | **required** | `KAFKA_UI_BOOTSTRAP_SERVERS` |
| `registry.url` | **required** | `KAFKA_UI_REGISTRY_URL` |
| `tls.ca.existingSecret` / `.key` | **required** / `ca.crt` | CA bundle, mounted as `/etc/kafka-ui/ca/ca.pem` |
| `tls.client.existingSecret` / `.certKey` / `.keyKey` | - / `tls.crt` / `tls.key` | mtls only; key must be PKCS#8 |
| `certManager.enabled` | `false` | mtls only: create a `Certificate` (PKCS8); `secretName` defaults to `<fullname>-client`; set `issuerRef.name` |
| `certManager.privateKey.algorithm` | `RSA` | `RSA`, `ECDSA` or `Ed25519` |
| `oidc.authServerUrl`, `oidc.clientId` | **required in oidc** | |
| `oidc.clientSecret.existingSecret` / `.key` | - / `client-secret` | `QUARKUS_OIDC_CREDENTIALS_SECRET` via `secretKeyRef` |
| `oidc.stateSecret.existingSecret` / `.key` | - / `state-secret` | `QUARKUS_OIDC_AUTHENTICATION_STATE_SECRET` (min 32 chars) |
| `config.search/tail/streams/produce.*` | unset | pass-through of the `KAFKA_UI_*` tuning variables, rendered only when set |
| `route.enabled` | `true` | Route with edge TLS, HTTP redirect and `haproxy.router.openshift.io/timeout: 5m` (SSE tails and searches) |
| `ingress.enabled` | `false` | generic alternative to the Route |
| `containerSecurityContext.readOnlyRootFilesystem` | `true` | read-only root with an emptyDir at `/tmp` (verified with the image) |
| `podDisruptionBudget.enabled`, `networkPolicy.enabled` | `false` | PDB defaults to `maxUnavailable: 1` |
| `extraEnv`, `extraEnvFrom`, `nodeSelector`, `tolerations`, `affinity`, `podAnnotations`, `podLabels`, `resources`, probes | | see `values.yaml` |

When a Route or Ingress is enabled, `QUARKUS_HTTP_PROXY_PROXY_ADDRESS_FORWARDING` and
`QUARKUS_HTTP_PROXY_ENABLE_FORWARDED_HOST` are set to `true` (the CSRF Origin check needs the forwarded host).
In oidc mode the client certificate is never mounted and `KAFKA_UI_TLS_CERT_FILE`/`KEY_FILE` are not set.
Secrets are mounted with `items` and no `subPath`, so certificate rotation (symlink swap) is picked up.

## Notes

- Secret rotation: OIDC secrets are injected as env vars, so rotating them needs a rollout
  (`oc rollout restart deploy/<name>`, or bump a `podAnnotations` checksum). Mounted certificates reload by themselves.
- Tunables not in the chart's `config` schema can be set with `extraEnv` (any `KAFKA_UI_*` or `QUARKUS_*` variable).

## Tests

```
deploy/helm/kafka-ui/tests/test.sh
```

Runs `helm lint` (also `--strict`) and renders every scenario, asserting on the output with `yq` (including the
rendering failures for missing required values). Needs `helm` and mikefarah `yq` v4 (not the Python yq) on the PATH.
