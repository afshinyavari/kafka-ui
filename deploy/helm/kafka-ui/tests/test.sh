#!/usr/bin/env bash
# Offline unit tests for the chart: renders each scenario with `helm template` and asserts with yq (mikefarah v4).
# Usage: deploy/helm/kafka-ui/tests/test.sh      (needs helm and yq on PATH)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CHART="$HERE/.."
CI="$CHART/../ci-values"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT
PASS=0; FAILED=0

ok()   { PASS=$((PASS+1)); echo "  ok   $1"; }
bad()  { FAILED=$((FAILED+1)); echo "  FAIL $1"; [ -n "${2:-}" ] && echo "       $2"; }

# render <name> <values-file> [extra helm args...] -> $OUT/<name>.yaml
render() {
  local name="$1" values="$2"; shift 2
  helm template kafka-ui "$CHART" -f "$values" "$@" >"$OUT/$name.yaml" 2>"$OUT/$name.err" || { bad "render $name" "$(cat "$OUT/$name.err")"; return 1; }
}
# q <name> <kind> <yq-expr>: evaluate expr on the document of the given kind
q() { yq "select(.kind == \"$2\") | $3" "$OUT/$1.yaml"; }
eq()   { # eq <desc> <name> <kind> <expr> <expected>
  local got; got="$(q "$2" "$3" "$4" | grep -v '^---$' | sed '/^$/d')"
  [ "$got" == "$5" ] && ok "$1" || bad "$1" "expected [$5], got [$got]"
}
has()  { grep -qE -- "$3" "$OUT/$2.yaml" && ok "$1" || bad "$1" "pattern not found: $3"; }
hasnt(){ grep -qE -- "$3" "$OUT/$2.yaml" && bad "$1" "unexpected pattern: $3" || ok "$1"; }
kinds(){ yq '.kind' "$OUT/$1.yaml" | grep -v '^---$' | sort | tr '\n' ' '; }
# fails <desc> <expected message regex> <helm args...>
fails() {
  local desc="$1" msg="$2"; shift 2
  local err; err="$(helm template kafka-ui "$CHART" "$@" 2>&1 >/dev/null)"
  if [ $? -eq 0 ] && [ -z "$err" ]; then bad "$desc" "rendering unexpectedly succeeded"
  elif echo "$err" | grep -qE -- "$msg"; then ok "$desc"
  else bad "$desc" "message [$msg] not in: $err"; fi
}

echo "== helm lint (also --strict)"
for f in "$CI"/*.yaml; do
  helm lint "$CHART" -f "$f" >"$OUT/lint.out" 2>&1 && ok "lint $(basename "$f")" || bad "lint $(basename "$f")" "$(cat "$OUT/lint.out")"
  helm lint --strict "$CHART" -f "$f" >"$OUT/lint.out" 2>&1 && ok "lint --strict $(basename "$f")" || bad "lint --strict $(basename "$f")" "$(cat "$OUT/lint.out")"
done

echo "== oidc (default mode, route)"
render oidc "$CI/oidc.yaml" && {
  hasnt "no KAFKA_UI_TLS_CERT_FILE" oidc 'KAFKA_UI_TLS_CERT_FILE'
  hasnt "no KAFKA_UI_TLS_KEY_FILE" oidc 'KAFKA_UI_TLS_KEY_FILE'
  hasnt "no client-cert volume" oidc 'client-cert'
  eq "mode oidc" oidc ConfigMap '.data.KAFKA_UI_MODE' "oidc"
  eq "CA file path" oidc ConfigMap '.data.KAFKA_UI_TLS_CA_FILE' "/etc/kafka-ui/ca/ca.pem"
  eq "CA secret item key" oidc Deployment '.spec.template.spec.volumes[0].secret.items[0].key' "ca-bundle.pem"
  eq "bootstrap servers" oidc ConfigMap '.data.KAFKA_UI_BOOTSTRAP_SERVERS' "my-cluster-kafka-bootstrap.kafka.svc:9093"
  eq "oidc client id" oidc ConfigMap '.data.QUARKUS_OIDC_CLIENT_ID' "kafka-ui"
  eq "oidc client secret from secretKeyRef" oidc Deployment '.spec.template.spec.containers[0].env[] | select(.name=="QUARKUS_OIDC_CREDENTIALS_SECRET") | .valueFrom.secretKeyRef.name + "/" + .valueFrom.secretKeyRef.key' "kafka-ui-oidc/client-secret"
  hasnt "no state secret when unset" oidc 'AUTHENTICATION_STATE_SECRET'
  eq "route present by default" oidc Route '.kind' "Route"
  eq "route timeout annotation" oidc Route '.metadata.annotations["haproxy.router.openshift.io/timeout"]' "5m"
  eq "route edge termination" oidc Route '.spec.tls.termination' "edge"
  eq "route insecure redirect" oidc Route '.spec.tls.insecureEdgeTerminationPolicy' "Redirect"
  eq "route targets service" oidc Route '.spec.to.name' "kafka-ui"
  eq "proxy address forwarding" oidc ConfigMap '.data.QUARKUS_HTTP_PROXY_PROXY_ADDRESS_FORWARDING' "true"
  eq "forwarded host" oidc ConfigMap '.data.QUARKUS_HTTP_PROXY_ENABLE_FORWARDED_HOST' "true"
  eq "no ingress" oidc Ingress '.kind' ""
  eq "no certificate in oidc" oidc Certificate '.kind' ""
  eq "no PDB by default" oidc PodDisruptionBudget '.kind' ""
  eq "no NetworkPolicy by default" oidc NetworkPolicy '.kind' ""
  eq "image default tag is appVersion" oidc Deployment '.spec.template.spec.containers[0].image' "image-registry.openshift-image-registry.svc:5000/kafka-ui/kafka-ui:0.1.0"
  eq "service port named http" oidc Service '.spec.ports[0].name + "/" + (.spec.ports[0].port|tostring)' "http/8080"
  eq "checksum/config annotation set" oidc Deployment '.spec.template.metadata.annotations["checksum/config"] | length' "64"
  eq "liveness probe" oidc Deployment '.spec.template.spec.containers[0].livenessProbe.httpGet.path' "/q/health/live"
  eq "readiness probe" oidc Deployment '.spec.template.spec.containers[0].readinessProbe.httpGet.path' "/q/health/ready"
  eq "service account token not automounted (pod)" oidc Deployment '.spec.template.spec.automountServiceAccountToken' "false"
  eq "service account token not automounted (sa)" oidc ServiceAccount '.automountServiceAccountToken' "false"
  eq "pod runAsNonRoot" oidc Deployment '.spec.template.spec.securityContext.runAsNonRoot' "true"
  eq "pod seccomp RuntimeDefault" oidc Deployment '.spec.template.spec.securityContext.seccompProfile.type' "RuntimeDefault"
  eq "pod has no runAsUser" oidc Deployment '.spec.template.spec.securityContext.runAsUser' "null"
  eq "pod has no fsGroup" oidc Deployment '.spec.template.spec.securityContext.fsGroup' "null"
  eq "no privilege escalation" oidc Deployment '.spec.template.spec.containers[0].securityContext.allowPrivilegeEscalation' "false"
  eq "drop ALL capabilities" oidc Deployment '.spec.template.spec.containers[0].securityContext.capabilities.drop[0]' "ALL"
  eq "container has no runAsUser" oidc Deployment '.spec.template.spec.containers[0].securityContext.runAsUser' "null"
  hasnt "no subPath mounts" oidc 'subPath'
  hasnt "unset tuning vars not rendered" oidc 'KAFKA_UI_SEARCH_|KAFKA_UI_TAIL_|KAFKA_UI_STREAMS_|KAFKA_UI_PRODUCE_'
}

echo "== oidc with tuning, readOnlyRootFilesystem, extras"
cat >"$OUT/v-extra.yaml" <<YAML
config:
  search: { maxScanned: 5000, regexTimeout: 250ms }
  tail: { maxRows: 200 }
  streams: { maxConcurrent: 8, maxPerUser: 1 }
  produce: { idLocation: HEADERS, idKind: GLOBAL_ID, valueHeaderPrefix: "x.value.", keyHeaderPrefix: "x.key." }
containerSecurityContext: { readOnlyRootFilesystem: true }
extraEnv: [ { name: FOO, value: bar } ]
podAnnotations: { a: b }
networkPolicy: { enabled: true }
YAML
render extra "$CI/oidc.yaml" -f "$OUT/v-extra.yaml" && {
  eq "search max scanned" extra ConfigMap '.data.KAFKA_UI_SEARCH_MAX_SCANNED' "5000"
  eq "search regex timeout" extra ConfigMap '.data.KAFKA_UI_SEARCH_REGEX_TIMEOUT' "250ms"
  eq "tail max rows" extra ConfigMap '.data.KAFKA_UI_TAIL_MAX_ROWS' "200"
  eq "streams per user" extra ConfigMap '.data.KAFKA_UI_STREAMS_MAX_PER_USER' "1"
  eq "produce id location" extra ConfigMap '.data.KAFKA_UI_PRODUCE_ID_LOCATION' "HEADERS"
  eq "produce key prefix" extra ConfigMap '.data.KAFKA_UI_PRODUCE_KEY_HEADER_PREFIX' "x.key."
  eq "unset search max matches stays unrendered" extra ConfigMap '.data.KAFKA_UI_SEARCH_MAX_MATCHES' "null"
  eq "readOnlyRootFilesystem true" extra Deployment '.spec.template.spec.containers[0].securityContext.readOnlyRootFilesystem' "true"
  eq "tmp emptyDir mounted" extra Deployment '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath=="/tmp") | .name' "tmp"
  eq "extraEnv rendered" extra Deployment '.spec.template.spec.containers[0].env[] | select(.name=="FOO") | .value' "bar"
  eq "podAnnotations rendered" extra Deployment '.spec.template.metadata.annotations.a' "b"
  eq "NetworkPolicy rendered" extra NetworkPolicy '.kind' "NetworkPolicy"
}

echo "== mtls with existing secret"
render mtls "$CI/mtls-existing-secret.yaml" && {
  eq "mode mtls" mtls ConfigMap '.data.KAFKA_UI_MODE' "mtls"
  eq "cert file env" mtls ConfigMap '.data.KAFKA_UI_TLS_CERT_FILE' "/etc/kafka-ui/client/tls.crt"
  eq "key file env" mtls ConfigMap '.data.KAFKA_UI_TLS_KEY_FILE' "/etc/kafka-ui/client/tls.key"
  eq "client secret name" mtls Deployment '.spec.template.spec.volumes[] | select(.name=="client-cert") | .secret.secretName' "my-client-cert"
  eq "client cert item" mtls Deployment '.spec.template.spec.volumes[] | select(.name=="client-cert") | .secret.items[0].key + ">" + .secret.items[0].path' "user.crt>tls.crt"
  eq "client key item" mtls Deployment '.spec.template.spec.volumes[] | select(.name=="client-cert") | .secret.items[1].key + ">" + .secret.items[1].path' "user.key>tls.key"
  eq "CA key configurable" mtls Deployment '.spec.template.spec.volumes[] | select(.name=="ca-bundle") | .secret.items[0].key' "ca.crt"
  eq "client mount present" mtls Deployment '.spec.template.spec.containers[0].volumeMounts[] | select(.name=="client-cert") | .mountPath' "/etc/kafka-ui/client"
  hasnt "no OIDC settings in mtls" mtls 'QUARKUS_OIDC'
  eq "no Certificate with existing secret" mtls Certificate '.kind' ""
}

echo "== mtls with cert-manager"
render cm "$CI/mtls-certmanager.yaml" && {
  eq "Certificate rendered" cm Certificate '.apiVersion + " " + .kind' "cert-manager.io/v1 Certificate"
  eq "PKCS8 encoding" cm Certificate '.spec.privateKey.encoding' "PKCS8"
  eq "private key algorithm default" cm Certificate '.spec.privateKey.algorithm' "RSA"
  eq "usages client auth" cm Certificate '.spec.usages[]' "client auth"
  eq "issuerRef" cm Certificate '.spec.issuerRef.name + "/" + .spec.issuerRef.kind + "/" + .spec.issuerRef.group' "kafka-clients-ca/ClusterIssuer/cert-manager.io"
  eq "commonName" cm Certificate '.spec.commonName' "kafka-ui"
  eq "dnsNames" cm Certificate '.spec.dnsNames[0]' "kafka-ui.example.com"
  eq "duration" cm Certificate '.spec.duration' "2160h"
  eq "renewBefore" cm Certificate '.spec.renewBefore' "360h"
  eq "certificate secretName used as client secret" cm Deployment '.spec.template.spec.volumes[] | select(.name=="client-cert") | .secret.secretName' "kafka-ui-client"
  eq "certificate secretName matches" cm Certificate '.spec.secretName' "kafka-ui-client"
  eq "cert file env" cm ConfigMap '.data.KAFKA_UI_TLS_CERT_FILE' "/etc/kafka-ui/client/tls.crt"
}
cat >"$OUT/v-cm-ec.yaml" <<YAML
certManager: { privateKey: { algorithm: ECDSA, size: 256 }, secretName: custom-secret }
YAML
render cmec "$CI/mtls-certmanager.yaml" -f "$OUT/v-cm-ec.yaml" && {
  eq "ECDSA algorithm configurable" cmec Certificate '.spec.privateKey.algorithm' "ECDSA"
  eq "PKCS8 kept with ECDSA" cmec Certificate '.spec.privateKey.encoding' "PKCS8"
  eq "custom secretName" cmec Deployment '.spec.template.spec.volumes[] | select(.name=="client-cert") | .secret.secretName' "custom-secret"
}
render cm_oidc "$CI/oidc.yaml" --set certManager.enabled=true && eq "certManager ignored in oidc mode" cm_oidc Certificate '.kind' ""

echo "== ingress instead of route"
render ing "$CI/ingress.yaml" && {
  eq "no route" ing Route '.kind' ""
  eq "ingress rendered" ing Ingress '.spec.ingressClassName' "nginx"
  eq "ingress host" ing Ingress '.spec.rules[0].host' "kafka-ui.example.com"
  eq "ingress tls" ing Ingress '.spec.tls[0].secretName' "kafka-ui-tls"
  eq "proxy forwarding with ingress" ing ConfigMap '.data.QUARKUS_HTTP_PROXY_PROXY_ADDRESS_FORWARDING' "true"
  eq "forwarded host with ingress" ing ConfigMap '.data.QUARKUS_HTTP_PROXY_ENABLE_FORWARDED_HOST' "true"
  eq "state secret via secretKeyRef" ing Deployment '.spec.template.spec.containers[0].env[] | select(.name=="QUARKUS_OIDC_AUTHENTICATION_STATE_SECRET") | .valueFrom.secretKeyRef.key' "state-secret"
  eq "replicas" ing Deployment '.spec.replicas' "2"
  eq "explicit image tag" ing Deployment '.spec.template.spec.containers[0].image' "registry.example.com/kafka/kafka-ui:1.2.3"
  eq "PDB rendered" ing PodDisruptionBudget '.kind' "PodDisruptionBudget"
}
render noexp "$CI/oidc.yaml" --set route.enabled=false && {
  hasnt "no proxy forwarding without route/ingress" noexp 'PROXY_ADDRESS_FORWARDING'
}

echo "== fix round 1"
render big "$CI/oidc.yaml" --set config.search.maxScanned=1000000 --set config.search.maxMatches=10000000 --set image.tag=5 && {
  eq "integer 1000000 not scientific" big ConfigMap '.data.KAFKA_UI_SEARCH_MAX_SCANNED' "1000000"
  eq "integer 10000000 not scientific" big ConfigMap '.data.KAFKA_UI_SEARCH_MAX_MATCHES' "10000000"
  eq "numeric image.tag" big Deployment '.spec.template.spec.containers[0].image' "image-registry.openshift-image-registry.svc:5000/kafka-ui/kafka-ui:5"
}
render rsa "$CI/mtls-certmanager.yaml" && eq "RSA default size 2048" rsa Certificate '.spec.privateKey.size' "2048"
render ecnosize "$CI/mtls-certmanager.yaml" --set certManager.privateKey.algorithm=ECDSA --set certManager.privateKey.size=null && eq "ECDSA default size 256" ecnosize Certificate '.spec.privateKey.size' "256"
render ed "$CI/mtls-certmanager.yaml" --set certManager.privateKey.algorithm=Ed25519 && eq "Ed25519 renders no size" ed Certificate '.spec.privateKey.size' "null"
render pdb "$CI/oidc.yaml" --set podDisruptionBudget.enabled=true && {
  eq "PDB maxUnavailable 1" pdb PodDisruptionBudget '.spec.maxUnavailable' "1"
  eq "PDB no minAvailable" pdb PodDisruptionBudget '.spec.minAvailable' "null"
}
render defro "$CI/oidc.yaml" && {
  eq "readOnlyRootFilesystem default true" defro Deployment '.spec.template.spec.containers[0].securityContext.readOnlyRootFilesystem' "true"
  eq "tmp emptyDir by default" defro Deployment '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath=="/tmp") | .name' "tmp"
  eq "startupProbe" defro Deployment '.spec.template.spec.containers[0].startupProbe.failureThreshold' "30"
}
render nulls "$CI/oidc.yaml" --set config.search=null --set config.tail=null --set config.streams=null --set config.produce=null && ok "null config sub-maps render" 
render rt "$CI/oidc.yaml" --set route.tls=null && eq "route tls rendered even when null" rt Route '.spec.tls.termination' "edge"
fails "route passthrough rejected by schema" 'termination' -f "$CI/oidc.yaml" --set route.tls.termination=passthrough
helm install kafka-ui "$CHART" -f "$CI/oidc.yaml" --set oidc.clientSecret.existingSecret= --dry-run=client 2>/dev/null | grep -q 'oidc.clientSecret.existingSecret is not set' && ok "NOTES warns when client secret unset" || bad "NOTES warns when client secret unset"

echo "== default image"
yq 'del(.image)' "$CI/oidc.yaml" >"$OUT/noimage-values.yaml"
render defimg "$OUT/noimage-values.yaml" && eq "default image is Docker Hub :appVersion" defimg Deployment '.spec.template.spec.containers[0].image' "docker.io/alfeenz/kafka-ui:0.1.0"

echo "== failure messages"
fails "empty image.repository" 'image\.repository must not be empty' -f "$CI/oidc.yaml" --set image.repository=
fails "mtls without client secret" 'tls\.client\.existingSecret|certManager\.enabled' -f "$CI/mtls-existing-secret.yaml" --set tls.client.existingSecret=
fails "oidc without clientId" 'oidc\.clientId is required' -f "$CI/oidc.yaml" --set oidc.clientId=
fails "oidc without authServerUrl" 'oidc\.authServerUrl is required' -f "$CI/oidc.yaml" --set oidc.authServerUrl=
fails "replicas>1 without stateSecret" 'oidc\.stateSecret' -f "$CI/oidc.yaml" --set replicaCount=2
fails "missing kafka.bootstrapServers" 'kafka\.bootstrapServers is required' -f "$CI/oidc.yaml" --set kafka.bootstrapServers=
fails "missing registry.url" 'registry\.url is required' -f "$CI/oidc.yaml" --set registry.url=
fails "missing tls.ca.existingSecret" 'tls\.ca\.existingSecret is required' -f "$CI/oidc.yaml" --set tls.ca.existingSecret=
fails "schema rejects bad mode" 'mode' -f "$CI/oidc.yaml" --set mode=basic

echo
echo "passed: $PASS, failed: $FAILED"
[ "$FAILED" -eq 0 ]
