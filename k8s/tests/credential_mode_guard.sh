#!/usr/bin/env bash
# Chart guard test: config.credentialMode is validated at render time.
#   none (default)  -> renders, CREDENTIAL_MODE="none"
#   single_tenant   -> only with a real UUID tenantId (and the ConfigMap enabled)
#   anything else   -> the render FAILS with a clear message
# Usage: k8s/tests/credential_mode_guard.sh   (needs helm)
set -u
cd "$(dirname "$0")/../.."
CHART=k8s/signal-engine-helm
BASE=(--set image.repository=example/signal-engine --set image.tag=ci)
fail=0

render() { helm template signal-engine "$CHART" "${BASE[@]}" "$@" 2>&1; }

expect_ok() { # name expected-mode args...
  local name=$1 mode=$2; shift 2
  local out; out=$(render "$@")
  if [ $? -ne 0 ]; then echo "FAIL $name: render failed: $(echo "$out" | tail -2)"; fail=1; return; fi
  if ! echo "$out" | grep -A1 'name: CREDENTIAL_MODE' | grep -q "value: \"$mode\""; then
    echo "FAIL $name: CREDENTIAL_MODE is not \"$mode\""; fail=1; return
  fi
  echo "ok   $name (CREDENTIAL_MODE=$mode)"
}

expect_fail() { # name message-fragment args...
  local name=$1 frag=$2; shift 2
  local out; out=$(render "$@")
  if [ $? -eq 0 ]; then echo "FAIL $name: render SUCCEEDED but must fail"; fail=1; return; fi
  if ! echo "$out" | grep -q "$frag"; then
    echo "FAIL $name: failed, but without '$frag': $(echo "$out" | tail -2)"; fail=1; return
  fi
  echo "ok   $name (rejected: $frag)"
}

UUID=123e4567-e89b-12d3-a456-426614174000

for V in values-qa.yaml values-prod.yaml; do
  expect_ok "$V default" none -f "$CHART/$V"
  helm lint "$CHART" -f "$CHART/$V" >/dev/null 2>&1 && echo "ok   helm lint $V" || { echo "FAIL helm lint $V"; fail=1; }
done
expect_ok   "explicit none"                   none          --set config.credentialMode=none
expect_ok   "none ignores a tenantId"         none          --set config.credentialMode=none --set config.tenantId=$UUID
expect_ok   "single_tenant + UUID"            single_tenant --set config.credentialMode=single_tenant --set config.tenantId=$UUID
expect_ok   "single_tenant mixed-case mode"   single_tenant --set config.credentialMode=Single_Tenant --set config.tenantId=$UUID
expect_fail "single_tenant without tenantId"  "requires config.tenantId to be a UUID" --set config.credentialMode=single_tenant
expect_fail "single_tenant with garbage"      "requires config.tenantId to be a UUID" --set config.credentialMode=single_tenant --set config.tenantId=not-a-uuid
expect_fail "single_tenant with nil UUID"     "not the nil UUID" --set config.credentialMode=single_tenant --set config.tenantId=00000000-0000-0000-0000-000000000000
expect_fail "single_tenant without ConfigMap" "needs configMap.enabled=true" --set config.credentialMode=single_tenant --set config.tenantId=$UUID --set configMap.enabled=false
expect_fail "multi_tenant does not exist"     "must be 'none' or 'single_tenant'" --set config.credentialMode=multi_tenant

[ $fail -eq 0 ] && echo "ALL CHART GUARD CHECKS PASSED" || echo "CHART GUARD CHECKS FAILED"
exit $fail
