#!/usr/bin/env bash
# Render tests for k8s/rebalancer-helm (helm template only; nothing is applied anywhere).
# Run by .github/workflows/rebalancer-chart-test.yml.
set -u
CHART="$(cd "$(dirname "$0")/.." && pwd)"
T=11111111-1111-1111-1111-111111111111
A=22222222-2222-2222-2222-222222222222
BASE=(--set image.repository=example/rebalancer --set image.tag=ci)
fail=0
render() { helm template r "$CHART" "${BASE[@]}" "$@" 2>&1; }
ok() { # name, args...
  local n=$1; shift; local out; out=$(render "$@") || { echo "FAIL $n: expected render ok"; echo "$out"; fail=1; return 1; }
  RENDERED=$out; echo "ok   $n"
}
refuse() { # name, expected-substring, args...
  local n=$1 want=$2; shift 2; local out
  if out=$(render "$@"); then echo "FAIL $n: expected render failure"; fail=1; return; fi
  if ! grep -qF -- "$want" <<<"$out"; then echo "FAIL $n: failure did not mention '$want'"; echo "$out"; fail=1; return; fi
  echo "ok   $n"
}
has()  { grep -qF -- "$1" <<<"$RENDERED" || { echo "FAIL missing: $1"; fail=1; }; }
lacks(){ grep -qF -- "$1" <<<"$RENDERED" && { echo "FAIL unexpected: $1"; fail=1; }; }
PILOT=(--set pilot.tenantId=$T --set pilot.accountId=$A)
BROKER=(--set credentialBroker.enabled=true)

ok "flag off, pilot only (unchanged)" "${PILOT[@]}"
has "name: PILOT_TENANT_ID"; has "name: PILOT_ALPACA_KEY_ID"; lacks "name: REBALANCER_SERVE_PROPOSAL_PLANS"; lacks "name: REBALANCER_CREDENTIAL_BROKER_URL"
refuse "flag off, no pilot ids" "pilot.tenantId must not be the nil UUID"
ok "flag on, proposal plans only" --set serveProposalPlans=true "${BROKER[@]}"
has 'name: REBALANCER_SERVE_PROPOSAL_PLANS'; has REBALANCER_CREDENTIAL_BROKER_URL; has CREDENTIAL_BROKER_SERVICE_TOKEN
has 'name: rebalancer-credential-broker'; lacks "name: PILOT_TENANT_ID"; lacks "name: PILOT_ACCOUNT_ID"; lacks "name: PILOT_ALPACA_KEY_ID"; lacks "pilot-tenant-id:"
has 'name: REBALANCER_PAPER_ONLY'; lacks "name: CREDENTIALS_ENCRYPTION_KEY"
ok "flag on, owner pilot alongside" --set serveProposalPlans=true "${BROKER[@]}" "${PILOT[@]}"
has "name: PILOT_TENANT_ID"; has "name: PILOT_ALPACA_KEY_ID"; has 'name: REBALANCER_SERVE_PROPOSAL_PLANS'
refuse "flag on, broker disabled" "requires credentialBroker.enabled=true" --set serveProposalPlans=true
refuse "flag on, broker secret name empty" "credentialBroker.secretName" --set serveProposalPlans=true "${BROKER[@]}" --set credentialBroker.secretName=
refuse "flag on, token key empty" "credentialBroker.tokenKey" --set serveProposalPlans=true "${BROKER[@]}" --set credentialBroker.tokenKey=
refuse "flag on, half pilot pair" "BOTH pilot.tenantId and pilot.accountId" --set serveProposalPlans=true "${BROKER[@]}" --set pilot.tenantId=$T
refuse "extraEnv encryption key" "CREDENTIALS_ENCRYPTION_KEY" --set serveProposalPlans=true "${BROKER[@]}" --set 'extraEnv[0].name=CREDENTIALS_ENCRYPTION_KEY' --set 'extraEnv[0].value=x'
refuse "extraEnv serve flag" "use the serveProposalPlans value" "${PILOT[@]}" --set 'extraEnv[0].name=REBALANCER_SERVE_PROPOSAL_PLANS' --set-string 'extraEnv[0].value=true'
exit $fail
