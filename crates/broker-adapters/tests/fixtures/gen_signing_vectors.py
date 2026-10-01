# Regenerate with: python gen_signing_vectors.py
# Independent reference for Kraken API-Sign, written from the documented algorithm and using only
# hashlib/hmac/base64 (it shares no code with the Rust crate):
#   API-Sign = base64( HMAC-SHA512( base64_decode(secret), uri_path + SHA256(nonce + urlencoded_post_data) ) )
# Output: signing_vectors.json, consumed by tests/signing.rs.
import base64
import hashlib
import hmac
import json
import os
import urllib.parse


def sign(urlpath, postdata, secret):
    nonce = dict(urllib.parse.parse_qsl(postdata))["nonce"]
    encoded = (nonce + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()


DOC_SECRET = "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg=="
DOC_SIG_FROM_MEMORY = "4/dpxb3iT4tp/ZCVEwSnEsLxx0bqyhLpdfOpc6fn7OR8+UClSV5n9E6aSS8MPtnRfp32bAb0nmbRn6H8ndwLUQ=="
vectors = []


def add(name, secret, path, body):
    vectors.append(
        dict(
            name=name,
            secret=secret,
            path=path,
            body=body,
            nonce=dict(urllib.parse.parse_qsl(body))["nonce"],
            sig=sign(path, body, secret),
        )
    )


add(
    "kraken_docs_example",
    DOC_SECRET,
    "/0/private/AddOrder",
    "nonce=1616492376594&ordertype=limit&pair=XBTUSD&price=37500&type=buy&volume=1.25",
)
fake_secret = base64.b64encode(bytes(range(64))).decode()
add("balance_ns_nonce", fake_secret, "/0/private/Balance", "nonce=1758463200123456789")
add(
    "addorder_userref_validate",
    fake_secret,
    "/0/private/AddOrder",
    "nonce=1758463200123456790&pair=XBTUSD&type=buy&ordertype=limit&volume=0.00250000&price=61234.5"
    "&userref=1234567890&validate=true",
)
add(
    "cancel_plain_txid_short_secret",
    base64.b64encode(b"unit-test-secret-not-real").decode(),
    "/0/private/CancelOrder",
    "nonce=42&txid=OABC12-XYZ23-DEF456",
)
add(
    "body_with_percent_encoded_chars",
    fake_secret,
    "/0/private/QueryOrders",
    "nonce=1758463200999999999&txid=OAAAAA-BBBBB-CCCCCC%2COBBBBB-CCCCC-DDDDDD&trades=true&note=a%20b%2Bc",
)
add("openorders_userref_max", fake_secret, "/0/private/OpenOrders", "nonce=9007199254740993&userref=2147483647")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signing_vectors.json")
with open(out, "w", newline="\n") as fh:
    fh.write(json.dumps(vectors, indent=1) + "\n")
print("wrote", out)
print("doc example (from memory) matches independent python:", vectors[0]["sig"] == DOC_SIG_FROM_MEMORY)
