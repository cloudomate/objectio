"""IAM ↔ Unity Catalog bridge test.

Proves attached IAM managed policies grant or deny `unity:*` actions the
same way they grant or deny `s3:*` — i.e. the same single IAM plane covers
S3 + Iceberg + Unity, with no parallel policy language.

Flow:
  1. Admin creates a non-admin user `alice` + an access key for her.
  2. Admin creates a Unity catalog `iam_bridge_test`.
  3. Baseline: alice signs a Unity request with her SigV4 keys and is
     allowed to list catalogs (no IAM policy attached → implicit allow).
  4. Admin creates IAM policy `deny-unity-delete` with one statement:
     Deny unity:DeleteCatalog on *.
  5. Admin attaches it to alice.
  6. alice tries to delete the catalog → 403 (denied by attached IAM policy).
  7. alice can still GET the catalog (Deny was specifically on Delete).
  8. Admin detaches the policy.
  9. alice can now delete the catalog (no policy in the way).

Usage:
    OBJECTIO_HOST=http://127.0.0.1:9000 \
      AWS_ACCESS_KEY_ID=<admin-ak> AWS_SECRET_ACCESS_KEY=<admin-sk> \
      .venv-dbx/bin/python test_iam_unity_bridge.py
"""

import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")

import botocore.auth
import botocore.awsrequest
import botocore.credentials
import requests


HOST = os.environ.get("OBJECTIO_HOST", "http://127.0.0.1:9000")
ADMIN_AK = os.environ["AWS_ACCESS_KEY_ID"]
ADMIN_SK = os.environ["AWS_SECRET_ACCESS_KEY"]
REGION = os.environ.get("OBJECTIO_REGION", "us-east-1")
RUN = str(int(time.time()))
CATALOG = f"iam_bridge_{RUN}"
USER_NAME = f"alice_{RUN}"
POLICY_NAME = f"deny-unity-delete-{RUN}"


def fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}")
    sys.exit(1)


def step(t: str) -> None:
    print(f"\n• {t}")


def ok(t: str) -> None:
    print(f"  ✓ {t}")


def sigv4_session(ak: str, sk: str) -> requests.Session:
    """A requests.Session that signs every outgoing request with SigV4.

    Sign-after-prepare so Content-Length is set before botocore hashes
    the body — same pattern unity_smoke_test.py uses successfully.
    """
    creds = botocore.credentials.Credentials(ak, sk)
    # S3SigV4Auth (not the generic SigV4Auth) emits the X-Amz-Content-SHA256
    # header. The gateway uses that header value as the canonical-request
    # payload hash; without it, the server falls back to "UNSIGNED-PAYLOAD"
    # while botocore signs the actual body hash → SignatureDoesNotMatch.
    signer = botocore.auth.S3SigV4Auth(creds, "s3", REGION)
    sess = requests.Session()

    def sign(req: requests.PreparedRequest) -> requests.PreparedRequest:
        aws_req = botocore.awsrequest.AWSRequest(
            method=req.method,
            url=req.url,
            data=req.body or b"",
            headers={k: v for k, v in req.headers.items() if k.lower() != "host"},
        )
        signer.add_auth(aws_req)
        for k, v in aws_req.headers.items():
            req.headers[k] = v
        return req

    original_send = sess.send

    def send(req, **kwargs):
        sign(req)
        return original_send(req, **kwargs)

    sess.send = send  # type: ignore[assignment]
    return sess


def main() -> None:
    print(f"Host:    {HOST}")
    print(f"Catalog: {CATALOG}")
    print(f"User:    {USER_NAME}")
    print(f"Policy:  {POLICY_NAME}")

    admin = sigv4_session(ADMIN_AK, ADMIN_SK)
    admin.headers["Content-Type"] = "application/json"
    admin.headers["Accept"] = "application/json"

    # ---------- 1. Create alice ----------
    step(f"Create non-admin user {USER_NAME!r}")
    r = admin.post(
        f"{HOST}/_admin/users",
        json={"display_name": USER_NAME, "email": f"{USER_NAME}@test.local", "tenant": ""},
    )
    if r.status_code not in (200, 201):
        fail(f"create user: {r.status_code} {r.text}")
    user_id = r.json()["user_id"]
    ok(f"user_id={user_id}")

    step("Mint access key for alice")
    r = admin.post(f"{HOST}/_admin/users/{user_id}/access-keys", json={})
    if r.status_code not in (200, 201):
        fail(f"create key: {r.status_code} {r.text}")
    key = r.json()
    alice_ak = key["access_key_id"]
    alice_sk = key["secret_access_key"]
    ok(f"access_key_id={alice_ak}")
    alice = sigv4_session(alice_ak, alice_sk)
    alice.headers["Content-Type"] = "application/json"
    alice.headers["Accept"] = "application/json"

    # ---------- 2. Admin creates Unity catalog ----------
    step(f"Admin creates Unity catalog {CATALOG!r}")
    r = admin.post(
        f"{HOST}/api/2.1/unity-catalog/catalogs",
        json={"name": CATALOG, "comment": "iam-bridge smoke test"},
    )
    if r.status_code not in (200, 201):
        fail(f"create catalog: {r.status_code} {r.text}")
    ok("catalog created by admin")

    # ---------- 3. Baseline: alice can GET ----------
    step("Baseline (no policy attached): alice GETs the catalog")
    r = alice.get(f"{HOST}/api/2.1/unity-catalog/catalogs/{CATALOG}")
    if r.status_code != 200:
        fail(f"alice baseline GET: {r.status_code} {r.text}")
    ok("alice can GET the catalog (implicit allow with no IAM policy)")

    # ---------- 4. Create the deny-Unity-delete IAM policy ----------
    step(f"Create IAM policy {POLICY_NAME!r} → Deny unity:DeleteCatalog on *")
    r = admin.post(
        f"{HOST}/_admin/policies",
        json={
            "name": POLICY_NAME,
            "policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Deny",
                        "Action": ["unity:DeleteCatalog"],
                        "Resource": ["*"],
                    },
                ],
            },
        },
    )
    if r.status_code not in (200, 201):
        fail(f"create policy: {r.status_code} {r.text}")
    ok("IAM policy created")

    # ---------- 5. Attach to alice ----------
    step(f"Attach {POLICY_NAME!r} to {USER_NAME!r}")
    r = admin.post(
        f"{HOST}/_admin/policies/attach",
        json={"policy_name": POLICY_NAME, "user_id": user_id, "group_id": ""},
    )
    if r.status_code not in (200, 201, 204):
        fail(f"attach policy: {r.status_code} {r.text}")
    ok("policy attached to alice")

    # ---------- 6. alice DELETE → 403 ----------
    step("alice tries to DELETE the catalog → expect 403 (denied by IAM)")
    r = alice.delete(f"{HOST}/api/2.1/unity-catalog/catalogs/{CATALOG}?force=true")
    if r.status_code != 403:
        fail(f"DELETE expected 403, got {r.status_code}: {r.text}")
    if "iam" not in r.text.lower() and "denied" not in r.text.lower():
        fail(f"403 message should mention IAM/denied, got: {r.text}")
    ok(f"DELETE rejected with 403 — message: {r.text[:200]}")

    # ---------- 7. alice GET still works ----------
    step("alice GETs the catalog → still allowed (Deny was scoped to DeleteCatalog)")
    r = alice.get(f"{HOST}/api/2.1/unity-catalog/catalogs/{CATALOG}")
    if r.status_code != 200:
        fail(f"GET should still work, got {r.status_code}: {r.text}")
    ok("GET still works (proves the IAM check is per-action, not blanket)")

    # ---------- 8. Detach ----------
    step("Detach policy from alice")
    r = admin.post(
        f"{HOST}/_admin/policies/detach",
        json={"policy_name": POLICY_NAME, "user_id": user_id, "group_id": ""},
    )
    if r.status_code not in (200, 204):
        fail(f"detach: {r.status_code} {r.text}")
    ok("detached")

    # ---------- 9. alice can now DELETE ----------
    step("alice DELETEs the catalog → now allowed")
    r = alice.delete(f"{HOST}/api/2.1/unity-catalog/catalogs/{CATALOG}?force=true")
    if r.status_code not in (200, 204):
        fail(f"DELETE after detach: {r.status_code} {r.text}")
    ok("catalog deleted by alice")

    # ---------- Cleanup ----------
    step("Cleanup")
    admin.delete(f"{HOST}/_admin/policies/{POLICY_NAME}")
    admin.delete(f"{HOST}/_admin/users/{user_id}")
    ok("policy + user removed")

    print("\n[PASS] IAM ↔ Unity bridge: attached IAM Deny correctly blocks unity:DeleteCatalog;")
    print("       detaching restores access. One IAM plane covers S3 + Unity.")


if __name__ == "__main__":
    main()
