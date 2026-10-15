#!/usr/bin/env bash
# scripts/generate-sbom.sh - regenerate docs/SBOM.spdx.json.
#
# Purpose: produce the SBOM of the development environment (the Python
#          dependencies installed in the devcontainer) as SPDX 2.3 JSON.
# Design.: the devcontainer's site-packages path is DERIVED at run time
#          (never hardcoded); a PINNED syft is downloaded into the
#          container's /tmp and scans those site-packages. syft emits
#          extracted license TEXTS (hasExtractedLicensingInfos) for
#          package-declared licenses it cannot map to an SPDX identifier;
#          this SBOM must carry SPDX identifiers and purls only, so the
#          script strips those texts and rewrites their LicenseRef-*
#          references to NOASSERTION.
# When:   run it whenever [project].dependencies changes, and commit the
#          result together with the dependency change
#          (see docs/CONTRIBUTING.md, SBOM section).
# Usage:  scripts/generate-sbom.sh          (from the repository root,
#          with the devcontainer running)
#
# syft pin: v1.54.1 — https://github.com/anchore/syft
set -euo pipefail

SYFT_VERSION="v1.54.1"
SYFT_NUMBER="${SYFT_VERSION#v}"
COMPOSE_FILE=".devcontainer/docker-compose.yml"
SERVICE="debian-observer-development"
OUT="docs/SBOM.spdx.json"

cd "$(dirname "$0")/.."

# The devcontainer must be running.
if ! docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE" true 2>/dev/null; then
    echo "ERROR: devcontainer service '${SERVICE}' is not running." >&2
    echo "       Start it first: docker compose -f ${COMPOSE_FILE} up -d" >&2
    exit 1
fi

# Detect the container architecture (syft publishes amd64 and arm64).
case "$(docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE" uname -m)" in
    x86_64) SYFT_ARCH="amd64" ;;
    aarch64 | arm64) SYFT_ARCH="arm64" ;;
    *)
        echo "ERROR: unsupported container architecture for the syft download." >&2
        exit 1
        ;;
esac
SYFT_URL="https://github.com/anchore/syft/releases/download/${SYFT_VERSION}/syft_${SYFT_NUMBER}_linux_${SYFT_ARCH}.tar.gz"

# 1. Derive the site-packages path of the devcontainer empirically.
SITE_PACKAGES="$(docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE" python -m site --user-site)"
echo "[sbom] devcontainer site-packages: ${SITE_PACKAGES}"

# 2. Download the pinned syft into the container's /tmp.
echo "[sbom] downloading pinned syft ${SYFT_VERSION} (linux_${SYFT_ARCH}) into the container /tmp ..."
docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE" sh -eu -c "
    curl -sfSL '${SYFT_URL}' -o /tmp/syft.tar.gz
    tar -xzf /tmp/syft.tar.gz -C /tmp syft
    /tmp/syft version
"

# 3. Scan the installed dependencies into a raw document.
RAW="$(mktemp)"
trap 'rm -f "$RAW"' EXIT
echo "[sbom] scanning installed dependencies ..."
docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE" \
    /tmp/syft "$SITE_PACKAGES" -o spdx-json --source-name "debian-observer-dev-deps" \
    > "$RAW"

# 4. Normalize the document: SPDX identifiers and purls only, no license texts.
python3 - "$RAW" "$OUT" <<'PY'
import json
import sys

doc = json.load(open(sys.argv[1], encoding="utf-8"))


def only_spdx_ids(value):
    """LicenseRef-* references dangle once the texts are stripped."""
    return "NOASSERTION" if value and "LicenseRef" in value else value


for package in doc.get("packages", []):
    package["licenseConcluded"] = only_spdx_ids(package.get("licenseConcluded"))
    package["licenseDeclared"] = only_spdx_ids(package.get("licenseDeclared"))
for entry in doc.get("files", []):
    entry["licenseConcluded"] = only_spdx_ids(entry.get("licenseConcluded"))
    entry["licenseDeclared"] = only_spdx_ids(entry.get("licenseDeclared"))
doc.pop("hasExtractedLicensingInfos", None)

with open(sys.argv[2], "w", encoding="utf-8") as f:
    json.dump(doc, f)
    f.write("\n")
print("[sbom] normalized: no hasExtractedLicensingInfos, LicenseRef-* -> NOASSERTION")
PY

# 5. Sanity-check the produced document.
python3 - "$OUT" <<'PY'
import json
import sys

text = open(sys.argv[1], encoding="utf-8").read()
doc = json.loads(text)
assert doc["spdxVersion"] == "SPDX-2.3", doc["spdxVersion"]
assert doc["dataLicense"] == "CC0-1.0", doc["dataLicense"]
assert not doc.get("hasExtractedLicensingInfos"), "license texts must not be embedded"
assert "LicenseRef" not in text, "LicenseRef-* must not remain"
assert doc.get("packages"), "no packages found"
print(f"[sbom] OK: {len(doc['packages'])} packages, spdxVersion={doc['spdxVersion']}")
PY
echo "[sbom] wrote ${OUT}"
