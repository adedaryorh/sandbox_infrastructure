python3 - <<'PY'
from pathlib import Path
import secrets
import re

path = Path(".env")
text = path.read_text()
text, count = re.subn(
    r"(?m)^SANDBOX_BOOTSTRAP_API_KEY=.*$",
    "SANDBOX_BOOTSTRAP_API_KEY=" + secrets.token_hex(32),
    text,
    )
if count != 1:
    raise SystemExit("Expected exactly one bootstrap API key entry.")
path.write_text(text)
path.chmod(0o600)
PY