"""Refuse to start if default/weak secrets are detected."""
import os
import sys

KNOWN_BAD = {
    "PG_PASSWORD":        ["nexus_password", "password", "postgres"],
    "POSTGRES_PASSWORD":  ["nexus_password", "password", "postgres"],
    "DASHBOARD_PASSWORD": ["nexus_secure_pass_123", "password", "admin"],
    "GRAFANA_PASSWORD":   ["change_this_before_production", "password", "admin"],
    "API_KEY":            ["", "changeme", "change_me", "password", "admin"],
}

REQUIRED_IN_PROD = [
    "PG_PASSWORD",
    "DASHBOARD_PASSWORD",
    "GRAFANA_PASSWORD",
    "API_KEY",
]


def _is_dev_env(env: str) -> bool:
    return env in {"development", "local", "test"}


def _is_placeholder(val: str) -> bool:
    upper = val.upper()
    return "CHANGE_ME" in upper or "REPLACE_ME" in upper

def validate():
    errors = []
    env = os.environ.get("ENV", "development").strip().lower()
    is_dev = _is_dev_env(env)

    if not is_dev:
        for var in REQUIRED_IN_PROD:
            if not os.getenv(var, "").strip():
                errors.append(f"  {var} is required when ENV={env}.")

    for var, bad_values in KNOWN_BAD.items():
        val = os.getenv(var, "").strip()
        if val in bad_values or _is_placeholder(val):
            msg = f"  {var} is set to a known default value '{val}'. Set a strong secret before deploying."
            if is_dev:
                print(f"WARNING: {msg}", file=sys.stderr)
            else:
                errors.append(msg)

    if errors:
        print("STARTUP ABORTED — insecure configuration detected:", file=sys.stderr)
        for e in errors:
            print(e, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    validate()
