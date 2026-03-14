"""Refuse to start if default/weak secrets are detected."""
import os
import sys

KNOWN_BAD = {
    "PG_PASSWORD":        ["nexus_password", "password", "postgres"],
    "DASHBOARD_PASSWORD": ["nexus_secure_pass_123", "password", "admin"],
}

def validate():
    errors = []
    env = os.environ.get("ENV", "development")

    for var, bad_values in KNOWN_BAD.items():
        val = os.getenv(var, "")
        if val in bad_values:
            msg = f"  {var} is set to a known default value '{val}'. Set a strong secret before deploying."
            if env == "development":
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
