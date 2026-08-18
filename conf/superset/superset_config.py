import os
import base64
import json
from flask_appbuilder.security.manager import AUTH_OAUTH
from superset.security import SupersetSecurityManager

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://platform:platform@postgres/superset",
)
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "changeme")
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False

# ── Reverse-proxy / ngrok fix ──────────────────────────────────────────────
ENABLE_PROXY_FIX = True
PROXY_FIX_CONFIG = {"x_for": 1, "x_proto": 1, "x_host": 1, "x_prefix": 1}

_public_url = os.environ.get("SUPERSET_PUBLIC_URL", "")
if _public_url.startswith("https"):
    PREFERRED_URL_SCHEME = "https"
# ──────────────────────────────────────────────────────────────────────────

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Admin"
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_ROLES_MAPPING = {"Admin": ["Admin"]}

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "token_key": "access_token",
        "icon": "fa-key",
        "remote_app": {
            "client_id": os.environ.get("OAUTH_CLIENT_ID"),
            "client_secret": os.environ.get("OAUTH_CLIENT_SECRET"),
            "server_metadata_url": os.environ.get("OPENID_PROVIDER_URL"),
            "client_kwargs": {
                "scope": "openid email profile",
                "response_type": "code",
            },
        },
    }
]

class KeycloakSecurityManager(SupersetSecurityManager):
    def oauth_user_info(self, provider, response=None):
        if provider == "keycloak":
            token = self.oauth.keycloak.token
            payload_b64 = token["id_token"].split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            claims = json.loads(base64.b64decode(payload_b64))
            return {
                "username": claims.get("preferred_username", ""),
                "first_name": claims.get("given_name", ""),
                "last_name": claims.get("family_name", ""),
                "email": claims.get("email", ""),
                "role_keys": ["Admin"],
            }
        return super().oauth_user_info(provider, response)

CUSTOM_SECURITY_MANAGER = KeycloakSecurityManager
