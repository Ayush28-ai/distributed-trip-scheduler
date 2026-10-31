"""JWT auth with three roles and tenant isolation.
- admin:    every tenant; can create tenants and see the cluster.
- operator: own tenant only; create/trigger jobs, pause/resume, replay dead letters.
- employee: own tenant only; read-only (jobs, runs, next fire times, stats).
Demo users come from env (ADMIN_USER/ADMIN_PASSWORD, OPERATORS, EMPLOYEES)."""
import hmac
import time
from dataclasses import dataclass

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings

bearer = HTTPBearer(auto_error=False)

ADMIN, OPERATOR, EMPLOYEE = "admin", "operator", "employee"


@dataclass
class User:
    username: str
    role: str      # admin | operator | employee
    tenant: str    # "*" for admin

    @property
    def is_admin(self) -> bool:
        return self.role == ADMIN

    @property
    def can_write(self) -> bool:
        return self.role in (ADMIN, OPERATOR)


def _users() -> dict[str, tuple[str, str, str]]:
    users = {settings.admin_user: (settings.admin_password, ADMIN, "*")}
    for role, spec in ((OPERATOR, settings.operators), (EMPLOYEE, settings.employees)):
        for entry in filter(None, spec.split(",")):
            name, pwd, tenant = entry.strip().split(":")
            users[name] = (pwd, role, tenant)
    return users


def authenticate(username: str, password: str) -> User | None:
    found = _users().get(username)
    if not found or not hmac.compare_digest(found[0], password):
        return None
    return User(username, found[1], found[2])


def issue_token(user: User) -> str:
    now = int(time.time())
    claims = {"sub": user.username, "role": user.role, "tenant": user.tenant,
              "iat": now, "exp": now + settings.jwt_ttl_s}
    return jwt.encode(claims, settings.jwt_secret, algorithm="HS256")


def current_user(creds: HTTPAuthorizationCredentials | None = Depends(bearer)) -> User:
    if creds is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    try:
        c = jwt.decode(creds.credentials, settings.jwt_secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or expired token")
    return User(c["sub"], c["role"], c["tenant"])


def require_admin(user: User = Depends(current_user)) -> User:
    if not user.is_admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "admin only")
    return user


def require_writer(user: User = Depends(current_user)) -> User:
    if not user.can_write:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "read-only role")
    return user


def check_tenant(user: User, tenant_id: str) -> None:
    if not user.is_admin and user.tenant != tenant_id:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "not allowed for this tenant")


def tenant_scope(user: User, requested: str | None) -> str | None:
    """Admin: whatever was requested (None = all). Others: always their own tenant."""
    if user.is_admin:
        return requested
    if requested and requested != user.tenant:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "not allowed for this tenant")
    return user.tenant
