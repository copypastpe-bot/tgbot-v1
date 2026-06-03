# Analytics Dashboard Deploy Runbook

Production URL:

```text
https://analytics.dastydev.ru
```

DNS:

```text
analytics.dastydev.ru A 91.200.150.68
```

Runtime:

- service path: `/opt/telegram-analytics`
- bind: `127.0.0.1:8090`
- systemd unit: `telegram-analytics.service`
- app command: `python -m analytics_app`

Required environment:

```text
ANALYTICS_DB_DSN=<read-only postgres dsn, or DB_DSN if read-only role is not ready>
ANALYTICS_SESSION_SECRET=<long random secret>
ANALYTICS_USERS=owner:<pbkdf2_hash>,partner:<pbkdf2_hash>
ANALYTICS_BIND_HOST=127.0.0.1
ANALYTICS_BIND_PORT=8090
```

Generate password hash:

```bash
python - <<'PY'
from analytics_app.auth import make_password_hash
print(make_password_hash("replace-this-password"))
PY
```

Nginx shape:

```nginx
server {
    server_name analytics.dastydev.ru;

    location / {
        proxy_pass http://127.0.0.1:8090;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Production verification:

```bash
getent hosts analytics.dastydev.ru
sudo nginx -t
sudo systemctl status telegram-analytics.service --no-pager
curl -I -fsS https://analytics.dastydev.ru/login
```

Before declaring ready, compare dashboard totals for one period against direct SQL.
