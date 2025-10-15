# Шпаргалка синхронизации проекта (VS Code → GitHub → Сервер)

## 1. Локально (VS Code / Cursor)
После правок выполняй:
```bash
git status
git add .
git commit -m "описание"
git push origin main

НА СЕРВЕРЕ!!!
cd /opt/telegram-bot
git fetch origin
git pull --rebase origin main

Если ошибка «unstaged changes»:
	•	Отбросить локальные правки:
git reset --hard
git clean -fd
git pull --rebase origin main

	•	Либо временно отложить:
git stash
git pull --rebase origin main
git stash pop

ПЕРЕЗАПУСК БОТА
sudo systemctl restart telegram-bot.service
journalctl -u telegram-bot.service -n 50 --no-pager -o cat

Проверка
cd /opt/telegram-bot
echo "HEAD: $(git rev-parse --short HEAD)"
grep -n 'Command("expense")' bot.py || true
grep -n 'ADMIN_TG_IDS' bot.py || true
grep -E '^ADMIN_IDS=' .env || true

Проверка одного экземпляра
ps aux | grep -E "[p]ython .*bot.py" | grep -v grep
sudo kill -9 PID   # если есть лишние
sudo systemctl restart telegram-bot.service

