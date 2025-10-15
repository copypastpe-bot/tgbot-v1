Cheat Sheet: Часто используемые команды

Управление сервисом бота (systemd)
sudo systemctl start telegram-bot.service
sudo systemctl stop telegram-bot.service
sudo systemctl restart telegram-bot.service
sudo systemctl status telegram-bot.service --no-pager -l
journalctl -u telegram-bot.service -f -n 80 --no-pager -l

Работа с процессами
ps aux | grep -E "[p]ython .*bot.py" | grep -v grep
kill -9 PID

Работа с PostgreSQL
sudo -u postgres psql -d clients_db
sudo -u postgres psql -d clients_db -v ON_ERROR_STOP=1 -f /tmp/filename.sql
\dt
\d+ table_name
SELECT * FROM clients LIMIT 10;

Git и работа с репозиторием
git status
git add .
git commit -m "описание изменений"
git push origin main
git pull origin main

Прочее
deactivate
cd /opt/telegram-bot
cat .env
