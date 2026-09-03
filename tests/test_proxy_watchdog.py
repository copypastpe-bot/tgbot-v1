"""Тесты сторожа канала до Telegram через прокси.

Проверяется решающая логика: когда будить владельца, когда молчать и когда
сообщать, что связь вернулась. Сеть не задействована.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from telegram_proxy_watchdog import (  # noqa: E402
    ACTION_DOWN,
    ACTION_RECOVERED,
    EMPTY_STATE,
    build_alert_text,
    decide,
    humanize_minutes,
    load_env,
    parse_ip_pool,
)

T0 = 1_700_000_000.0


def test_quiet_when_everything_works():
    state, action = decide(dict(EMPTY_STATE), ok=True, now=T0)
    assert action is None
    assert state == EMPTY_STATE


def test_single_failure_does_not_wake_the_owner():
    """Одна неудачная проверка - это рябь на сети, а не авария."""
    state, action = decide(dict(EMPTY_STATE), ok=False, now=T0)
    assert action is None
    assert state["failures"] == 1
    assert state["down_since"] == T0


def test_second_failure_in_a_row_raises_the_alarm():
    state, _ = decide(dict(EMPTY_STATE), ok=False, now=T0)
    state, action = decide(state, ok=False, now=T0 + 300)
    assert action == ACTION_DOWN
    assert state["last_alert"] == T0 + 300
    assert state["down_since"] == T0, "начало аварии не должно съезжать"


def test_alarm_is_not_repeated_every_check():
    state, _ = decide(dict(EMPTY_STATE), ok=False, now=T0)
    state, _ = decide(state, ok=False, now=T0 + 300)
    state, action = decide(state, ok=False, now=T0 + 600)
    assert action is None
    assert state["failures"] == 3


def test_alarm_repeats_once_an_hour_while_the_outage_lasts():
    state, _ = decide(dict(EMPTY_STATE), ok=False, now=T0)
    state, _ = decide(state, ok=False, now=T0 + 300)
    state, action = decide(state, ok=False, now=T0 + 300 + 3601)
    assert action == ACTION_DOWN


def test_recovery_is_reported_only_if_the_owner_was_alarmed():
    state, _ = decide(dict(EMPTY_STATE), ok=False, now=T0)
    state, _ = decide(state, ok=False, now=T0 + 300)
    state, action = decide(state, ok=True, now=T0 + 900)
    assert action == ACTION_RECOVERED
    assert state == EMPTY_STATE


def test_recovery_after_a_ripple_stays_silent():
    """Сбой был один, тревогу не поднимали - значит и отбой давать некому."""
    state, _ = decide(dict(EMPTY_STATE), ok=False, now=T0)
    state, action = decide(state, ok=True, now=T0 + 300)
    assert action is None
    assert state == EMPTY_STATE


def test_alert_text_names_the_rollback():
    state = {"failures": 2, "down_since": T0, "last_alert": T0 + 300}
    text = build_alert_text(ACTION_DOWN, state, T0 + 300)
    assert "TELEGRAM_PROXY_URL" in text
    assert "Contabo" in text
    assert "5 мин" in text


def test_recovery_text_says_how_long_it_lasted():
    state = {"failures": 3, "down_since": T0, "last_alert": T0 + 300}
    text = build_alert_text(ACTION_RECOVERED, state, T0 + 7200)
    assert "восстановлена" in text
    assert "2 ч" in text


def test_humanize_minutes():
    assert humanize_minutes(30) == "1 мин"
    assert humanize_minutes(300) == "5 мин"
    assert humanize_minutes(3600) == "1 ч"
    assert humanize_minutes(5400) == "1 ч 30 мин"


def test_parse_ip_pool_accepts_the_env_format():
    assert parse_ip_pool("1.2.3.4, 5.6.7.8;9.10.11.12") == ["1.2.3.4", "5.6.7.8", "9.10.11.12"]
    assert parse_ip_pool("") == []


def test_load_env_reads_quotes_and_skips_comments(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# комментарий\n"
        "BOT_TOKEN=\"123:abc\"\n"
        "\n"
        "TELEGRAM_PROXY_URL=http://user:pass@1.2.3.4:39443\n"
        "СЛОМАННАЯ СТРОКА\n",
        encoding="utf-8",
    )
    values = load_env(env_file)
    assert values["BOT_TOKEN"] == "123:abc"
    assert values["TELEGRAM_PROXY_URL"] == "http://user:pass@1.2.3.4:39443"
    assert "СЛОМАННАЯ СТРОКА" not in values
