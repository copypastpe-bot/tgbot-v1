"""Разведка перед включением разговора с клиентом: приходят ли нужные события.

Ничего не меняет — только читает amoCRM. Отвечает на три вопроса, от которых
зависит, заработает ли фича вообще:

1. Приходят ли события перехода в «Заказ оформлен» воронки реализации.
2. Заполнена ли в этих сделках дата работы — без неё письма не будет.
3. Успевает ли вопрос за сутки, или заказ оформляют впритык.

Запуск на сервере, из каталога бота:
    .venv-wahelp/bin/python -m scripts.check_order_created_events [дней]

Телефоны печатаются последними четырьмя цифрами: отчёт может попасть в лог.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import os
import sys

from dotenv import load_dotenv

from notifications.amo_exchange import incoming_from_amo
from notifications.amocrm_api import AmoCRMAPIClient, extract_event_entity_id, normalize_lead
from notifications.client_messaging import (
    is_order_created_event,
    order_from_lead,
    plan_confirmation,
)

MOSCOW_TZ = timezone(timedelta(hours=3))


def _normalize_phone(raw: str) -> tuple[str | None, str | None]:
    digits = "".join(ch for ch in (raw or "") if ch.isdigit())
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) != 11:
        return None, None
    return f"+{digits}", digits


async def main(days: int) -> None:
    load_dotenv()
    api_base = (os.getenv("AMOCRM_API_BASE") or "").strip().rstrip("/")
    token = (os.getenv("AMOCRM_API_TOKEN") or "").strip()
    if not api_base or not token:
        print("Нет доступа к amoCRM: заполните AMOCRM_API_BASE и AMOCRM_API_TOKEN")
        return

    now = datetime.now(MOSCOW_TZ)
    since = int((now - timedelta(days=days)).timestamp())
    print(f"Смотрю события за {days} сут. с {(now - timedelta(days=days)):%d.%m %H:%M}\n")

    async with AmoCRMAPIClient(api_base, token) as client:
        events = await client.fetch_events(event_types=["lead_status_changed"],
                                           created_from=since, limit=250)
        ours = [event for event in events if is_order_created_event(event)]
        print(f"Событий смены этапа: {len(events)}")
        print(f"Из них «Заказ оформлен» воронки реализации: {len(ours)}\n")

        for event in ours:
            lead_id = extract_event_entity_id(event)
            if not lead_id:
                continue
            lead = await client.fetch_lead(lead_id)
            contact_ids = normalize_lead(lead).contact_ids
            contact = None
            for contact_id in contact_ids:
                contact = await client.fetch_contact(int(contact_id))
                break
            incoming = incoming_from_amo(lead, contact, normalize_phone=_normalize_phone)
            order = order_from_lead(lead, tz=MOSCOW_TZ)
            plan = plan_confirmation(order_at=order.order_at, now=now)

            phone = f"...{incoming.digits[-4:]}" if incoming.digits else "нет телефона"
            when = order.order_at.strftime("%d.%m %H:%M") if order.order_at else "нет даты"
            question = plan.ask_at.strftime("%d.%m %H:%M") if plan.ask_at else "не будет"
            print(f"сделка {lead_id}: работа {when}, телефон {phone}, "
                  f"адрес {'есть' if order.address else 'нет'}, "
                  f"вопрос {question} ({plan.reason})")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 1))
