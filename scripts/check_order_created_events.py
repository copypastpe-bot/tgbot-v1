"""Разведка перед включением разговора с клиентом: что робот увидел бы сейчас.

Ничего не меняет — только читает amoCRM. Показывает по каждому оформленному
заказу за период: дату работы, адрес, телефон, найденную дочернюю сделку
реализации и то, будет ли вопрос за сутки.

Триггер — переход лида в «Передано в работу»: именно тогда робот владельца
переносит заказ из календаря в CRM и заводит дочернюю сделку. В «Заказ оформлен»
сделка не переходит, она там создаётся, поэтому события смены этапа туда нет.

Запуск на сервере, из каталога бота:
    .venv/bin/python -m scripts.check_order_created_events [дней]

Телефоны печатаются последними четырьмя цифрами: отчёт может попасть в лог.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import os
import sys

from dotenv import load_dotenv

from notifications.amo_exchange import incoming_from_amo, outcome_of_event
from notifications.amocrm_api import AmoCRMAPIClient, extract_event_entity_id, normalize_lead
from notifications.client_messaging import (
    child_deal_id,
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
        ours = [event for event in events if outcome_of_event(event) == "won"]
        print(f"Событий смены этапа: {len(events)}")
        print(f"Из них «Передано в работу» — оформленных заказов: {len(ours)}\n")

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
            deal_id = child_deal_id(await client.fetch_lead_notes(lead_id))

            phone = f"...{incoming.digits[-4:]}" if incoming.digits else "нет телефона"
            when = order.order_at.strftime("%d.%m %H:%M") if order.order_at else "нет даты"
            question = plan.ask_at.strftime("%d.%m %H:%M") if plan.ask_at else "не будет"
            print(f"лид {lead_id}: работа {when}, телефон {phone}, "
                  f"адрес {'есть' if order.address else 'нет'}, "
                  f"сделка {deal_id or 'НЕ НАЙДЕНА'}, "
                  f"вопрос {question} ({plan.reason})")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 1))
