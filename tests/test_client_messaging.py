"""Правила разговора с клиентом до работы.

Два правила здесь важнее остальных, потому что нарушение любого из них робот
не заметит, а владелец заметит поздно:

1. **Непонятое не гадаем.** Робот различает ровно «да» и «нет», потому что сам
   просит ответить одним словом. Всё остальное — «перенесите на среду»,
   «не знаю» — уходит владельцу целиком: угадывать смысл дороже, чем лишнее
   сообщение.
2. **Владельца зовём один раз на заказ.** Сторож молчунов ходит раз в десять
   минут; забудь он про уже отправленный сигнал — владелец получал бы одно
   и то же сообщение каждые десять минут до самой работы.
"""

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from notifications import client_messaging as cm

from notifications.client_messaging import (
    AMO_PIPELINE_REALIZATION,
    AMO_STAGE_CONFIRMED,
    AMO_STAGE_ORDER_CREATED,
    ASK_BEFORE,
    CONFIRM_REQUEST_EVENT,
    SILENCE_LIMIT,
    child_deal_id,
    decide_on_answer,
    is_question_letter,
    letter_failure_words,
    letter_payload,
    may_ask_question,
    order_from_lead,
    pick_realization_deal,
    owner_alert_text,
    parse_answer,
    plan_confirmation,
    prefer_deal_details,
    should_call_owner,
    should_move_deal,
    should_report_unasked,
)

MSK = timezone(timedelta(hours=3))


class ParseAnswerTests(unittest.TestCase):
    def test_yes_variants(self):
        for text in ("Да", "да", "ДА", "да ", "да!", "Да, жду"):
            self.assertEqual(parse_answer(text), "yes", text)

    def test_no_variants(self):
        for text in ("Нет", "нет", "НЕТ", "нет!", "Нет, отмена"):
            self.assertEqual(parse_answer(text), "no", text)

    def test_anything_else_is_unclear(self):
        """Непонятое не гадаем — оно уходит владельцу."""
        for text in ("перенесите на среду", "5", "", "ok", "давайте"):
            self.assertEqual(parse_answer(text), "unclear", text)

    def test_uncertainty_is_not_a_refusal(self):
        """«Не знаю» — это не отказ, а разговор: им занимается владелец.

        Разница видна в отчёте: посчитай робот такое отказом, владелец увидел бы
        завышенное число отказавшихся клиентов и стал бы искать несуществующую
        проблему.
        """
        for text in ("не знаю", "не уверен", "не смогу сказать"):
            self.assertEqual(parse_answer(text), "unclear", text)

    def test_stop_is_not_an_answer(self):
        """STOP — отписка, ей занимается другая ветка."""
        self.assertEqual(parse_answer("STOP"), "unclear")

    def test_missing_text_is_unclear(self):
        self.assertEqual(parse_answer(None), "unclear")


class PlanConfirmationTests(unittest.TestCase):
    NOW = datetime(2026, 8, 28, 12, 0, tzinfo=MSK)

    def test_question_is_scheduled_a_day_before(self):
        order_at = self.NOW + timedelta(days=3)
        plan = plan_confirmation(order_at=order_at, now=self.NOW)
        self.assertTrue(plan.send_confirmation)
        self.assertEqual(plan.ask_at, order_at - ASK_BEFORE)

    def test_urgent_order_gets_no_question(self):
        """Заказ меньше чем за сутки: только подтверждение, вопроса нет."""
        plan = plan_confirmation(order_at=self.NOW + timedelta(hours=5), now=self.NOW)
        self.assertTrue(plan.send_confirmation)
        self.assertIsNone(plan.ask_at)

    def test_order_exactly_a_day_ahead_gets_no_question(self):
        """Ровно сутки: спрашивать уже поздно — вопрос ушёл бы этой же секундой."""
        plan = plan_confirmation(order_at=self.NOW + ASK_BEFORE, now=self.NOW)
        self.assertTrue(plan.send_confirmation)
        self.assertIsNone(plan.ask_at)

    def test_order_without_date_gets_nothing(self):
        plan = plan_confirmation(order_at=None, now=self.NOW)
        self.assertFalse(plan.send_confirmation)
        self.assertIsNone(plan.ask_at)

    def test_past_order_gets_nothing(self):
        """Сделку могли оформить задним числом — писать про вчера незачем."""
        plan = plan_confirmation(order_at=self.NOW - timedelta(hours=1), now=self.NOW)
        self.assertFalse(plan.send_confirmation)

    def test_every_decision_explains_itself(self):
        """Причина попадает в журнал: без неё репетицию не проверить."""
        for order_at in (None, self.NOW - timedelta(hours=1),
                         self.NOW + timedelta(hours=5), self.NOW + timedelta(days=3)):
            self.assertTrue(plan_confirmation(order_at=order_at, now=self.NOW).reason)


class SilenceTests(unittest.TestCase):
    """Три часа тишины считаются от факта отправки вопроса.

    Плановое время сюда не годится: заказ на 8 сентября заводится 3 сентября,
    и по плановому времени робот позвал бы владельца к «молчанию» задолго до
    того, как вопрос вообще ушёл клиенту.
    """

    NOW = datetime(2026, 8, 28, 12, 0, tzinfo=MSK)

    def test_owner_is_called_after_three_hours(self):
        sent_at = self.NOW - SILENCE_LIMIT - timedelta(minutes=1)
        self.assertTrue(should_call_owner(asked_sent_at=sent_at, notified_at=None,
                                          now=self.NOW))

    def test_owner_is_not_called_too_early(self):
        sent_at = self.NOW - timedelta(hours=1)
        self.assertFalse(should_call_owner(asked_sent_at=sent_at, notified_at=None,
                                           now=self.NOW))

    def test_owner_is_called_only_once(self):
        sent_at = self.NOW - timedelta(hours=5)
        self.assertFalse(should_call_owner(asked_sent_at=sent_at,
                                           notified_at=self.NOW - timedelta(hours=1),
                                           now=self.NOW))

    def test_unasked_order_never_calls_owner(self):
        """Вопрос ещё не ушёл — молчания нет, звать не о чем."""
        self.assertFalse(should_call_owner(asked_sent_at=None, notified_at=None,
                                           now=self.NOW))


class ChildDealTests(unittest.TestCase):
    """Связь лид → сделка реализации. Проверено на живых данных 2026-08-28:
    робот владельца заводит сделку той же минутой, а amoCRM оставляет в лиде
    примечание `lead_auto_created` со ссылкой на неё."""

    def _note(self, deal_id, created_at=100):
        return {"note_type": "lead_auto_created", "created_at": created_at,
                "params": {"type": "child", "lead_type": "child",
                           "lead_id": deal_id, "link": {"id": deal_id, "type": 2}}}

    def test_finds_child_deal(self):
        self.assertEqual(child_deal_id([self._note(31570357)]), 31570357)

    def test_ignores_other_notes(self):
        notes = [{"note_type": "call_out", "params": {"duration": 9}},
                 {"note_type": "amomail_message", "params": {}},
                 self._note(31570357)]
        self.assertEqual(child_deal_id(notes), 31570357)

    def test_takes_the_freshest_link(self):
        """Заказ могли завести дважды — двигать надо последнюю сделку."""
        notes = [self._note(111, created_at=100), self._note(222, created_at=200)]
        self.assertEqual(child_deal_id(notes), 222)

    def test_no_link_means_no_guess(self):
        self.assertIsNone(child_deal_id([]))
        self.assertIsNone(child_deal_id([{"note_type": "call_in", "params": {}}]))


class PickRealizationDealTests(unittest.TestCase):
    """Запасной путь, когда ссылки в примечании нет: открытая сделка с той же
    датой работы."""

    def _deal(self, deal_id, order_at_raw, status_id=AMO_STAGE_ORDER_CREATED,
              pipeline_id=AMO_PIPELINE_REALIZATION):
        return {"id": deal_id, "pipeline_id": pipeline_id, "status_id": status_id,
                "custom_fields_values": [{"field_id": 18701,
                                          "values": [{"value": order_at_raw}]}]}

    def test_matches_by_order_datetime(self):
        deals = [self._deal(1, "1787000000"), self._deal(2, "1788174000")]
        self.assertEqual(pick_realization_deal(deals, order_at_raw="1788174000"), 2)

    def test_ignores_deals_that_moved_on(self):
        """Сделка уже подтверждена — значит это чужой, более ранний заказ."""
        deals = [self._deal(1, "1788174000", status_id=AMO_STAGE_CONFIRMED)]
        self.assertIsNone(pick_realization_deal(deals, order_at_raw="1788174000"))

    def test_ignores_other_pipelines(self):
        deals = [self._deal(1, "1788174000", pipeline_id=4482751)]
        self.assertIsNone(pick_realization_deal(deals, order_at_raw="1788174000"))

    def test_without_date_nothing_is_guessed(self):
        """Нет даты — нет признака. Пусть сделку подвинет человек."""
        deals = [self._deal(1, "1788174000")]
        self.assertIsNone(pick_realization_deal(deals, order_at_raw=None))

    def test_foreign_contact_is_skipped(self):
        deal = self._deal(1, "1788174000")
        deal["_embedded"] = {"contacts": [{"id": 999}]}
        self.assertIsNone(pick_realization_deal(deals=[deal],
                                                order_at_raw="1788174000",
                                                contact_ids={37926153}))


class OrderFromLeadTests(unittest.TestCase):
    def _lead(self, *fields):
        return {"custom_fields_values": list(fields)}

    def test_reads_datetime_and_address(self):
        order = order_from_lead(self._lead(
            {"field_id": 18701, "values": [{"value": 1788174000}]},
            {"field_id": 18639, "values": [{"value": "Панина 7к2"}]},
        ), tz=MSK)
        self.assertEqual(order.address, "Панина 7к2")
        self.assertEqual(order.order_at,
                         datetime.fromtimestamp(1788174000, tz=MSK))

    def test_missing_fields_are_empty(self):
        order = order_from_lead(self._lead(), tz=MSK)
        self.assertIsNone(order.order_at)
        self.assertIsNone(order.address)

    def test_broken_datetime_does_not_crash(self):
        """Поле могли заполнить руками: письмо не уйдёт, но проход не упадёт."""
        order = order_from_lead(self._lead(
            {"field_id": 18701, "values": [{"value": "завтра днём"}]},
        ), tz=MSK)
        self.assertIsNone(order.order_at)

    def test_deal_details_win_over_the_lead(self):
        """Адрес берём из сделки: 2026-08-28 в лиде вместо адреса стояло слово
        «адрес», и клиент получил письмо с ним."""
        lead = order_from_lead(self._lead(
            {"field_id": 18701, "values": [{"value": 1788015600}]},
            {"field_id": 18639, "values": [{"value": "адрес"}]},
        ), tz=MSK)
        deal = order_from_lead(self._lead(
            {"field_id": 18701, "values": [{"value": 1788015600}]},
            {"field_id": 18639, "values": [{"value": "Академика Сахарова 109к2"}]},
        ), tz=MSK)
        merged = prefer_deal_details(deal, lead)
        self.assertEqual(merged.address, "Академика Сахарова 109к2")

    def test_lead_fills_what_the_deal_lacks(self):
        """Сделки нет или поле в ней пустое — письмо всё равно уходит."""
        lead = order_from_lead(self._lead(
            {"field_id": 18701, "values": [{"value": 1788015600}]},
            {"field_id": 18639, "values": [{"value": "Панина 7к2"}]},
        ), tz=MSK)
        merged = prefer_deal_details(order_from_lead(self._lead(), tz=MSK), lead)
        self.assertEqual(merged.address, "Панина 7к2")
        self.assertEqual(merged.order_at, lead.order_at)

    def test_letter_payload_is_human_readable(self):
        payload = letter_payload(
            order_at=datetime(2026, 8, 31, 14, 0, tzinfo=MSK),
            address="Панина 7к2")
        self.assertEqual(payload["date"], "31.08.2026")
        self.assertEqual(payload["time"], "14:00")
        self.assertEqual(payload["address"], "Панина 7к2")


class DecideOnAnswerTests(unittest.TestCase):
    """Ответом считается только то, что пришло ПОСЛЕ отправленного вопроса.

    Живой случай 2026-09-03: клиент написал по своему делу («нам нужен акт»)
    за пять дней до того, как робот собирался задать вопрос, — и робот принял
    это за ответ. Напиши клиент «да, спасибо», сделка уехала бы в «Заказ
    подтвержден» без всякого вопроса.
    """

    SENT_AT = datetime(2026, 9, 3, 10, 0, tzinfo=MSK)

    def test_yes_confirms_the_order(self):
        self.assertEqual(decide_on_answer(answer="yes", status="planned",
                                          asked_sent_at=self.SENT_AT), "confirm")

    def test_late_yes_still_confirms(self):
        """Решение владельца 7: «Да» после сигнала всё равно двигает сделку."""
        self.assertEqual(decide_on_answer(answer="yes", status="owner_notified",
                                          asked_sent_at=self.SENT_AT), "confirm")

    def test_no_calls_owner(self):
        self.assertEqual(decide_on_answer(answer="no", status="planned",
                                          asked_sent_at=self.SENT_AT), "call_owner")

    def test_unclear_calls_owner(self):
        self.assertEqual(decide_on_answer(answer="unclear", status="planned",
                                          asked_sent_at=self.SENT_AT), "call_owner")

    def test_owner_is_not_called_twice_about_one_order(self):
        """Владельца уже позвали — дальше он разговаривает сам."""
        for answer in ("no", "unclear"):
            self.assertEqual(decide_on_answer(answer=answer, status="owner_notified",
                                              asked_sent_at=self.SENT_AT),
                             "ignore", answer)

    def test_settled_order_ignores_anything(self):
        for status in ("confirmed", "refused"):
            self.assertEqual(decide_on_answer(answer="yes", status=status,
                                              asked_sent_at=self.SENT_AT),
                             "ignore", status)

    def test_nothing_counts_before_the_question_was_sent(self):
        """Решение владельца 1: нет вопроса — нет ожидания. Никогда.

        Ни «да», ни «нет», ни непонятное: пока письмо не ушло, клиент отвечает
        не нам, а по своему делу — такие письма владелец читает в Wahelp.
        """
        for answer in ("yes", "no", "unclear"):
            for status in ("planned", "owner_notified"):
                self.assertEqual(
                    decide_on_answer(answer=answer, status=status, asked_sent_at=None),
                    "ignore", f"{answer}/{status}")


class QuestionGuardTests(unittest.TestCase):
    """Проверка за секунду до вопроса: заказ ещё жив?

    Между планированием и отправкой проходят сутки. За эти сутки сделку могли
    удалить (так и случилось 2026-09-03), отменить или довести до конца —
    решение владельца 2: по закрытой сделке вопросов не задаём.
    """

    def _deal(self, **over):
        deal = {"id": 31583273, "pipeline_id": AMO_PIPELINE_REALIZATION,
                "status_id": AMO_STAGE_ORDER_CREATED}
        deal.update(over)
        return deal

    def test_live_order_gets_its_question(self):
        allowed, reason = may_ask_question(self._deal())
        self.assertTrue(allowed)
        self.assertEqual(reason, "")

    def test_missing_deal_stops_the_question(self):
        """Сделку удалили — спрашивать не о чем и некуда двигать."""
        allowed, reason = may_ask_question(None)
        self.assertFalse(allowed)
        self.assertTrue(reason)

    def test_empty_answer_from_crm_is_a_missing_deal(self):
        """На удалённую сделку amoCRM отвечает пустым телом, а не ошибкой.

        Прочитай робот такой ответ как «сделка не в той воронке» — причина
        в журнале была бы враньём, и разбор следующего случая ушёл бы не туда.
        """
        allowed, reason = may_ask_question({"text": ""})
        self.assertFalse(allowed)
        self.assertEqual(reason, "заказа больше нет в CRM")

    def test_deal_that_left_order_created_stops_the_question(self):
        allowed, reason = may_ask_question(self._deal(status_id=AMO_STAGE_CONFIRMED))
        self.assertFalse(allowed)
        self.assertTrue(reason)

    def test_deal_that_left_the_realization_pipeline_stops_the_question(self):
        allowed, reason = may_ask_question(self._deal(pipeline_id=1))
        self.assertFalse(allowed)
        self.assertTrue(reason)

    def test_every_refusal_explains_itself(self):
        """Причина уходит в отмену письма: без неё потом не разобрать, что было."""
        for deal in (None, self._deal(status_id=AMO_STAGE_CONFIRMED),
                     self._deal(pipeline_id=1)):
            self.assertTrue(may_ask_question(deal)[1])

    def test_only_the_question_is_checked(self):
        """Письмо «заказ принят» проверкой не затрагивается: оно уходит сразу
        при оформлении, проверять там нечего."""
        self.assertTrue(is_question_letter(CONFIRM_REQUEST_EVENT))
        for event_key in ("order_created", "order_confirmed_thanks", "birthday_congrats"):
            self.assertFalse(is_question_letter(event_key), event_key)


class MoveDealTests(unittest.TestCase):
    def test_deal_moves_only_from_order_created(self):
        self.assertTrue(should_move_deal(AMO_STAGE_ORDER_CREATED))

    def test_deal_that_went_further_is_not_touched(self):
        """Сделку мог двинуть человек: робот не откатывает чужую работу."""
        self.assertFalse(should_move_deal(AMO_STAGE_CONFIRMED))
        self.assertFalse(should_move_deal(142))
        self.assertFalse(should_move_deal(None))


class OwnerAlertTests(unittest.TestCase):
    ORDER_AT = datetime(2026, 8, 31, 14, 0, tzinfo=MSK)

    def _text(self, kind, answer_text="перенесите на среду"):
        return owner_alert_text(
            kind=kind,
            name="Дарья",
            phone="+79001234567",
            order_at=self.ORDER_AT,
            answer_text=answer_text,
            lead_link="https://example.amocrm.ru/leads/detail/123",
        )

    def test_owner_sees_full_phone(self):
        """Бот приватный, владелец — единственный получатель, и ему нужно
        позвонить клиенту, не открывая CRM (его решение 2026-08-26)."""
        self.assertIn("+79001234567", self._text("refused"))

    def test_owner_sees_answer_in_full(self):
        """Непонятое не пересказываем: владелец читает слова клиента сам."""
        text = self._text("unclear", answer_text="а можно перенести на среду вечером?")
        self.assertIn("а можно перенести на среду вечером?", text)

    def test_owner_sees_order_date_and_link(self):
        text = self._text("refused")
        self.assertIn("31.08.2026", text)
        self.assertIn("14:00", text)
        self.assertIn("https://example.amocrm.ru/leads/detail/123", text)

    def test_silence_alert_says_what_happened(self):
        text = owner_alert_text(kind="silence", name="Дарья", phone="+79001234567",
                                order_at=self.ORDER_AT, answer_text=None,
                                lead_link=None)
        self.assertIn("не ответил", text.lower())
        self.assertIn("+79001234567", text)

    def test_unasked_alert_says_the_question_never_went_out(self):
        """Разница со «сторожем молчунов» принципиальная: там клиент молчит,
        здесь его никто и не спрашивал. Спутай робот эти две беды — владелец
        стал бы звонить клиенту с претензией на пустом месте."""
        text = owner_alert_text(kind="not_asked", name="Дарья",
                                phone="+79001234567", order_at=self.ORDER_AT,
                                answer_text=None, lead_link=None,
                                reason="amoCRM не ответила")
        self.assertIn("не ушёл", text)
        self.assertIn("amoCRM не ответила", text)
        self.assertIn("+79001234567", text)

    def test_kinds_are_distinguishable(self):
        """Владелец должен с первой строки понимать, что случилось."""
        heads = {self._text(kind).splitlines()[0]
                 for kind in ("refused", "unclear", "silence", "not_asked")}
        self.assertEqual(len(heads), 4)


class UnaskedQuestionTests(unittest.TestCase):
    """Письмо-вопрос может умереть, так и не уйдя. Раньше владелец узнавал
    об этом через сторож молчунов — с враньём в тексте («клиент не ответил»),
    хотя клиент вопроса и не получал. Теперь отдельный сигнал, и говорит правду."""

    def _call(self, **over):
        args = {"pending_status": "planned", "letter_status": "failed",
                "asked_sent_at": None, "notified_at": None}
        args.update(over)
        return should_report_unasked(**args)

    def test_dead_letter_calls_the_owner(self):
        self.assertTrue(self._call(letter_status="failed"))

    def test_cancelled_letter_calls_the_owner_too(self):
        """Живой случай 2026-09-03: письмо отменено ещё утром («клиент не писал
        нам первым»), работа завтра, клиент ни о чём не знает. Молчать нельзя."""
        self.assertTrue(self._call(letter_status="cancelled"))

    def test_letter_still_trying_is_not_a_reason_to_call(self):
        for status in ("pending", "sending", "sent"):
            self.assertFalse(self._call(letter_status=status), status)

    def test_deliberate_drop_is_not_a_reason_to_call(self):
        """Робот сам отменил вопрос, потому что заказа больше нет в CRM.
        Это несостоявшееся событие, а не беда: владельца не тревожим."""
        self.assertFalse(self._call(pending_status="dropped",
                                    letter_status="cancelled"))

    def test_sent_question_is_not_this_branch(self):
        self.assertFalse(self._call(
            asked_sent_at=datetime(2026, 9, 7, 10, 0, tzinfo=MSK)))

    def test_owner_is_called_only_once(self):
        self.assertFalse(self._call(
            notified_at=datetime(2026, 9, 7, 10, 0, tzinfo=MSK)))


class LetterFailureWordsTests(unittest.TestCase):
    """Причину владелец читает глазами, а пишет её работник очереди —
    по-английски и своими словами. Между ними нужен перевод."""

    def test_technical_reason_becomes_human(self):
        text = letter_failure_words("wahelp requires connection")
        self.assertIn("не писал нам первым", text)

    def test_unknown_reason_is_shown_as_is(self):
        """Незнакомую причину не проглатываем: полуправда хуже английского."""
        self.assertEqual(letter_failure_words("amoCRM не ответила"),
                         "amoCRM не ответила")

    def test_missing_reason_still_says_something(self):
        for value in (None, "", "   "):
            self.assertTrue(letter_failure_words(value))


class LetterTextsTests(unittest.TestCase):
    """Ключи писем, которые бот ставит в очередь, должны существовать в файле
    текстов. Разойдись они — робот падал бы прямо в разговоре с клиентом,
    и узнали бы мы об этом от клиента, а не от тестов."""

    KEYS = ("order_created", "order_confirm_request", "order_confirmed_thanks")

    def test_all_letters_exist(self):
        from notifications.rules import load_notification_rules

        rules = load_notification_rules(
            Path(__file__).resolve().parent.parent / "docs" / "notification_rules.json")
        for key in self.KEYS:
            event = rules.get_event(key)
            self.assertTrue(event.template.strip(), key)
            self.assertEqual(event.recipient, "client", key)

    def test_confirmation_letter_asks_for_one_word(self):
        """Разбор ответа читает первое слово — письмо обязано об этом просить."""
        from notifications.rules import load_notification_rules

        rules = load_notification_rules(
            Path(__file__).resolve().parent.parent / "docs" / "notification_rules.json")
        template = rules.get_event("order_confirm_request").template.lower()
        self.assertIn("одним словом", template)
        self.assertIn("да/нет", template)


if __name__ == "__main__":
    unittest.main()


class TakeOrderRules(unittest.TestCase):
    """Брать ли заказ в работу.

    Проверка воронки задумывалась с самого начала — в модуле так и написано:
    «воронку проверяем всегда, без этого робот написал бы клиентам чужих
    сделок». Но дорог к дочерней сделке две (примечание в лиде и поиск по
    времени), а забор стоял только на второй. 2026-08-31 заказ по коврам
    прошёл по первой.
    """

    LEAD = {"id": 31581307, "pipeline_id": 4482751}

    def _deal(self, pipeline_id, **extra):
        return {"id": 31585339, "pipeline_id": pipeline_id, **extra}

    def _flagged(self, entity):
        """Та же карточка, но с галочкой «веду сам»."""
        return {**entity, "custom_fields_values": [
            {"field_id": cm.AMO_FIELD_OWNER_HANDLES, "values": [{"value": "1"}]}]}

    def test_cleaning_order_is_taken(self):
        verdict, _ = cm.decide_on_order(
            lead=self.LEAD, deal=self._deal(AMO_PIPELINE_REALIZATION))
        self.assertEqual(verdict, "take")

    def test_carpet_order_is_skipped(self):
        """Живой случай: сейлзбот увёл сделку в «Ковры Кристал» (4645519)."""
        verdict, reason = cm.decide_on_order(
            lead=self.LEAD, deal=self._deal(4645519))
        self.assertEqual(verdict, "skip")
        self.assertEqual(reason, "заказ не нашей воронки")

    def test_missing_deal_waits_instead_of_writing_blind(self):
        """Сделки нет — не «не наш заказ», а «сейлзбот ещё не успел»."""
        verdict, reason = cm.decide_on_order(lead=self.LEAD, deal=None)
        self.assertEqual(verdict, "wait")
        self.assertIn("ещё нет", reason)

    def test_owner_flag_on_deal_stops_the_robot(self):
        with mock.patch.object(cm, "AMO_FIELD_OWNER_HANDLES", 999001):
            verdict, reason = cm.decide_on_order(
                lead=self.LEAD,
                deal=self._flagged(self._deal(AMO_PIPELINE_REALIZATION)))
        self.assertEqual(verdict, "skip")
        self.assertEqual(reason, "владелец ведёт заказ сам")

    def test_owner_flag_on_lead_stops_the_robot_before_waiting(self):
        """Сказано «веду сам» — робот не ждёт сделку, а уходит совсем."""
        with mock.patch.object(cm, "AMO_FIELD_OWNER_HANDLES", 999001):
            verdict, reason = cm.decide_on_order(
                lead=self._flagged(self.LEAD), deal=None)
        self.assertEqual(verdict, "skip")
        self.assertEqual(reason, "владелец ведёт заказ сам")


class OwnerHandlesFlag(unittest.TestCase):
    """Чтение галочки «заказ ведёт владелец» из карточки amoCRM."""

    FIELD = 999001

    def _card(self, value):
        return {"custom_fields_values": [
            {"field_id": self.FIELD, "values": [{"value": value}]}]}

    def test_checked_box_is_read(self):
        with mock.patch.object(cm, "AMO_FIELD_OWNER_HANDLES", self.FIELD):
            self.assertTrue(cm.owner_handles(self._card(True)))
            self.assertTrue(cm.owner_handles(self._card("1")))

    def test_unchecked_box_is_not_read(self):
        """amoCRM отдаёт снятую галочку пустым значением, но «0» тоже бывает."""
        with mock.patch.object(cm, "AMO_FIELD_OWNER_HANDLES", self.FIELD):
            self.assertFalse(cm.owner_handles(self._card("0")))
            self.assertFalse(cm.owner_handles(self._card(False)))
            self.assertFalse(cm.owner_handles({"custom_fields_values": []}))
            self.assertFalse(cm.owner_handles(None))

    def test_field_not_created_yet_means_no_flag(self):
        """Пока номер поля не проставлен, метки нет — а не «есть у всех»."""
        with mock.patch.object(cm, "AMO_FIELD_OWNER_HANDLES", 0):
            self.assertFalse(cm.owner_handles(self._card("1")))

    def test_other_field_is_not_mistaken_for_the_flag(self):
        with mock.patch.object(cm, "AMO_FIELD_OWNER_HANDLES", self.FIELD):
            self.assertFalse(cm.owner_handles(
                {"custom_fields_values": [
                    {"field_id": 18639, "values": [{"value": "Ленина 1"}]}]}))


class WaitingForTheChildDeal(unittest.TestCase):
    """Сколько ждать сделку, прежде чем сдаться."""

    NOW = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)

    def test_fresh_event_keeps_waiting(self):
        self.assertTrue(cm.should_keep_waiting(
            event_at=self.NOW - timedelta(minutes=10), now=self.NOW))

    def test_old_event_gives_up(self):
        self.assertFalse(cm.should_keep_waiting(
            event_at=self.NOW - cm.DEAL_WAIT_LIMIT - timedelta(minutes=1),
            now=self.NOW))

    def test_boundary_belongs_to_giving_up(self):
        """Ровно час — уже сдаёмся: иначе граница зависит от секунды прохода."""
        self.assertFalse(cm.should_keep_waiting(
            event_at=self.NOW - cm.DEAL_WAIT_LIMIT, now=self.NOW))
