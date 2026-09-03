"""Точка подключения проверки перед отправкой письма.

Работник очереди — тупой курьер: он умеет только доставлять письма и ничего
не знает ни про amoCRM, ни про ожидания подтверждения. Но письмо-вопрос нельзя
отправлять вслепую: между планированием и отправкой проходят сутки, и за эти
сутки заказ мог исчезнуть.

Поэтому у курьера есть две точки подключения, которые заполняет `bot.py`:

* `before_send` — «можно ли ещё слать?». Три ответа: слать, отменить письмо
  (заказа больше нет) и отложить (CRM не ответила — это не повод молчать
  навсегда).
* `after_send` — «письмо ушло». По нему в ожидании появляется факт отправки,
  и только с этого момента ответ клиента считается ответом.

Главное, что здесь проверяется: **отметка об отправке ставится в той же
транзакции, что и сама отметка «письмо отправлено».** Разойдись они — робот
получил бы отправленный вопрос, про который не помнит, что задавал его.
"""

import unittest
from unittest import mock

from notifications.outbox import PRE_SEND_OK, NotificationOutboxEntry, PreSendVerdict
from notifications.worker import NotificationWorker


class FakeTransaction:
    def __init__(self, log):
        self.log = log

    async def __aenter__(self):
        self.log.append("tx:begin")
        return self

    async def __aexit__(self, *exc):
        self.log.append("tx:commit")
        return False


class FakeConn:
    def __init__(self, log):
        self.log = log

    def transaction(self):
        return FakeTransaction(self.log)

    async def execute(self, *args, **kwargs):
        return None


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *exc):
        return False


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


class FakeSendResult:
    channel = "clients_wa"
    response = {"data": {"id": "msg-1"}}


def make_entry(event_key="order_confirm_request"):
    return NotificationOutboxEntry(
        id=77,
        event_key=event_key,
        recipient_kind="clients",
        client_id=1268,
        template="Подтвердите заказ на {{date}}",
        payload={"date": "08.09.2026"},
        locale="ru-RU",
        scheduled_at=None,
        attempts=1,
        client_phone="79990000000",
        client_name="Анна",
        client_preferred_channel=None,
        client_user_id_wa=None,
        client_user_id_tg=None,
        client_user_id_max=None,
        client_requires_connection=False,
        notifications_enabled=True,
    )


class BeforeSendTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.log = []
        self.pool = FakePool(FakeConn(self.log))
        self.entry = make_entry()

    def _worker(self, **kwargs):
        return NotificationWorker(self.pool, rules=None, **kwargs)

    async def test_letter_is_sent_when_nothing_objects(self):
        async def allow(entry):
            return PRE_SEND_OK

        worker = self._worker(before_send=allow)
        with mock.patch("notifications.worker.send_with_rules") as send, \
                mock.patch("notifications.worker.mark_outbox_sent") as sent:
            send.return_value = FakeSendResult()
            await worker._handle_entry(self.entry)
        self.assertTrue(send.called)
        self.assertTrue(sent.called)

    async def test_cancelled_letter_never_reaches_the_client(self):
        """Заказа больше нет — вопрос не уходит, письмо отменяется с причиной."""
        async def refuse(entry):
            return PreSendVerdict("cancel", "заказа больше нет в CRM")

        worker = self._worker(before_send=refuse)
        with mock.patch("notifications.worker.send_with_rules") as send, \
                mock.patch("notifications.worker.cancel_outbox_entry") as cancel:
            await worker._handle_entry(self.entry)
        self.assertFalse(send.called)
        self.assertTrue(cancel.called)
        self.assertEqual(cancel.call_args.args[2], "заказа больше нет в CRM")

    async def test_unreachable_crm_only_postpones_the_letter(self):
        """CRM не ответила — это не «заказа нет». Письмо ждёт следующей попытки,
        иначе сетевая заминка навсегда съедала бы живые вопросы."""
        async def postpone(entry):
            return PreSendVerdict("retry", "amoCRM не ответила")

        worker = self._worker(before_send=postpone)
        with mock.patch("notifications.worker.send_with_rules") as send, \
                mock.patch("notifications.worker.cancel_outbox_entry") as cancel, \
                mock.patch("notifications.worker.mark_outbox_failure") as failure:
            await worker._handle_entry(self.entry)
        self.assertFalse(send.called)
        self.assertFalse(cancel.called)
        self.assertTrue(failure.called)

    async def test_postponed_letter_waits_longer_than_a_usual_retry(self):
        """Попыток всего пять. Тратить их за полчаса на одну и ту же недоступную
        CRM бессмысленно: живой вопрос сгорел бы, не дождавшись её возвращения."""
        async def postpone(entry):
            return PreSendVerdict("retry", "amoCRM не ответила")

        worker = self._worker(before_send=postpone, precheck_retry_minutes=15)
        with mock.patch("notifications.worker.send_with_rules"), \
                mock.patch("notifications.worker.mark_outbox_failure") as failure:
            await worker._handle_entry(self.entry)
        self.assertEqual(failure.call_args.kwargs["retry_delay_minutes"], 15)

    async def test_worker_without_hooks_works_as_before(self):
        worker = self._worker()
        with mock.patch("notifications.worker.send_with_rules") as send, \
                mock.patch("notifications.worker.mark_outbox_sent") as sent:
            send.return_value = FakeSendResult()
            await worker._handle_entry(self.entry)
        self.assertTrue(send.called)
        self.assertTrue(sent.called)


class AfterSendTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.log = []
        self.pool = FakePool(FakeConn(self.log))
        self.entry = make_entry()

    async def test_fact_of_sending_is_recorded_in_the_same_transaction(self):
        seen = []

        async def after(conn, entry):
            self.log.append("after_send")
            seen.append(entry)

        worker = NotificationWorker(self.pool, rules=None, after_send=after)
        with mock.patch("notifications.worker.send_with_rules") as send, \
                mock.patch("notifications.worker.mark_outbox_sent") as sent:
            send.return_value = FakeSendResult()
            sent.side_effect = lambda *a, **kw: self.log.append("mark_sent")
            await worker._handle_entry(self.entry)
        self.assertEqual(seen, [self.entry])
        self.assertEqual(self.log, ["tx:begin", "mark_sent", "after_send", "tx:commit"])

    async def test_nothing_is_recorded_when_sending_failed(self):
        called = []

        async def after(conn, entry):
            called.append(entry)

        worker = NotificationWorker(self.pool, rules=None, after_send=after)
        with mock.patch("notifications.worker.send_with_rules") as send, \
                mock.patch("notifications.worker.mark_outbox_failure"):
            send.side_effect = RuntimeError("wahelp упал")
            await worker._handle_entry(self.entry)
        self.assertEqual(called, [])


if __name__ == "__main__":
    unittest.main()
