import unittest

from notifications.amocrm import format_amocrm_admin_alert, normalize_amocrm_payload


class AmoCRMWebhookTests(unittest.TestCase):
    def test_normalizes_added_lead(self):
        event = normalize_amocrm_payload(
            {
                "leads[add][0][id]": "123456",
                "leads[add][0][name]": "Уборка завтра",
                "leads[add][0][price]": "7500",
                "leads[add][0][responsible_user_id]": "111",
            }
        )

        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, "leads.add")
        self.assertEqual(event.entity_kind, "lead")
        self.assertEqual(event.entity_id, "123456")
        self.assertEqual(event.title, "Уборка завтра")
        self.assertEqual(event.amount, "7500")

    def test_normalizes_added_unsorted(self):
        event = normalize_amocrm_payload(
            {
                "unsorted[add][0][uid]": "abc-123",
                "unsorted[add][0][source_name]": "Сайт",
                "unsorted[add][0][data][name]": "Новая заявка",
                "unsorted[add][0][data][phone]": "+79991234567",
            }
        )

        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, "unsorted.add")
        self.assertEqual(event.entity_kind, "unsorted")
        self.assertEqual(event.entity_id, "abc-123")
        self.assertEqual(event.title, "Новая заявка")
        self.assertEqual(event.source, "Сайт")
        self.assertEqual(event.phone, "+79991234567")

    def test_normalizes_added_message(self):
        event = normalize_amocrm_payload(
            {
                "message[add][0][id]": "m-1",
                "message[add][0][talk_id]": "t-9",
                "message[add][0][entity_id]": "123456",
                "message[add][0][text]": "Хочу клининг",
                "message[add][0][author][name]": "Иван",
                "message[add][0][origin]": "telegram",
            }
        )

        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, "message.add")
        self.assertEqual(event.entity_kind, "message")
        self.assertEqual(event.entity_id, "m-1")
        self.assertEqual(event.lead_id, "123456")
        self.assertEqual(event.text, "Хочу клининг")
        self.assertEqual(event.contact_name, "Иван")
        self.assertEqual(event.source, "telegram")

    def test_normalizes_digital_pipeline_chat(self):
        event = normalize_amocrm_payload(
            {
                "leads[chat][0][id]": "123456",
                "leads[chat][0][message]": "Пропущенный входящий",
                "leads[chat][0][contact][name]": "Мария",
            }
        )

        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, "leads.chat")
        self.assertEqual(event.entity_kind, "chat")
        self.assertEqual(event.entity_id, "123456")
        self.assertEqual(event.lead_id, "123456")
        self.assertEqual(event.text, "Пропущенный входящий")
        self.assertEqual(event.contact_name, "Мария")

    def test_unknown_payload_is_not_supported_but_preserved(self):
        event = normalize_amocrm_payload({"contacts[update][0][id]": "42"})

        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, "unknown")
        self.assertEqual(event.entity_kind, None)
        self.assertEqual(event.payload["contacts[update][0][id]"], "42")

    def test_formats_operational_alert_with_link(self):
        event = normalize_amocrm_payload(
            {
                "message[add][0][id]": "m-1",
                "message[add][0][entity_id]": "123456",
                "message[add][0][text]": "Хочу уборку завтра",
                "message[add][0][author][name]": "Иван",
                "message[add][0][origin]": "whatsapp",
            }
        )

        text = format_amocrm_admin_alert(event, account_domain="rocket.amocrm.ru")

        self.assertIn("amoCRM: новая входящая заявка", text)
        self.assertIn("Тип: новое входящее сообщение", text)
        self.assertIn("Сделка: #123456", text)
        self.assertIn("Клиент: Иван", text)
        self.assertIn("Источник: whatsapp", text)
        self.assertIn("Текст: Хочу уборку завтра", text)
        self.assertIn("https://rocket.amocrm.ru/leads/detail/123456", text)
        self.assertIn("Нужно ответить или позвонить.", text)


if __name__ == "__main__":
    unittest.main()
