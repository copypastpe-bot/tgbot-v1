import unittest

from notifications.amocrm_api import (
    AmoCRMAPIAuthError,
    AmoCRMAPIClient,
    AmoCRMAPIRateLimitError,
    AmoCRMAlert,
    AmoCRMLead,
    build_lead_link,
    build_new_lead_alert,
    build_unanswered_message_alert,
    build_unsorted_alert,
    extract_contact_phone,
    extract_event_identity,
    extract_event_message_id,
    extract_event_type,
    extract_lead_contact_ids,
    format_amocrm_api_alert,
    is_accepted_call_note,
    normalize_lead,
    should_skip_new_lead_alert,
)


class AmoCRMApiExtractionTests(unittest.TestCase):
    def test_extracts_contact_phone_from_custom_fields(self):
        contact = {
            "id": 10,
            "name": "Иван",
            "custom_fields_values": [
                {
                    "field_code": "PHONE",
                    "values": [{"value": "+7 999 123-45-67", "enum_code": "WORK"}],
                }
            ],
        }

        self.assertEqual(extract_contact_phone(contact), "+7 999 123-45-67")

    def test_extracts_lead_contact_ids_from_embedded_contacts(self):
        lead = {
            "id": 123,
            "_embedded": {
                "contacts": [
                    {"id": 10, "is_main": True},
                    {"id": 11, "is_main": False},
                ]
            },
        }

        self.assertEqual(extract_lead_contact_ids(lead), [10, 11])

    def test_identifies_accepted_call_note(self):
        note = {
            "note_type": "call_in",
            "params": {
                "duration": 35,
                "phone": "+79991234567",
            },
        }

        self.assertTrue(is_accepted_call_note(note, min_duration_sec=20))

    def test_rejects_short_call_note(self):
        note = {"note_type": "call_in", "params": {"duration": 0}}

        self.assertFalse(is_accepted_call_note(note, min_duration_sec=20))

    def test_extracts_event_type_and_message_id(self):
        event = {
            "id": "ev-1",
            "type": "incoming_chat_message",
            "value_after": [
                {"message": {"id": "msg-1", "text": "Хочу уборку"}},
            ],
        }

        self.assertEqual(extract_event_type(event), "incoming_chat_message")
        self.assertEqual(extract_event_message_id(event), "msg-1")

    def test_builds_lead_link(self):
        self.assertEqual(
            build_lead_link("https://raketacleancrm.amocrm.ru", 123),
            "https://raketacleancrm.amocrm.ru/leads/detail/123",
        )


class FakeHTTPResponse:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        return str(self._payload)


class FakeHTTPSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, *, headers, params, timeout):
        self.calls.append((url, headers, params, timeout))
        return self.response


class AmoCRMApiClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_adds_bearer_token_and_base_url(self):
        session = FakeHTTPSession(FakeHTTPResponse(200, {"ok": True}))
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        result = await client.get("/api/v4/events", params={"limit": 1})

        self.assertEqual(result, {"ok": True})
        url, headers, params, timeout = session.calls[0]
        self.assertEqual(url, "https://example.amocrm.ru/api/v4/events")
        self.assertEqual(headers["Authorization"], "Bearer token")
        self.assertEqual(params, {"limit": 1})

    async def test_get_raises_auth_error_on_401(self):
        session = FakeHTTPSession(FakeHTTPResponse(401, {"detail": "bad token"}))
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        with self.assertRaises(AmoCRMAPIAuthError):
            await client.get("/api/v4/events")

    async def test_get_raises_rate_limit_error_on_429(self):
        session = FakeHTTPSession(FakeHTTPResponse(429, {"detail": "too many"}))
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        with self.assertRaises(AmoCRMAPIRateLimitError):
            await client.get("/api/v4/events")


class SequenceHTTPSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, headers, params, timeout):
        self.calls.append((url, params))
        return self.responses.pop(0)


class AmoCRMFetchersTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_events_uses_type_and_created_at_filters(self):
        session = SequenceHTTPSession([
            FakeHTTPResponse(200, {"_embedded": {"events": [{"id": "ev-1", "type": "lead_added"}]}})
        ])
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        events = await client.fetch_events(event_types=["lead_added"], created_from=100)

        self.assertEqual(events[0]["id"], "ev-1")
        url, params = session.calls[0]
        self.assertEqual(url, "https://example.amocrm.ru/api/v4/events")
        self.assertEqual(params["filter[type]"], "lead_added")
        self.assertEqual(params["filter[created_at][from]"], 100)

    async def test_fetch_lead_requests_contacts(self):
        session = SequenceHTTPSession([
            FakeHTTPResponse(200, {"id": 123, "_embedded": {"contacts": [{"id": 10}]}})
        ])
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        lead = await client.fetch_lead(123)

        self.assertEqual(lead["id"], 123)
        url, params = session.calls[0]
        self.assertEqual(url, "https://example.amocrm.ru/api/v4/leads/123")
        self.assertEqual(params["with"], "contacts")

    async def test_fetch_unsorted_filters_pipeline_and_created_from(self):
        session = SequenceHTTPSession([
            FakeHTTPResponse(200, {"_embedded": {"unsorted": [{"uid": "u-1"}]}})
        ])
        client = AmoCRMAPIClient("https://example.amocrm.ru", "token", session=session)

        items = await client.fetch_unsorted(pipeline_id=55, created_from=100)

        self.assertEqual(items[0]["uid"], "u-1")
        url, params = session.calls[0]
        self.assertEqual(url, "https://example.amocrm.ru/api/v4/leads/unsorted")
        self.assertEqual(params["filter[pipeline_id]"], 55)
        self.assertEqual(params["filter[created_at][from]"], 100)


class AmoCRMAlertRulesTests(unittest.TestCase):
    def test_skips_new_lead_when_new_lead_status_and_accepted_call(self):
        lead = AmoCRMLead(
            lead_id=123,
            name="Входящий звонок",
            pipeline_id=55,
            status_id=777,
            created_at=100,
            contact_ids=[10],
            payload={},
        )
        notes = [{"note_type": "call_in", "params": {"duration": 30}}]

        self.assertTrue(
            should_skip_new_lead_alert(
                lead,
                target_pipeline_id=55,
                new_lead_status_id=777,
                notes=notes,
            )
        )

    def test_skips_new_lead_when_new_lead_status_and_short_call(self):
        lead = AmoCRMLead(
            lead_id=123,
            name="Входящий звонок",
            pipeline_id=55,
            status_id=777,
            created_at=100,
            contact_ids=[10],
            payload={},
        )
        notes = [{"note_type": "call_in", "params": {"duration": 0}}]

        self.assertTrue(
            should_skip_new_lead_alert(
                lead,
                target_pipeline_id=55,
                new_lead_status_id=777,
                notes=notes,
            )
        )

    def test_does_not_skip_non_call_new_lead(self):
        lead = AmoCRMLead(
            lead_id=123,
            name="Заявка с сайта",
            pipeline_id=55,
            status_id=777,
            created_at=100,
            contact_ids=[10],
            payload={},
        )

        self.assertFalse(
            should_skip_new_lead_alert(
                lead,
                target_pipeline_id=55,
                new_lead_status_id=777,
                notes=[],
            )
        )

    def test_formats_unanswered_message_alert(self):
        alert = AmoCRMAlert(
            alert_type="unanswered_message",
            title="Открытая сделка",
            lead_id=123,
            contact_id=10,
            contact_name="Иван",
            phone="+79991234567",
            source="WhatsApp",
            text="Хочу уборку",
            comment=None,
            link="https://example.amocrm.ru/leads/detail/123",
        )

        text = format_amocrm_api_alert(alert)

        self.assertIn("Тип: новое входящее сообщение без ответа 10 минут", text)
        self.assertIn("Сделка: #123", text)
        self.assertIn("Клиент: Иван", text)
        self.assertIn("Телефон: +79991234567", text)
        self.assertIn("Текст: Хочу уборку", text)


class AmoCRMPollingAlertBuildTests(unittest.TestCase):
    def test_normalize_lead_keeps_pipeline_status_and_contacts(self):
        lead = normalize_lead(
            {
                "id": 123,
                "name": "Заявка с сайта",
                "pipeline_id": 55,
                "status_id": 777,
                "created_at": 1710000000,
                "_embedded": {"contacts": [{"id": 10}]},
            }
        )

        self.assertEqual(lead.lead_id, 123)
        self.assertEqual(lead.pipeline_id, 55)
        self.assertEqual(lead.status_id, 777)
        self.assertEqual(lead.contact_ids, [10])

    def test_build_new_lead_alert_uses_contact_phone(self):
        lead = normalize_lead(
            {
                "id": 123,
                "name": "Заявка с сайта",
                "pipeline_id": 55,
                "status_id": 777,
                "_embedded": {"contacts": [{"id": 10}]},
            }
        )
        contact = {
            "id": 10,
            "name": "Иван",
            "custom_fields_values": [
                {"field_code": "PHONE", "values": [{"value": "+79991234567"}]},
            ],
        }

        alert = build_new_lead_alert(lead, contact=contact, api_base="https://example.amocrm.ru")

        self.assertEqual(alert.alert_type, "new_lead")
        self.assertEqual(alert.lead_id, 123)
        self.assertEqual(alert.contact_name, "Иван")
        self.assertEqual(alert.phone, "+79991234567")

    def test_build_unsorted_alert_uses_uid_and_phone(self):
        item = {
            "uid": "u-1",
            "pipeline_id": 55,
            "source_name": "SIP",
            "_embedded": {"leads": [{"id": 123}]},
            "metadata": {"phone": "+79991234567"},
        }

        alert = build_unsorted_alert(item, api_base="https://example.amocrm.ru")

        self.assertEqual(alert.alert_type, "new_unsorted")
        self.assertEqual(alert.lead_id, 123)
        self.assertEqual(alert.phone, "+79991234567")
        self.assertEqual(alert.source, "SIP")


class AmoCRMIncomingMessageTests(unittest.TestCase):
    def test_extract_event_identity_from_chat_event(self):
        event = {
            "id": "ev-1",
            "type": "incoming_chat_message",
            "entity_id": 10,
            "entity_type": "contact",
            "created_at": 1710000000,
            "value_after": [
                {
                    "message": {"id": "msg-1", "text": "Хочу уборку"},
                    "talk": {"id": "talk-1"},
                    "lead": {"id": 123},
                    "contact": {"id": 10},
                }
            ],
        }

        identity = extract_event_identity(event)

        self.assertEqual(identity["event_id"], "ev-1")
        self.assertEqual(identity["message_id"], "msg-1")
        self.assertEqual(identity["lead_id"], 123)
        self.assertEqual(identity["contact_id"], 10)
        self.assertEqual(identity["talk_id"], "talk-1")
        self.assertEqual(identity["text"], "Хочу уборку")

    def test_build_unanswered_message_alert(self):
        alert = build_unanswered_message_alert(
            lead={"id": 123, "name": "Открытая сделка"},
            contact={
                "id": 10,
                "name": "Иван",
                "custom_fields_values": [{"field_code": "PHONE", "values": [{"value": "+79991234567"}]}],
            },
            text="Хочу уборку",
            api_base="https://example.amocrm.ru",
        )

        self.assertEqual(alert.alert_type, "unanswered_message")
        self.assertEqual(alert.lead_id, 123)
        self.assertEqual(alert.contact_name, "Иван")
        self.assertEqual(alert.phone, "+79991234567")


if __name__ == "__main__":
    unittest.main()
