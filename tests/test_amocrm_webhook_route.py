import unittest

from notifications.webhook import WahelpWebhookServer


class FakeRelUrl:
    def __init__(self, query):
        self.query = query


class FakeRequest:
    def __init__(self, *, query=None, headers=None, form=None, content_type="application/x-www-form-urlencoded"):
        self.rel_url = FakeRelUrl(query or {})
        self.headers = headers or {}
        self._form = form or {}
        self.content_type = content_type

    async def post(self):
        return self._form

    async def json(self):
        return self._form


class AmoCRMWebhookRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_when_amocrm_token_is_not_configured(self):
        seen = []

        async def handler(payload):
            seen.append(payload)
            return True

        server = WahelpWebhookServer(
            pool=None,
            amocrm_token=None,
            amocrm_handler=handler,
        )

        resp = await server._handle_amocrm(
            FakeRequest(
                query={"token": "anything"},
                form={"leads[add][0][id]": "123"},
            )
        )

        self.assertEqual(resp.status, 503)
        self.assertEqual(seen, [])

    async def test_rejects_invalid_amocrm_token(self):
        seen = []

        async def handler(payload):
            seen.append(payload)
            return True

        server = WahelpWebhookServer(
            pool=None,
            amocrm_token="secret",
            amocrm_handler=handler,
        )

        resp = await server._handle_amocrm(
            FakeRequest(
                query={"token": "wrong"},
                form={"leads[add][0][id]": "123"},
            )
        )

        self.assertEqual(resp.status, 401)
        self.assertEqual(seen, [])

    async def test_accepts_valid_amocrm_form_payload(self):
        seen = []

        async def handler(payload):
            seen.append(payload)
            return True

        server = WahelpWebhookServer(
            pool=None,
            amocrm_token="secret",
            amocrm_handler=handler,
        )

        resp = await server._handle_amocrm(
            FakeRequest(
                query={"token": "secret"},
                form={"leads[add][0][id]": "123"},
            )
        )

        self.assertEqual(resp.status, 200)
        self.assertEqual(seen, [{"leads[add][0][id]": "123"}])


if __name__ == "__main__":
    unittest.main()
