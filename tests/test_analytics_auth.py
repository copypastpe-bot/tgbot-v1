import unittest

from analytics_app.auth import (
    make_password_hash,
    sign_session,
    unsign_session,
    verify_password,
)


class AnalyticsAuthTests(unittest.TestCase):
    def test_password_hash_verifies_correct_password(self):
        password_hash = make_password_hash("secret-password", salt="fixedsalt", iterations=1000)

        self.assertTrue(verify_password("secret-password", password_hash))
        self.assertFalse(verify_password("wrong-password", password_hash))

    def test_invalid_hash_does_not_verify(self):
        self.assertFalse(verify_password("secret-password", "bad-hash"))

    def test_session_sign_and_unsign_round_trip(self):
        cookie = sign_session("owner", secret="session-secret", now=1_700_000_000)

        self.assertEqual(unsign_session(cookie, secret="session-secret", now=1_700_000_100), "owner")

    def test_session_rejects_wrong_secret_and_expired_cookie(self):
        cookie = sign_session("owner", secret="session-secret", now=1_700_000_000)

        self.assertIsNone(unsign_session(cookie, secret="wrong-secret", now=1_700_000_100))
        self.assertIsNone(unsign_session(cookie, secret="session-secret", now=1_700_100_000, max_age_seconds=3600))


if __name__ == "__main__":
    unittest.main()
