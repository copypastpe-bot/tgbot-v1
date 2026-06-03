import unittest

from analytics_app.config import AnalyticsConfig, load_config


class AnalyticsConfigTests(unittest.TestCase):
    def test_load_config_uses_analytics_dsn_over_bot_dsn(self):
        env = {
            "DB_DSN": "postgres://bot",
            "ANALYTICS_DB_DSN": "postgres://analytics",
            "ANALYTICS_SESSION_SECRET": "secret",
            "ANALYTICS_USERS": "owner:hash1,partner:hash2",
        }

        cfg = load_config(env)

        self.assertEqual(cfg.db_dsn, "postgres://analytics")
        self.assertEqual(cfg.bind_host, "127.0.0.1")
        self.assertEqual(cfg.bind_port, 8090)
        self.assertEqual(cfg.users, {"owner": "hash1", "partner": "hash2"})

    def test_load_config_falls_back_to_db_dsn(self):
        cfg = load_config(
            {
                "DB_DSN": "postgres://bot",
                "ANALYTICS_SESSION_SECRET": "secret",
                "ANALYTICS_USERS": "owner:hash1",
            }
        )

        self.assertEqual(cfg.db_dsn, "postgres://bot")

    def test_load_config_requires_dsn_secret_and_users(self):
        with self.assertRaisesRegex(ValueError, "DB_DSN"):
            load_config({"ANALYTICS_SESSION_SECRET": "secret", "ANALYTICS_USERS": "owner:hash"})

        with self.assertRaisesRegex(ValueError, "ANALYTICS_SESSION_SECRET"):
            load_config({"DB_DSN": "postgres://bot", "ANALYTICS_USERS": "owner:hash"})

        with self.assertRaisesRegex(ValueError, "ANALYTICS_USERS"):
            load_config({"DB_DSN": "postgres://bot", "ANALYTICS_SESSION_SECRET": "secret"})

    def test_config_type_is_explicit(self):
        cfg = load_config(
            {
                "DB_DSN": "postgres://bot",
                "ANALYTICS_SESSION_SECRET": "secret",
                "ANALYTICS_USERS": "owner:hash1",
                "ANALYTICS_BIND_HOST": "0.0.0.0",
                "ANALYTICS_BIND_PORT": "9000",
            }
        )

        self.assertIsInstance(cfg, AnalyticsConfig)
        self.assertEqual(cfg.bind_host, "0.0.0.0")
        self.assertEqual(cfg.bind_port, 9000)


if __name__ == "__main__":
    unittest.main()
