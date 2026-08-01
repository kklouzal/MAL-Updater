from __future__ import annotations

import unittest

from mal_updater.redaction import (
    REDACTED,
    TRUNCATED_DEPTH_KEY,
    TRUNCATED_ITEMS_KEY,
    TRUNCATED_TEXT_SUFFIX,
    sanitize_text,
    sanitize_url,
    sanitize_value,
)


class SharedSanitizerTests(unittest.TestCase):
    def test_url_strips_userinfo_fragment_and_neutralizes_values(self) -> None:
        sentinel = "SENTINEL-url-credential"
        sanitized = sanitize_url(
            f"https://user:{sentinel}@example.invalid:8443/path/to/hook"
            f"?access_token={sentinel}&limit=50&limit=&code=oauth-code#private"
        )
        self.assertEqual(
            "https://example.invalid:8443/path/to/hook?"
            "access_token=%3Credacted%3E&limit=%3Cvalue%3E&limit=%3Cvalue%3E&code=%3Credacted%3E",
            sanitized,
        )
        self.assertNotIn(sentinel, sanitized)

    def test_url_marks_all_sensitive_keys_case_insensitively(self) -> None:
        labels = (
            "token", "access_token", "refresh_token", "authorization", "auth",
            "password", "passwd", "secret", "client_secret", "api_key", "x-api-key",
            "cookie", "set-cookie", "session", "sessionid", "code", "credential",
        )
        url = "https://example.invalid/a?" + "&".join(
            f"{label.upper()}=SENTINEL-{index}" for index, label in enumerate(labels)
        )
        sanitized = sanitize_url(url)
        for index, label in enumerate(labels):
            self.assertNotIn(f"SENTINEL-{index}", sanitized)
            self.assertIn(f"{label.upper()}=%3Credacted%3E", sanitized)

    def test_url_rejects_malformed_urls_and_ports(self) -> None:
        malformed = (
            "https://example.invalid:not-a-port/path",
            "https://example.invalid:99999/path",
            "https://[broken.invalid/path",
            "https://example.invalid/%not-hex",
            "not an absolute url",
        )
        for value in malformed:
            with self.subTest(value=value):
                self.assertEqual("<invalid-url>", sanitize_url(value))

    def test_text_redacts_authorization_and_bound_values_across_separators(self) -> None:
        sentinel = "SENTINEL-credential-value"
        basic = "U0VOVElORUwtQkFTSUM="
        text = (
            f"Bearer {sentinel} Basic {basic} token={sentinel} &ACCESS_TOKEN={sentinel} "
            f"password: {sentinel} "
            f"{{\"refresh_token\": \"{sentinel} with spaces\", 'client_secret':'{sentinel}'}}"
        )
        sanitized = sanitize_text(text)
        self.assertNotIn(sentinel, sanitized)
        self.assertNotIn(basic, sanitized)
        self.assertGreaterEqual(sanitized.count(REDACTED), 7)

    def test_text_redacts_obvious_jwt_but_keeps_useful_semantics(self) -> None:
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJlYWJj"
        sanitized = sanitize_text(f"HTTP 401 invalid_grant retry denied {jwt}")
        self.assertEqual("HTTP 401 invalid_grant retry denied <redacted>", sanitized)

    def test_text_only_redacts_account_identity_when_label_bound(self) -> None:
        sentinel_email = "sentinel.user@example.invalid"
        prose = f"Contact {sentinel_email}; anime title: My Secret, Login!, User Friendly."
        self.assertEqual(prose, sanitize_text(prose))
        bound = sanitize_text(
            f"username={sentinel_email} EMAIL: {sentinel_email} "
            f"account={sentinel_email} login={sentinel_email} user={sentinel_email}"
        )
        self.assertNotIn(sentinel_email, bound)
        self.assertEqual(5, bound.count(REDACTED))

    def test_text_bound_is_explicit_and_stable(self) -> None:
        sanitized = sanitize_text("x" * 100, max_length=30)
        self.assertEqual(30, len(sanitized))
        self.assertTrue(sanitized.endswith(TRUNCATED_TEXT_SUFFIX))
        with self.assertRaises(ValueError):
            sanitize_text("value", max_length=5)

    def test_structured_values_redact_sensitive_keys_and_preserve_scalars(self) -> None:
        sentinel = "SENTINEL-structured-credential"
        labels = (
            "token", "ACCESS_TOKEN", "refresh-token", "Authorization", "auth",
            "PASSWORD", "passwd", "secret", "client_secret", "api-key", "x-api-key",
            "Cookie", "set-cookie", "session", "SESSIONID", "code", "credential",
            "username", "email", "account", "login", "user",
        )
        payload = {label: sentinel for label in labels}
        payload.update(
            {
                "title": "My Secret Anime",
                "reason": "HTTP 401 invalid_grant",
                "ordinary_email": "viewer@example.invalid",
                "nested": [{"password": sentinel}, ("ordinary prose", {"token": sentinel})],
                "count": 3,
                "ratio": 1.5,
                "enabled": True,
                "missing": None,
            }
        )
        sanitized = sanitize_value(payload)
        rendered = repr(sanitized)
        self.assertNotIn(sentinel, rendered)
        for label in labels:
            self.assertEqual(REDACTED, sanitized[label])
        self.assertEqual("My Secret Anime", sanitized["title"])
        self.assertEqual("HTTP 401 invalid_grant", sanitized["reason"])
        self.assertEqual("viewer@example.invalid", sanitized["ordinary_email"])
        self.assertIsInstance(sanitized["nested"], list)
        self.assertIsInstance(sanitized["nested"][1], tuple)
        self.assertEqual((3, 1.5, True, None), tuple(sanitized[key] for key in ("count", "ratio", "enabled", "missing")))

    def test_structured_generic_code_fails_closed_for_lowercase_snake_case(self) -> None:
        sanitized = sanitize_value(
            {
                "code": "sentinel_code_123",
                "reason": "HTTP 401 invalid_grant",
                "reason_code": "auth_degraded",
                "auth_failure_kind": "invalid_grant",
            }
        )

        self.assertEqual(REDACTED, sanitized["code"])
        self.assertEqual("HTTP 401 invalid_grant", sanitized["reason"])
        self.assertEqual("auth_degraded", sanitized["reason_code"])
        self.assertEqual("invalid_grant", sanitized["auth_failure_kind"])

    def test_structured_collection_and_depth_bounds_are_explicit(self) -> None:
        self.assertEqual(
            [0, 1, 2, {TRUNCATED_ITEMS_KEY: 17}],
            sanitize_value(list(range(20)), max_items=3),
        )
        self.assertEqual(
            (0, 1, {TRUNCATED_ITEMS_KEY: 3}),
            sanitize_value(tuple(range(5)), max_items=2),
        )
        bounded_dict = sanitize_value({"a": 1, "b": 2, "c": 3}, max_items=2)
        self.assertEqual({"a": 1, "b": 2, TRUNCATED_ITEMS_KEY: 1}, bounded_dict)
        depth_bounded = sanitize_value({"one": {"two": {"secret": "SENTINEL"}}}, max_depth=2)
        self.assertEqual({TRUNCATED_DEPTH_KEY: True}, depth_bounded["one"]["two"])

    def test_structured_bounds_are_configurable_and_validated(self) -> None:
        sanitized = sanitize_value({"message": "z" * 100}, max_string=24)
        self.assertEqual(24, len(sanitized["message"]))
        self.assertTrue(sanitized["message"].endswith(TRUNCATED_TEXT_SUFFIX))
        with self.assertRaises(ValueError):
            sanitize_value([], max_items=-1)
        with self.assertRaises(ValueError):
            sanitize_value([], max_depth=-1)


if __name__ == "__main__":
    unittest.main()
