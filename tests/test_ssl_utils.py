from __future__ import annotations

import json
import ssl
from unittest.mock import MagicMock, patch

import pytest

from apache_airflow_provider_rmq.utils.ssl import build_ssl_context

SSL_MODULE = "apache_airflow_provider_rmq.utils.ssl.ssl"


class TestDisabled:
    def test_returns_none_when_ssl_disabled(self):
        assert build_ssl_context({"ssl_enabled": False}) is None

    def test_returns_none_when_ssl_key_missing(self):
        assert build_ssl_context({}) is None


class TestBasicContext:
    def test_returns_ssl_context_when_enabled(self):
        ctx = build_ssl_context({"ssl_enabled": True})
        assert isinstance(ctx, ssl.SSLContext)

    def test_creates_default_context(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            mock_ctx.return_value = MagicMock(spec=ssl.SSLContext)
            result = build_ssl_context({"ssl_enabled": True})
            mock_ctx.assert_called_once()
            assert result is mock_ctx.return_value


class TestNestedSslOptions:
    def test_loads_ca_certs(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "ssl_options": {"ca_certs": "/path/ca.pem"},
            })
            ctx.load_verify_locations.assert_called_once_with("/path/ca.pem")

    def test_loads_cert_chain(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "ssl_options": {"certfile": "/cert.pem", "keyfile": "/key.pem"},
            })
            ctx.load_cert_chain.assert_called_once_with(
                certfile="/cert.pem", keyfile="/key.pem",
            )

    def test_cert_none_disables_verification(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "ssl_options": {"cert_reqs": "CERT_NONE"},
            })
            assert ctx.check_hostname is False
            assert ctx.verify_mode == ssl.CERT_NONE

    def test_ssl_options_as_json_string(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "ssl_options": json.dumps({"ca_certs": "/path/ca.pem"}),
            })
            ctx.load_verify_locations.assert_called_once_with("/path/ca.pem")


class TestFlatExtras:
    """Test that flat extras (from form widgets) are also recognized."""

    def test_flat_ca_certs(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "ca_certs": "/path/ca.pem",
            })
            ctx.load_verify_locations.assert_called_once_with("/path/ca.pem")

    def test_flat_cert_chain(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "certfile": "/cert.pem",
                "keyfile": "/key.pem",
            })
            ctx.load_cert_chain.assert_called_once_with(
                certfile="/cert.pem", keyfile="/key.pem",
            )

    def test_nested_takes_precedence_over_flat(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "ca_certs": "/flat/ca.pem",
                "ssl_options": {"ca_certs": "/nested/ca.pem"},
            })
            ctx.load_verify_locations.assert_called_once_with("/nested/ca.pem")


class TestPartialCertChain:
    def test_certfile_only_does_not_load(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "ssl_options": {"certfile": "/cert.pem"},
            })
            ctx.load_cert_chain.assert_not_called()

    def test_keyfile_only_does_not_load(self):
        with patch(f"{SSL_MODULE}.create_default_context") as mock_ctx:
            ctx = MagicMock(spec=ssl.SSLContext)
            mock_ctx.return_value = ctx
            build_ssl_context({
                "ssl_enabled": True,
                "ssl_options": {"keyfile": "/key.pem"},
            })
            ctx.load_cert_chain.assert_not_called()


class TestInvalidJson:
    def test_invalid_json_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid JSON in ssl_options"):
            build_ssl_context({
                "ssl_enabled": True,
                "ssl_options": "{not valid json",
            })