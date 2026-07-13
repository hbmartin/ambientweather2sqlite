"""Property-based tests for the vendored HTTP request helpers."""

# ruff: noqa: SLF001

import contextlib
import json
import urllib.parse
from http import HTTPStatus
from http.client import HTTPException, HTTPMessage
from unittest import TestCase
from unittest.mock import Mock, patch

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite import mureq

_QUERY_TEXT = st.text(
    alphabet=st.characters(
        blacklist_characters="&=+#",
        blacklist_categories=("Cs",),
    ),
    max_size=20,
)
_JSON_VALUES = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(max_size=20),
    lambda children: (
        st.lists(children, max_size=4)
        | st.dictionaries(st.text(max_size=10), children, max_size=4)
    ),
    max_leaves=10,
)


class TestQueryPreparation(TestCase):
    @given(params=st.dictionaries(_QUERY_TEXT, _QUERY_TEXT, max_size=8))
    def test_prepare_params_round_trips(self, params):
        encoded = mureq._prepare_params(params)

        self.assertEqual(
            dict(urllib.parse.parse_qsl(encoded, keep_blank_values=True)),
            params,
        )

    @given(
        path=st.from_regex(r"/[a-z]{0,12}", fullmatch=True),
        existing=st.dictionaries(_QUERY_TEXT, _QUERY_TEXT, max_size=4),
        added=st.dictionaries(_QUERY_TEXT, _QUERY_TEXT, max_size=4),
    )
    def test_path_combines_existing_and_added_query(self, path, existing, added):
        existing_query = urllib.parse.urlencode(existing)
        parsed = urllib.parse.urlparse(f"http://example.test{path}?{existing_query}")

        result = mureq._path_with_query_or_params(
            urllib.parse.urlencode(added),
            parsed,
        )
        result_query = urllib.parse.urlparse(result).query

        self.assertEqual(
            urllib.parse.parse_qsl(result_query, keep_blank_values=True),
            [*existing.items(), *added.items()],
        )


class TestBodyAndHeaderPreparation(TestCase):
    @given(value=_JSON_VALUES)
    def test_json_body_round_trips(self, value):
        headers = HTTPMessage()

        body = mureq._prepare_body(None, None, value, headers)

        if value is None:
            self.assertIsNone(body)
            self.assertNotIn("Content-Type", headers)
        else:
            self.assertEqual(json.loads(body), value)
            self.assertEqual(headers["Content-Type"], "application/json")

    @given(fields=st.dictionaries(_QUERY_TEXT, _QUERY_TEXT, max_size=8))
    def test_form_body_round_trips(self, fields):
        headers = HTTPMessage()

        body = mureq._prepare_body(None, fields, None, headers)

        self.assertEqual(
            dict(urllib.parse.parse_qsl(body, keep_blank_values=True)),
            fields,
        )
        self.assertEqual(
            headers["Content-Type"],
            "application/x-www-form-urlencoded",
        )

    @given(body=st.binary(max_size=100))
    def test_explicit_body_takes_precedence(self, body):
        headers = HTTPMessage()

        result = mureq._prepare_body(
            body, {"ignored": "form"}, {"ignored": True}, headers
        )

        self.assertEqual(result, body)
        self.assertNotIn("Content-Type", headers)

    @given(user_agent=st.text(min_size=1, max_size=40))
    def test_explicit_user_agent_is_preserved(self, user_agent):
        headers = mureq._prepare_outgoing_headers({"User-Agent": user_agent})

        self.assertEqual(headers["User-Agent"], user_agent)

    def test_duplicate_incoming_headers_are_joined(self):
        headers = HTTPMessage()
        headers["X-Reading"] = "first"
        headers["X-Reading"] = "second"

        result = mureq._prepare_incoming_headers(headers)

        self.assertEqual(result["X-Reading"], "first,second")


class TestRedirectPreparation(TestCase):
    @given(
        status=st.integers().filter(
            lambda value: value not in {301, 302, 303, 307, 308}
        ),
    )
    def test_non_redirect_status_is_ignored(self, status):
        self.assertIsNone(
            mureq._check_redirect(
                "http://example.test/base/page",
                status,
                {"Location": "/next"},
            ),
        )

    @given(segment=st.from_regex(r"[a-z]{1,12}", fullmatch=True))
    def test_relative_redirect_uses_source_directory(self, segment):
        result = mureq._check_redirect(
            "http://example.test/base/page",
            302,
            {"Location": segment},
        )

        self.assertEqual(result, f"http://example.test/base/{segment}")

    @given(segment=st.from_regex(r"[a-z]{1,12}", fullmatch=True))
    def test_absolute_path_redirect_uses_source_host(self, segment):
        result = mureq._check_redirect(
            "http://example.test/base/page",
            307,
            {"Location": f"/{segment}?source=test"},
        )

        self.assertEqual(result, f"http://example.test/{segment}?source=test")


class TestResponse(TestCase):
    @given(status=st.integers(min_value=100, max_value=599))
    def test_ok_matches_http_error_range(self, status):
        response = mureq.Response("http://example.test", status, HTTPMessage(), b"")

        self.assertEqual(
            response.ok,
            not HTTPStatus.BAD_REQUEST
            <= status
            < HTTPStatus.NETWORK_AUTHENTICATION_REQUIRED,
        )
        self.assertEqual(response.content, b"")
        self.assertEqual(repr(response), f"Response(status_code={status})")

    @given(value=_JSON_VALUES)
    def test_json_response_round_trips(self, value):
        response = mureq.Response(
            "http://example.test",
            200,
            HTTPMessage(),
            json.dumps(value).encode(),
        )

        self.assertEqual(response.json(), value)

    def test_raise_for_status_includes_status(self):
        response = mureq.Response("http://example.test", 503, HTTPMessage(), b"")

        with self.assertRaisesRegex(HTTPException, "503"):
            response.raise_for_status()

    def test_debug_string_describes_binary_body(self):
        response = mureq.Response(
            "http://example.test",
            200,
            {"Content-Type": "application/octet-stream"},
            b"\xff\x00",
        )

        self.assertIn("<2 bytes binary data>", response._debugstr())


class TestRequestPreparation(TestCase):
    @given(
        path=st.from_regex(r"/[a-z]{0,12}", fullmatch=True),
        params=st.dictionaries(_QUERY_TEXT, _QUERY_TEXT, max_size=4),
        port=st.integers(min_value=1, max_value=65535),
    )
    def test_http_request_preserves_path_query_and_port(self, path, params, port):
        encoded = urllib.parse.urlencode(params)

        url, connection, result_path = mureq._prepare_request(
            f"http://example.test:{port}{path}",
            enc_params=encoded,
        )
        self.addCleanup(connection.close)

        self.assertEqual(connection.host, "example.test")
        self.assertEqual(connection.port, port)
        self.assertEqual(
            result_path,
            urllib.parse.urlparse(url).path + (f"?{encoded}" if encoded else ""),
        )

    @given(source_address=st.ip_addresses(v=4).map(str))
    def test_string_source_address_gets_ephemeral_port(self, source_address):
        _, connection, _ = mureq._prepare_request(
            "http://example.test/",
            source_address=source_address,
        )
        self.addCleanup(connection.close)

        self.assertEqual(connection.source_address, (source_address, 0))

    def test_rejects_unsupported_or_hostless_urls(self):
        with self.assertRaisesRegex(ValueError, "unrecognized scheme"):
            mureq._prepare_request("ftp://example.test/file")
        with self.assertRaisesRegex(ValueError, "host is missing"):
            mureq._prepare_request("http:///missing-host")
        with self.assertRaisesRegex(ValueError, "https\\+unix is not implemented"):
            mureq._prepare_request("https+unix://%2Ftmp%2Fserver.sock/")


class TestConvenienceRequests(TestCase):
    def test_get_retries_one_timeout(self):
        response = Mock(body=b"weather")
        with patch.object(
            mureq, "request", side_effect=[TimeoutError, response]
        ) as request:
            result = mureq.get("http://example.test", auto_retry=True)

        self.assertEqual(result, "weather")
        self.assertEqual(request.call_count, 2)

    def test_http_method_helpers_delegate(self):
        response = Mock()
        with patch.object(mureq, "request", return_value=response) as request:
            self.assertIs(mureq.post("http://example.test", b"body"), response)
            self.assertIs(mureq.head("http://example.test"), response)
            self.assertIs(mureq.put("http://example.test", b"body"), response)
            self.assertIs(mureq.patch("http://example.test", b"body"), response)
            self.assertIs(mureq.delete("http://example.test"), response)

        self.assertEqual(
            [call.args[0] for call in request.call_args_list],
            ["POST", "HEAD", "PUT", "PATCH", "DELETE"],
        )

    def test_request_wraps_read_oserror(self):
        raw_response = Mock()
        raw_response.read.side_effect = OSError("broken stream")
        response_context = contextlib.nullcontext(raw_response)

        with (
            patch.object(mureq, "yield_response", return_value=response_context),
            self.assertRaisesRegex(HTTPException, "broken stream"),
        ):
            mureq.request("GET", "http://example.test")
