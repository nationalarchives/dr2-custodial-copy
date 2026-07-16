import io
import json
import unittest
from unittest.mock import MagicMock

from mock_tape_api import FILE_RESPONSE, Handler


class MockTapeApiHandlerTests(unittest.TestCase):
    def make_handler(self):
        handler = Handler.__new__(Handler)
        handler.send_error = MagicMock()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        handler.send_json = MagicMock()
        return handler

    def test_check_path_matches_expected_path(self):
        handler = self.make_handler()
        handler.path = "/v1/file?id=123"

        is_valid = Handler.check_path(handler, "/v1/file")

        self.assertTrue(is_valid)
        handler.send_error.assert_not_called()

    def test_check_path_sends_404_when_path_does_not_match(self):
        handler = self.make_handler()
        handler.path = "/v1/wrong"

        is_valid = Handler.check_path(handler, "/v1/file")

        self.assertFalse(is_valid)
        handler.send_error.assert_called_once_with(404)

    def test_do_get_sends_401_without_authorization_header(self):
        handler = self.make_handler()
        handler.path = "/v1/file"
        handler.headers = {}
        handler.check_path = MagicMock()

        Handler.do_GET(handler)

        handler.send_error.assert_called_once_with(401)
        handler.check_path.assert_not_called()
        handler.send_json.assert_not_called()

    def test_do_get_returns_expected_json_for_authorized_request(self):
        handler = self.make_handler()
        handler.path = "/v1/file"
        handler.headers = {"Authorization": "Bearer token"}

        Handler.do_GET(handler)

        handler.send_error.assert_not_called()
        handler.send_json.assert_called_once_with(FILE_RESPONSE)

    def test_send_json_writes_json_response(self):
        handler = Handler.__new__(Handler)
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        handler.wfile = io.BytesIO()

        body = {"hello": "world"}
        Handler.send_json(handler, body)

        handler.send_response.assert_called_once_with(200)
        handler.send_header.assert_called_once_with("Content-Type", "application/json")
        handler.end_headers.assert_called_once()
        self.assertEqual(handler.wfile.getvalue(), json.dumps(body).encode("utf-8"))

    def test_do_post_sends_token_on_login_path(self):
        handler = self.make_handler()
        handler.path = "/v1/security/login"
        handler.check_path = MagicMock(return_value=True)

        Handler.do_POST(handler)

        handler.check_path.assert_called_once_with("/v1/security/login")
        handler.send_json.assert_called_once_with({"response": "atoken"})


if __name__ == "__main__":
    unittest.main()
