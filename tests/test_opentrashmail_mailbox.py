import unittest
from unittest.mock import patch

from core.base_mailbox import MailboxAccount, create_mailbox


class OpenTrashMailMailboxTests(unittest.TestCase):
    def _build_mailbox(self, **extra):
        config = {
            "opentrashmail_api_url": "https://mail.example.com",
            "opentrashmail_password": "secret-pass",
        }
        config.update(extra)
        return create_mailbox("opentrashmail", extra=config)

    @patch("requests.request")
    def test_get_email_can_compose_local_address_when_domain_configured(self, mock_request):
        mailbox = self._build_mailbox(opentrashmail_domain="xiyoufm.com")

        with patch.object(type(mailbox), "_generate_local_part", return_value="demo1234"):
            account = mailbox.get_email()

        self.assertEqual(account.email, "demo1234@xiyoufm.com")
        self.assertEqual(account.account_id, "demo1234@xiyoufm.com")
        self.assertEqual(account.extra["domain"], "xiyoufm.com")
        mock_request.assert_not_called()

    @patch("requests.request")
    def test_get_email_parses_random_address_from_html(self, mock_request):
        mock_request.return_value.status_code = 200
        mock_request.return_value.text = """
        <nav aria-label="breadcrumb">
          <ul><li>ashamed.glove@xiyoufm.com</li></ul>
        </nav>
        <script>history.pushState({urlpath:"/address/ashamed.glove@xiyoufm.com"}, "", "/address/ashamed.glove@xiyoufm.com");</script>
        """

        mailbox = self._build_mailbox()
        account = mailbox.get_email()

        self.assertEqual(account.email, "ashamed.glove@xiyoufm.com")
        self.assertEqual(account.account_id, "ashamed.glove@xiyoufm.com")
        mock_request.assert_called_once_with(
            "GET",
            "https://mail.example.com/api/random",
            params={"password": "secret-pass"},
            json=None,
            headers={"accept": "application/json, text/plain, */*"},
            proxies=None,
            timeout=15,
        )

    @patch("requests.request")
    def test_get_current_ids_reads_json_listing(self, mock_request):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {
            "1775019492111": {
                "email": "test@xiyoufm.com",
                "subject": "测试",
            },
            "1775019500000": {
                "email": "test@xiyoufm.com",
                "subject": "验证码 123456",
            },
        }
        mock_request.return_value.text = ""

        mailbox = self._build_mailbox()
        ids = mailbox.get_current_ids(MailboxAccount(email="test@xiyoufm.com"))

        self.assertEqual(ids, {"1775019492111", "1775019500000"})
        mock_request.assert_called_once_with(
            "GET",
            "https://mail.example.com/json/test@xiyoufm.com",
            params={"password": "secret-pass"},
            json=None,
            headers={"accept": "application/json, text/plain, */*"},
            proxies=None,
            timeout=10,
        )

    @patch("time.sleep", return_value=None)
    @patch("requests.request")
    def test_wait_for_code_reads_detail_and_skips_excluded_codes(self, mock_request, _sleep):
        mock_request.side_effect = [
            _response(
                {
                    "m1": {
                        "email": "test@xiyoufm.com",
                        "subject": "Your code 111111",
                    }
                }
            ),
            _response(
                {
                    "raw": "Subject: Your code 111111\r\n\r\n111111",
                    "parsed": {
                        "subject": "Your code 111111",
                        "body": "111111",
                    },
                }
            ),
            _response(
                {
                    "m1": {
                        "email": "test@xiyoufm.com",
                        "subject": "Your code 111111",
                    },
                    "m2": {
                        "email": "test@xiyoufm.com",
                        "subject": "Your code 222222",
                    },
                }
            ),
            _response(
                {
                    "raw": "Subject: verification code\r\n\r\n222222",
                    "parsed": {
                        "subject": "verification code",
                        "body": "222222",
                    },
                }
            ),
        ]

        mailbox = self._build_mailbox()
        code = mailbox.wait_for_code(
            MailboxAccount(email="test@xiyoufm.com"),
            timeout=5,
            exclude_codes={"111111"},
        )

        self.assertEqual(code, "222222")
        self.assertEqual(mock_request.call_count, 4)


def _response(payload, status_code=200):
    response = unittest.mock.Mock()
    response.status_code = status_code
    response.json.return_value = payload
    response.text = ""
    return response


if __name__ == "__main__":
    unittest.main()
