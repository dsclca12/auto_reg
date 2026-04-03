import unittest
from unittest import mock

from core.base_mailbox import MailboxAccount, create_mailbox


class FreemailMailboxTests(unittest.TestCase):
    def _build_mailbox(self):
        mailbox = create_mailbox(
            "freemail",
            extra={"freemail_api_url": "https://freemail.example"},
        )
        mailbox._session = mock.Mock()
        return mailbox

    @mock.patch("time.sleep", return_value=None)
    def test_wait_for_code_skips_excluded_verification_code_field(self, _sleep):
        mailbox = self._build_mailbox()
        mailbox._session.get.side_effect = [
            _response(
                [
                    {"id": "m1", "verification_code": "111111"},
                ]
            ),
            _response(
                [
                    {"id": "m1", "verification_code": "111111"},
                    {"id": "m2", "verification_code": "222222"},
                ]
            ),
        ]

        code = mailbox.wait_for_code(
            MailboxAccount(email="demo@example.com"),
            timeout=5,
            exclude_codes={"111111"},
        )

        self.assertEqual(code, "222222")
        self.assertEqual(mailbox._session.get.call_count, 2)

    @mock.patch("time.sleep", return_value=None)
    def test_wait_for_code_skips_excluded_preview_extracted_code(self, _sleep):
        mailbox = self._build_mailbox()
        mailbox._session.get.side_effect = [
            _response(
                [
                    {"id": "m1", "verification_code": None, "preview": "Your verification code is 111111"},
                ]
            ),
            _response(
                [
                    {"id": "m1", "verification_code": None, "preview": "Your verification code is 111111"},
                    {"id": "m2", "verification_code": None, "preview": "Your verification code is 222222"},
                ]
            ),
        ]

        code = mailbox.wait_for_code(
            MailboxAccount(email="demo@example.com"),
            timeout=5,
            exclude_codes={"111111"},
        )

        self.assertEqual(code, "222222")
        self.assertEqual(mailbox._session.get.call_count, 2)


def _response(payload):
    response = mock.Mock()
    response.json.return_value = payload
    return response


if __name__ == "__main__":
    unittest.main()
