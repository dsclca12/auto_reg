import unittest
from unittest import mock

from core.base_mailbox import DuckMailMailbox, MailboxAccount


class DuckMailMailboxTests(unittest.TestCase):
    @mock.patch("time.sleep", return_value=None)
    def test_wait_for_code_skips_excluded_codes(self, _sleep):
        mailbox = DuckMailMailbox(api_key="direct-key")
        mailbox._request = mock.Mock(side_effect=[
            _response(
                {
                    "hydra:member": [
                        {"id": "m1", "subject": "Your code is 140351"},
                    ]
                }
            ),
            _response(
                {
                    "id": "m1",
                    "subject": "Your code is 140351",
                    "text": "verification code 140351",
                }
            ),
            _response(
                {
                    "hydra:member": [
                        {"id": "m1", "subject": "Your code is 140351"},
                        {"id": "m2", "subject": "Your code is 240852"},
                    ]
                }
            ),
            _response(
                {
                    "id": "m2",
                    "subject": "Your code is 240852",
                    "text": "verification code 240852",
                }
            ),
        ])

        code = mailbox.wait_for_code(
            MailboxAccount(email="demo@example.com", account_id="token-1"),
            timeout=5,
            exclude_codes={"140351"},
        )

        self.assertEqual(code, "240852")
        self.assertEqual(mailbox._request.call_count, 4)


def _response(payload):
    response = mock.Mock()
    response.json.return_value = payload
    response.text = ""
    return response


if __name__ == "__main__":
    unittest.main()
