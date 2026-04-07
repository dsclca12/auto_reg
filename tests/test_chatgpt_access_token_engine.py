import unittest

from core.task_runtime import StopTaskRequested
from platforms.chatgpt.access_token_only_registration_engine import (
    AccessTokenOnlyRegistrationEngine,
)


class _DummyEmailService:
    def create_email(self):
        return {"email": "user@example.com", "service_id": "svc-1"}

    def get_verification_code(self, **kwargs):
        return "123456"


class AccessTokenOnlyRegistrationEngineTests(unittest.TestCase):
    def test_run_propagates_stop_request_via_interrupt_checker(self):
        engine = AccessTokenOnlyRegistrationEngine(
            email_service=_DummyEmailService(),
            proxy_url="http://127.0.0.1:7890",
            callback_logger=lambda msg: None,
            interrupt_checker=lambda: (_ for _ in ()).throw(StopTaskRequested()),
        )

        with self.assertRaises(StopTaskRequested):
            engine.run()


if __name__ == "__main__":
    unittest.main()
