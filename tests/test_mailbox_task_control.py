import unittest

from core.base_mailbox import BaseMailbox, MailboxAccount
from core.task_runtime import (
    RegisterTaskControl,
    SkipCurrentAttemptRequested,
    StopTaskRequested,
)


class _PollingMailbox(BaseMailbox):
    def __init__(self):
        self.poll_count = 0

    def get_email(self) -> MailboxAccount:
        return MailboxAccount(email="demo@example.com")

    def get_current_ids(self, account: MailboxAccount) -> set:
        return set()

    def wait_for_code(
        self,
        account: MailboxAccount,
        keyword: str = "",
        timeout: int = 120,
        before_ids: set = None,
        code_pattern: str = None,
        **kwargs,
    ) -> str:
        def poll_once():
            self.poll_count += 1
            return None

        return self._run_polling_wait(
            timeout=timeout,
            poll_interval=0.01,
            poll_once=poll_once,
        )


class MailboxTaskControlTests(unittest.TestCase):
    def test_skip_current_interrupts_wait_loop(self):
        mailbox = _PollingMailbox()
        mailbox._task_control = RegisterTaskControl()
        mailbox._task_control.request_skip_current()

        with self.assertRaises(SkipCurrentAttemptRequested):
            mailbox.wait_for_code(mailbox.get_email(), timeout=1)

        self.assertEqual(mailbox.poll_count, 0)

    def test_stop_interrupts_wait_loop(self):
        mailbox = _PollingMailbox()
        mailbox._task_control = RegisterTaskControl()
        mailbox._task_control.request_stop()

        with self.assertRaises(StopTaskRequested):
            mailbox.wait_for_code(mailbox.get_email(), timeout=1)

        self.assertEqual(mailbox.poll_count, 0)


if __name__ == "__main__":
    unittest.main()
