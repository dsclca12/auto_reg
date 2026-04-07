import unittest

from core.base_platform import BasePlatform, RegisterConfig
from core.task_runtime import (
    RegisterTaskControl,
    RegisterTaskStore,
    SkipCurrentAttemptRequested,
    StopTaskRequested,
)


class _DummyPlatform(BasePlatform):
    name = "dummy"
    display_name = "Dummy"

    def register(self, email: str, password: str = None):
        return None

    def check_valid(self, account) -> bool:
        return True


class RegisterTaskControlTests(unittest.TestCase):
    def test_skip_request_is_consumed_only_once(self):
        control = RegisterTaskControl()

        control.request_skip_current()

        with self.assertRaises(SkipCurrentAttemptRequested):
            control.checkpoint()

        control.checkpoint()

    def test_stop_request_is_sticky(self):
        control = RegisterTaskControl()

        control.request_stop()

        with self.assertRaises(StopTaskRequested):
            control.checkpoint()
        with self.assertRaises(StopTaskRequested):
            control.checkpoint()

    def test_skip_current_targets_only_active_attempts_in_multithread_mode(self):
        control = RegisterTaskControl()
        attempt_a = control.start_attempt()
        attempt_b = control.start_attempt()

        control.request_skip_current()

        with self.assertRaises(SkipCurrentAttemptRequested):
            control.checkpoint(attempt_id=attempt_a)
        with self.assertRaises(SkipCurrentAttemptRequested):
            control.checkpoint(attempt_id=attempt_b)

        control.finish_attempt(attempt_a)
        control.finish_attempt(attempt_b)

        attempt_c = control.start_attempt()
        control.checkpoint(attempt_id=attempt_c)
        control.finish_attempt(attempt_c)

    def test_platform_interrupt_checker_consumes_targeted_skip_for_active_attempt(self):
        control = RegisterTaskControl()
        platform = _DummyPlatform(RegisterConfig())
        platform.bind_task_control(control)
        platform._task_attempt_token = control.start_attempt()
        checker = platform.build_interrupt_checker()

        control.request_skip_current()

        with self.assertRaises(SkipCurrentAttemptRequested):
            checker()

        control.finish_attempt(platform._task_attempt_token)


class RegisterTaskStoreTests(unittest.TestCase):
    def test_snapshot_contains_control_and_skip_fields(self):
        store = RegisterTaskStore()
        task_id = "task-runtime-snapshot"

        store.create(
            task_id,
            platform="chatgpt",
            total=2,
            source="manual",
            meta={"scope": "unit"},
        )
        store.request_skip_current(task_id)
        store.finish(
            task_id,
            status="done",
            success=1,
            skipped=1,
            errors=["error-a"],
        )

        snapshot = store.snapshot(task_id)

        self.assertEqual(snapshot["success"], 1)
        self.assertEqual(snapshot["skipped"], 1)
        self.assertEqual(snapshot["errors"], ["error-a"])
        self.assertEqual(
            snapshot["control"]["pending_skip_requests"],
            1,
        )


if __name__ == "__main__":
    unittest.main()
