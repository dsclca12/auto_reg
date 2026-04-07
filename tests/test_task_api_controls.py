import unittest

from fastapi.testclient import TestClient

from api.tasks import _create_task_record, _task_store, RegisterTaskRequest
from main import app


class TaskApiControlTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.req = RegisterTaskRequest(platform="chatgpt", count=1, concurrency=1)

    def test_skip_current_endpoint_returns_control_snapshot(self):
        task_id = "task-api-skip"
        _create_task_record(task_id, self.req, "manual", {"scope": "unit"})

        response = self.client.post(f"/api/tasks/{task_id}/skip-current")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["task_id"], task_id)
        self.assertIn("control", payload)
        self.assertEqual(payload["control"]["pending_skip_requests"], 1)

    def test_stop_endpoint_marks_control_as_requested(self):
        task_id = "task-api-stop"
        _create_task_record(task_id, self.req, "manual", {"scope": "unit"})

        response = self.client.post(f"/api/tasks/{task_id}/stop")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["control"]["stop_requested"])

        snapshot = self.client.get(f"/api/tasks/{task_id}").json()
        self.assertTrue(snapshot["control"]["stop_requested"])

    def test_control_endpoints_return_404_for_unknown_task(self):
        self.assertEqual(
            self.client.post("/api/tasks/task-missing/skip-current").status_code,
            404,
        )
        self.assertEqual(
            self.client.post("/api/tasks/task-missing/stop").status_code,
            404,
        )

    def test_stream_logs_finishes_when_task_is_stopped(self):
        task_id = "task-api-stream-stop"
        _create_task_record(task_id, self.req, "manual", {"scope": "unit"})
        _task_store.append_log(task_id, "[12:00:00] 等待中")
        _task_store.finish(
            task_id,
            status="stopped",
            success=0,
            skipped=0,
            errors=[],
        )

        with self.client.stream("GET", f"/api/tasks/{task_id}/logs/stream") as response:
            self.assertEqual(response.status_code, 200)
            body = "".join(response.iter_text())

        self.assertIn('"line": "[12:00:00] \\u7b49\\u5f85\\u4e2d"', body)
        self.assertIn('"done": true', body)
        self.assertIn('"status": "stopped"', body)


if __name__ == "__main__":
    unittest.main()
