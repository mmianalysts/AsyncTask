import logging
import os
import re
import time
import unittest
from concurrent.futures import Future
from typing import Any

from pymongo.collection import Collection

from async_task import AsyncTask


class TestTask(AsyncTask):
    def get_task_id(self, duration: int, error: bool):
        return f"test_async_{duration}_{error}"

    def main(self, logger: logging.Logger, duration: int, error: bool = False):
        logger.info("task start")
        time.sleep(duration)
        if error:
            raise ValueError("error")
        logger.info("task finished")
        return "ok"


class MockCollection(Collection):
    def __init__(self):
        self.data: list[dict[str, Any]] = []

    def find_one(self, query: dict[str, Any], *args, **kwargs):
        for item in self.data:
            if all(key in item and item[key] == value for key, value in query.items()):
                return item

    def update_one(self, query, data, upsert):  # type: ignore
        assert "$set" in data
        data = data["$set"]
        for item in self.data:
            if all(key in item and item[key] == value for key, value in query.items()):
                item.update(data)
                return
        if upsert:
            self.data.append(query | data)


class TestAsyncTaskMethods(unittest.TestCase):
    """
    test_get_task_id: 测试生成task_id的方法
    test_get_logger: 测试获取logger的方法
    test_update_task: 测试update_task方法
    test_run_new_task: 测试通过run函数新建任务
    test_run_existing_task: 测试通过run函数重新运行失败的任务
    test_retrieve: 测试retrieve方法
    test_callback_success: 测试callback方法，任务成功
    test_callback_failure: 测试callback方法，任务失败
    test_app_run_task: 测试通过api启动任务
    test_app_retrieve_task: 测试通过api获取任务状态
    test_run_multiple_tasks: 测试同时运行多个任务时的并发控制和状态更新
    """

    logger_dir = "."

    def setUp(self):
        collection = MockCollection()
        self.collection = collection
        self.task = TestTask(collection, self.logger_dir, max_worker=2)

    def tearDown(self):
        for file in os.listdir():
            if re.match(r"(test.+|threads_num)\.log", file):
                os.remove(file)

    def test_get_task_id(self):
        task_id = self.task.get_task_id(1, False)
        self.assertEqual(task_id, "test_async_1_False")

    def test_get_logger(self):
        logger = self.task.get_logger("test_async_task_id")
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "test_async_task_id")

    def test_update_task(self):
        task_id = "test_async_task_id"
        data = {"status": "running"}
        self.task.update_task(task_id, data)
        task = self.collection.find_one({"task_id": task_id})
        assert task, "task not found"
        self.assertEqual(task["status"], "running")

    def test_run_new_task(self):
        task_id = self.task.run(duration=2, error=False)
        task = self.collection.find_one({"task_id": task_id})
        assert task, "task not found"
        self.assertEqual(task["status"], "running")
        time.sleep(2)  # 等待任务完成后运行tearDown

    def test_run_existing_task(self):
        task_id = "test_async_existing_task_id"
        self.task.update_task(task_id, {"status": "failed", "params": {"duration": 1, "error": ""}})
        new_task_id = self.task.run(task_id=task_id)
        self.assertEqual(task_id, new_task_id)

    def test_retrieve(self):
        task_id = "test_async_task_id"
        self.task.update_task(task_id, {"status": "running"})
        task = self.task.retrieve(task_id)
        assert task, "task not found"
        self.assertEqual(task["status"], "running")

    def test_callback_success(self):
        task_id = "test_async_task_id"
        future = Future()
        future.set_result("ok")
        self.task.callback(future, task_id)
        task = self.collection.find_one({"task_id": task_id})
        assert task, "task not found"
        self.assertEqual(task["status"], "success")
        self.assertEqual(task["result"], "ok")

    def test_callback_failure(self):
        task_id = "test_async_task_id"
        future = Future()
        future.set_exception(ValueError("error"))
        self.task.callback(future, task_id)
        task = self.collection.find_one({"task_id": task_id})
        assert task, "task not found"
        self.assertEqual(task["status"], "failed")
        self.assertIn("ValueError: error", task["error"])

    def test_app_run_task(self):
        app = self.task.app(endpoint="/test")
        client = app.test_client()
        res = client.post("/test", json={"data": {"duration": 1, "error": False}}).get_json()
        self.assertEqual(res["status"], "ok")
        task_id = res["data"]["task_id"]
        task = self.collection.find_one({"task_id": task_id})
        assert task, "task not found"
        self.assertEqual(task["status"], "running")

    def test_app_retrieve_task(self):
        app = self.task.app(endpoint="/test")
        client = app.test_client()
        task_id = self.task.get_task_id(duration=1, error=False)
        self.task.update_task(task_id, {"status": "running"})
        res = client.get("/test", query_string={"task_id": task_id}).get_json()
        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["data"]["status"], "running")

    def test_run_multiple_tasks(self):
        app = self.task.app(endpoint="/test")
        client = app.test_client()
        tasks = [
            {"duration": 5, "error": False},
            {"duration": 5, "error": True},
            {"duration": 8, "error": False},
            {"duration": 8, "error": True},
        ]
        results = [client.post("/test", json={"data": task}).get_json() for task in tasks]
        time.sleep(6)

        for (expect_status, expect_error), result in zip(
            [
                ("success", ""),
                ("failed", "ValueError: error"),
                ("running", ""),
                ("running", ""),
            ],
            results,
        ):
            doc = self.collection.find_one({"task_id": result["data"]["task_id"]})
            assert doc, f"task {result['data']['task_id']} not found"
            self.assertEqual(doc["status"], expect_status)
            error = doc.get("error", "").strip().split("\n")[-1]
            self.assertEqual(error, expect_error)

        time.sleep(8)
        for (expect_status, expect_error), result in zip(
            [
                ("success", ""),
                ("failed", "ValueError: error"),
            ],
            results[-2:],
        ):
            doc = self.collection.find_one({"task_id": result["data"]["task_id"]})
            assert doc, f"task {result['data']['task_id']} not found"
            self.assertEqual(doc["status"], expect_status)
            error = doc.get("error", "").strip().split("\n")[-1]
            self.assertEqual(error, expect_error)


if __name__ == "__main__":
    unittest.main()
