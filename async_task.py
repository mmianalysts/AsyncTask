import abc
import inspect
import logging
import os
import time
import traceback
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import Enum
from functools import cache, cached_property
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Optional, TypeVar, Union
from uuid import uuid4
from warnings import warn

from flask import Flask, request
from pymongo.collection import Collection

_lock = Lock()
T = TypeVar("T")


class ExecutorMonitor:
    def __init__(self, log_dir: Path, executor: ThreadPoolExecutor, log_interval: int = 60):
        self.runnig = False
        self.log_interval = log_interval
        self.executor = executor
        self.log_file = log_dir / "threads_num.log"

    @cached_property
    def logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)
        handler = TimedRotatingFileHandler(self.log_file, when="d", interval=1, backupCount=30)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def monitor(self):
        while self.runnig:
            self.logger.info("Current work queue size: %s", self.executor._work_queue.qsize())
            time.sleep(self.log_interval)

    def start(self):
        self.runnig = True
        Thread(target=self.monitor, daemon=True).start()

    def stop(self):
        self.runnig = False


class TaskStatus(str, Enum):
    """任务状态"""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"


class AsyncTask(abc.ABC):
    """异步任务类，负责异步执行用户定义的main方法，更新结果到mongo表中。设置DISABLE_STREAM_HANDLER环境变量可以禁用控制台输出

    >>> class Api(AsyncTask):
    >>>    def main(self, args1, param1 = param1, logger=None):
    >>>       logger.info("start")
    >>>       return "ok"
    >>> api = Api(collection, logger_dir, max_worker=4)
    >>> app = api.app()
    >>> app.run()

    必须定义方法：
        main(logger, *args, **kwargs) -> Any: 算法主函数，返回值会更新到mongo表中
            logger会通过main(logger=logger)的方式传入，用于记录日志
            或者通过self.get_logger(task_id)获取logger对象
            或者在main开始后通过logging.getLogger(task_id)获取logger对象

    可选定义方法：
        get_task_id(*args, **kwargs) -> str: 生成task_id的方法, 默认使用uuid4().hex
        retrieve(task_id: str) -> Optional[dict]: 获取任务状态, 默认根据task_id从mongo表中查status, result, error
        callback(_f: Future, task_id: str): 异步任务完成后的回调函数， 默认是将main函数的返回结果更新到mongo表的result字段中

    Args:
        collection (pymongo.Collection): 用于保存结果的mongo表对象
        logger_dir (str): 日志文件保存目录
        max_worker (int): 使用默认线程池的最大线程数
        executor (concurrent.futures.Executor): 自定义执行任务的线程池或进程池
        task_timeout (int, timedelta): 任务超时时间, 默认4小时, 单位秒
        outdate_time (None, int, timedelta): 当前时间减更新时间大于该时间时任务会被重新提交，默认无过期时间，单位秒

    Methods:
        get_task_id: 用户定义的生成task_id的方法, 输入参数和main方法一致
        get_logger: 根据task_id获取logger对象
        main: 用户定义的主函数，返回值需要能够更新到mongo表中
        run: 入口函数，负责检查task_id，异步执行main, 返回task_id
        app: 获取flask app对象，用于启动web服务
    """

    def __init__(
        self,
        collection: Collection,
        logger_dir: str,
        max_worker: Optional[int] = None,
        executor: Optional[Executor] = None,
        task_timeout: Union[int, timedelta] = timedelta(hours=4),
        outdate_time: Optional[Union[int, timedelta]] = None,
    ) -> None:
        self.collection = collection

        self.logger_dir = Path(logger_dir)
        self.logger_dir.mkdir(parents=True, exist_ok=True)

        self._executor = executor if executor else ThreadPoolExecutor(max_workers=max_worker)
        if isinstance(self._executor, ThreadPoolExecutor):
            self._monitor = ExecutorMonitor(
                log_dir=self.logger_dir, executor=self._executor, log_interval=60
            )
            self._monitor.start()
        else:
            warn("Monitoring ProcessPoolExecutor is not supported", UserWarning)
            self._monitor = None

        if isinstance(task_timeout, int):
            task_timeout = timedelta(seconds=task_timeout)
        self.task_timeout = task_timeout

        if outdate_time is not None:
            if isinstance(outdate_time, int):
                outdate_time = timedelta(seconds=outdate_time)
            if not isinstance(outdate_time, timedelta):
                raise ValueError("rerun_time_limit must be int or timedelta")
        self.outdate_time = outdate_time
        # 确认main方法能够接受logger参数
        code_obj = self.main.__code__
        if not ("logger" in code_obj.co_varnames or code_obj.co_flags & inspect.CO_VARKEYWORDS):
            raise ValueError("logger must be the first argument in main")

    @cache
    def get_logger(self, task_id: str) -> logging.Logger:
        logger = logging.getLogger(task_id)
        logger.setLevel(logging.DEBUG)
        handler = RotatingFileHandler(
            self.logger_dir / f"{task_id}.log", maxBytes=1024 * 1024 * 10, backupCount=10
        )
        formatter = logging.Formatter(
            "[{process:>5}] {asctime} | {levelname: <8} | "
            + task_id
            + " | {module}:{lineno} - {message}",
            style="{",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        if not os.getenv("DISABLE_STREAM_HANDLER"):
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        return logger

    def get_task_id(self, *args, **kwargs) -> str:
        """用户定义的生成task_id的方法, 输入参数和main方法一致, 默认使用uuid4().hex"""
        warn("get_task_id is not implemented, using uuid4().hex", UserWarning)
        return uuid4().hex

    @abc.abstractmethod
    def main(self, logger, *args, **kwargs) -> Any:
        """用户定义的主函数，返回值会直接更新mongo表中的result字段"""
        ...

    def update_task(self, task_id: str, data: dict[str, Any]):
        """更新任务状态至mongo, 使用upsert=True
        Args:
            task_id (str): 任务id
            data (dict): 更新的数据
        """
        query = {"task_id": task_id}
        data.setdefault("update_time", datetime.now())
        self.collection.update_one(query, {"$set": data}, upsert=True)

    def run(self, task_id: str = "", **params):
        """入口函数，负责检查task_id，异步执行main, 返回task_id"""
        if task_id:
            task = self.collection.find_one({"task_id": task_id})
            if not task:
                raise ValueError(f"task_id: {task_id} not found")
            params = task["params"]
        else:
            task = None
            task_id = self.get_task_id(**params)

        now = datetime.now()
        logger = self.get_logger(task_id)
        # 提交新任务：任务不存在，任务失败，任务超时，或任务超过重跑时间限制
        with _lock:  # 防止重复提交任务
            task = self.collection.find_one({"task_id": task_id})
            if (
                # 任务不存在
                not task
                # 任务失败
                or TaskStatus(task["status"]) in (TaskStatus.FAILED, TaskStatus.PENDING)
                # 任务超时
                or (
                    TaskStatus(task["status"]) == TaskStatus.RUNNING
                    and task["update_time"] < now - self.task_timeout
                )
                # 任务过期
                or (self.outdate_time and task["update_time"] < now - self.outdate_time)
            ):
                _future = self._executor.submit(self.main, logger=logger, **params)
                self.update_task(
                    task_id, data={"status": TaskStatus.RUNNING.value, "params": params}
                )
                logger.info(f"任务已提交，状态已更新为'{TaskStatus.RUNNING.value}'")
                _future.add_done_callback(lambda _f: self.callback(_f, task_id))
            else:
                logger.info(f"任务已存在，状态为'{task['status']}'")
        return task_id

    def retrieve(self, task_id: str) -> Optional[dict]:
        return self.collection.find_one(
            {"task_id": task_id}, {"status": 1, "result": 1, "error": 1, "_id": 0}
        )

    def callback(self, _f: Future[T], task_id: str) -> Optional[T]:
        """异步任务完成后的回调函数，默认是将main函数的返回结果更新到mongo表中"""
        logger = self.get_logger(task_id)
        try:
            result = _f.result()
            self.update_task(task_id, data={"status": TaskStatus.SUCCESS.value, "result": result})
            logger.info("任务完成，状态已更新")
            return result
        except Exception:
            logger.exception("任务失败")
            self.update_task(
                task_id,
                data={"status": TaskStatus.FAILED.value, "error": traceback.format_exc()},
            )

    def app(self, endpoint: str = "/api"):
        """web服务入口"""
        app = Flask(__name__)

        @app.get("/ping")
        def ping():
            return {"status": "ok"}

        @app.post(endpoint)
        def run_task():
            data = request.get_json()
            task_id = self.run(**data["data"])
            return {
                "status": "ok",
                "data": {"task_id": task_id},
                "error_data": [],
                "error_msg": "",
            }

        @app.get(endpoint)
        def retrieve_task():
            try:
                data = request.get_json()
                task_id = data["task_id"]
            except Exception:
                task_id = request.args.get("task_id")
            assert task_id, "task_id is required"
            task = self.retrieve(task_id)
            return {
                "status": "ok",
                "data": task,
                "error_data": [],
                "error_msg": "",
            }

        @app.errorhandler(Exception)
        def handle_error(_):
            return {
                "status": "error",
                "data": "[]",
                "err_data": "[]",
                "error_msg": traceback.format_exc(),
            }, 500

        return app
