# AsyncTask

异步任务Web框架

## 使用样例

```python
>>> class Api(AsyncTask):
>>>    def main(self, args1, param1 = param1, logger=None):
>>>       logger.info("start")
>>>       return "ok"
>>> api = Api(collection, logger_dir, max_worker=4)
>>> app = api.app()
>>> app.run()
```

### 必须定义的方法

- main(logger, *args, **kwargs) -> Any: 算法主函数，返回值会更新到mongo表中

> logger会通过`main(logger=logger)`的方式传入，用于记录日志
> 或者通过`self.get_logger(task_id)`获取logger对象
> 或者通过`logging.getLogger(task_id)`获取logger对象(logger的配置可以看做main启动前完成的)

### 可选定义方法

- `get_task_id(*args, **kwargs) -> str`: 生成task_id的方法, 默认使用uuid4().hex
- `retrieve(task_id: str) -> Optional[dict]`: 获取任务状态的方法, 默认根据task_id从mongo表中查询status, result, error字段
- `callback(_f: Future, task_id: str)`: 异步任务完成后的回调函数， 默认是将main函数的返回结果更新到mongo表的result字段中

### Args

- `collection (pymongo.Collection)`: 用于保存结果的mongo表对象
- `logger_dir (str)`: 日志文件保存目录
- `max_worker (int)`: 使用默认线程池的最大线程数
- `executor (concurrent.futures.Executor)`: 自定义执行任务的线程池或进程池
- `task_timeout (int, timedelta)`: 任务超时时间, 默认4小时, 单位秒
- `rerun_time_limit (None, int, timedelta)`: 当前时间减更新时间大于该时间时任务会被重新提交，默认无过期时间，单位秒

### Methods

- `get_task_id`: 用户定义的生成task_id的方法, 输入参数和main方法一致
- `get_logger`: 根据task_id获取logger对象
- `main`: 用户定义的主函数，返回值需要能够更新到mongo表中
- `run`: 入口函数，负责检查task_id，异步执行main, 返回task_id
- `app`: 获取flask app对象，用于启动web服务

### 环境变量

- `DISABLE_STREAM_HANDLER`: 禁用控制台日志输出
