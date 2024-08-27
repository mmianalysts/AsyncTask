# 模版仓库

## 目录结构

``` shell
├──.github
│  └── workflow             # github action配置文件
├── tests                   # 测试文件夹
│  └── test_example.py      # 测试文件
├── .flake8                 # flake8配置文件
├── .gitignore              # gitignore
├── .pre-commit-config.yaml # pre-commit配置文件
├── .python-version         # python版本, action用，写具体的python版本或范围(>=3.6)
├── pyproject.toml          # 项目配置文件
├── README.md               # 说明文档
├── requirements.txt        # 如果是python包，依赖填到pyproject.toml中, 如果是其他项目，依赖填到这里
└── setup.py                # python包需要此文件作为入口，具体配置在pyproject.toml中
```

## 规范工具

### pre-commit

> 安装： `pip install pre-commit && pre-commit install`

安装后会在`.git/hooks`目录下生成`pre-commit`文件

每次使用`git commit`时会自动运行pre-commit配置的规范检查工具，包含以下工具（可以自己配置其他检查）：

- `black`: 代码格式化工具，会自动格式化代码，在`pyproject.toml`中[配置](https://black.readthedocs.io/en/stable/usage_and_configuration/the_basics.html)
- `isort`: 代码导入排序工具，会自动排序导入，在`pyproject.toml`中[配置](https://pycqa.github.io/isort/docs/configuration/options.html)
- `flake8`: 代码静态检查工具，会检查代码风格，可以在`.flake8`中[配置](https://flake8.pycqa.org/en/latest/user/configuration.html),

  > flake8常用[错误码](https://pycodestyle.pycqa.org/en/latest/intro.html#error-codes)
  > - E501: 行长度超过max-line-length
  > - E2xx: 多余空格
  > - E3xx: 多余空行

### github action

配置文件：`.github/check.yml`

- `black`: 检查代码格式
- `flake8`: 检查代码风格
- `unittest`: 运行测试用例

## 其他资源

- gitmoji: `https://gitmoji.js.org/`
- commit规范：`https://www.conventionalcommits.org/zh-hans/v1.0.0/`
