# Django Cuiqu 项目

这是一个基于Django的Neo4j知识图谱查询系统。

## 项目结构

```
django_cuiqu/
├── manage.py                # Django管理脚本
├── requirements.txt         # 项目依赖
├── README.md               # 项目说明
├── .gitignore              # Git忽略文件
├── django_cuiqu/           # 主项目配置
│   ├── __init__.py
│   ├── settings.py         # Django设置
│   ├── urls.py             # 主URL配置
│   ├── wsgi.py             # WSGI配置
│   └── asgi.py             # ASGI配置
├── query_neo4j/            # 主要应用
│   ├── models.py           # 数据模型
│   ├── views.py            # 视图函数
│   ├── urls.py             # URL配置
│   └── ...                 # 其他功能模块
├── templates/              # HTML模板
├── static/                 # 静态文件
└── media/                  # 媒体文件
```

## 安装和运行

### 1. 创建虚拟环境

```bash
python -m venv venv
```

### 2. 激活虚拟环境

**Windows:**
```bash
venv\Scripts\activate
```

**Linux/Mac:**
```bash
source venv/bin/activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 初始化数据库

```bash
python manage.py migrate
```

### 5. 创建超级用户（可选）

```bash
python manage.py createsuperuser
```

### 6. 运行开发服务器

```bash
python manage.py runserver
```

项目将在 http://127.0.0.1:8000/ 上运行。

## 主要功能

- Neo4j图数据库查询
- 知识图谱搜索
- 实体关系分析
- 文件上传和处理
- 数据可视化

## API接口

主要的API接口包括：

- `/searchNode` - 节点搜索
- `/searchGraph` - 图谱搜索
- `/searchIndex` - 索引搜索
- `/downLoad` - 文件下载
- `/upload` - 文件上传
- 更多接口请查看 `query_neo4j/urls.py`

## 配置说明

在生产环境中，请确保：

1. 修改 `settings.py` 中的 `SECRET_KEY`
2. 设置 `DEBUG = False`
3. 配置正确的 `ALLOWED_HOSTS`
4. 配置数据库连接
5. 配置Neo4j连接参数

## 依赖要求

- Python 3.8+
- Django 4.2+
- Neo4j数据库
- 其他依赖请查看 `requirements.txt` 