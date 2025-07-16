# PostgreSQL ORM对象生成器

这个程序使用 SQLAlchemy 连接 PostgreSQL 数据库，并生成以 `t_` 开头的数据表对应的SQLAlchemy ORM类定义。

## 功能特性

- ✅ 连接 PostgreSQL 数据库
- ✅ 自动发现以 `t_` 开头的数据表
- ✅ 使用 SQLAlchemy 反射功能生成ORM类定义
- ✅ 自动生成完整的models.py文件
- ✅ 支持多种PostgreSQL数据类型映射
- ✅ 自动处理主键、外键、关系定义
- ✅ 支持自定义表名前缀
- ✅ 完整的错误处理和日志记录

## 安装依赖

```bash
pip install -r requirements.txt
```

## 数据库配置

### 方法1：使用环境变量

创建 `.env` 文件：

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

### 方法2：直接修改代码

在 `app.py` 中修改 `DATABASE_CONFIG` 字典：

```python
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'your_database_name',
    'username': 'your_username',
    'password': 'your_password'
}
```

## 使用方法

### 基本使用

```bash
python app.py
```

### 编程方式使用

```python
from app import PostgreSQLORMGenerator

# 创建ORM生成器实例
generator = PostgreSQLORMGenerator()

# 连接数据库
if generator.connect():
    # 生成并打印ORM类定义
    generator.print_orm_classes("t_")
    
    # 生成完整的ORM文件
    output_file = generator.generate_complete_orm_file("t_", "models.py")
    
    # 获取ORM类代码字典
    orm_codes = generator.generate_orm_classes("t_")
    
    # 关闭连接
    generator.close()
```

## 主要类和方法

### PostgreSQLORMGenerator 类

#### 主要方法：

- `connect()` - 连接到 PostgreSQL 数据库
- `get_table_names(prefix="t_")` - 获取指定前缀的表名列表
- `generate_orm_classes(prefix="t_")` - 生成ORM类代码字典
- `generate_complete_orm_file(prefix="t_", output_file="models.py")` - 生成完整的ORM文件
- `print_orm_classes(prefix="t_")` - 打印ORM类定义
- `to_class_name(table_name)` - 将表名转换为类名
- `get_python_type_mapping(sqlalchemy_type)` - 数据类型映射
- `close()` - 关闭数据库连接

## 输出示例

程序运行后会输出类似以下内容：

```
正在生成以 't_' 开头的表对应的ORM类...
================================================================================
PostgreSQL表结构对应的SQLAlchemy ORM类定义
================================================================================

# 导入语句:
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Numeric, ForeignKey, BigInteger, SmallInteger, Float, Date, Time, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

# 表: t_users
------------------------------------------------------------
class Users(Base):
    __tablename__ = 't_users'

    id = Column(Integer, primary_key=True)
    username = Column(String(50), nullable=False)
    email = Column(String(100), nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)

    def __repr__(self):
        return f'<Users({self.id})>'

正在生成完整的ORM文件...
✅ ORM文件已生成: models.py
您可以在项目中直接导入和使用这些ORM类
```

## 注意事项

1. 确保 PostgreSQL 服务正在运行
2. 确保数据库用户有足够的权限访问表结构
3. 程序默认查找 `public` 模式下的表
4. 支持自定义表名前缀，不仅限于 `t_`
5. 表名会自动转换为Pascal风格的类名（例如：`t_user_info` → `UserInfo`）
6. 程序会自动生成`models.py`文件，可直接在项目中使用
7. 支持外键关系的自动识别和relationship定义

## 错误处理

程序包含完整的错误处理和日志记录：

- 数据库连接错误
- 表反射错误
- ORM类生成错误
- 文件写入错误
- 权限不足错误

所有错误都会记录到日志中，方便调试。

## 生成的文件

程序会生成以下文件：

1. `models.py` - 包含所有ORM类定义的完整文件
2. 日志输出 - 控制台显示详细的生成过程

生成的`models.py`文件可以直接在您的项目中使用，无需额外修改。 