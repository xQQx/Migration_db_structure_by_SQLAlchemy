# SQLAlchemy ORM模型导入数据库使用指南

这个工具可以将SQLAlchemy ORM模型定义导入到PostgreSQL数据库中，自动创建对应的表结构。

## 功能特性

- ✅ 从models.py文件动态加载ORM模型
- ✅ 分析模型与数据库表结构差异
- ✅ 安全的表创建流程（支持数据备份）
- ✅ 详细的表结构验证
- ✅ 交互式操作界面
- ✅ 完整的错误处理和日志记录

## 使用前提条件

1. 确保已经运行了`app.py`生成了`models.py`文件
2. 确保PostgreSQL数据库正在运行
3. 确保数据库用户有创建表的权限

## 基本使用

### 方法1：直接运行
```bash
python import_models_to_db.py
```

### 方法2：编程方式使用
```python
from import_models_to_db import ModelImporter

# 创建导入器实例
importer = ModelImporter()

# 连接数据库
if importer.connect():
    # 加载模型
    if importer.load_models_from_file("models.py"):
        # 检查差异
        differences = importer.check_table_differences()
        
        # 创建表
        if importer.create_tables():
            # 验证结果
            results = importer.verify_table_structure()
            importer.print_verification_results(results)
    
    # 关闭连接
    importer.close()
```

## 操作流程

### 1. 连接数据库
程序首先连接到PostgreSQL数据库，使用与app.py相同的连接配置。

### 2. 加载ORM模型
从`models.py`文件动态加载所有的ORM模型类。

### 3. 分析表结构差异
比较ORM模型和数据库中现有表的差异，分为：
- **新表** - 模型中有但数据库中没有的表
- **现有表** - 模型和数据库中都有的表
- **孤立表** - 数据库中有但模型中没有的表

### 4. 用户确认
程序会显示差异分析结果，询问用户是否继续操作。

### 5. 数据备份（可选）
如果有现有表需要更新，程序会询问是否需要备份现有数据。

### 6. 创建表结构
执行表结构创建/更新操作。

### 7. 验证结果
验证表结构是否正确创建，并显示详细的验证结果。

## 示例输出

```
正在加载ORM模型...
成功加载 3 个模型类:
  - Users -> t_users
  - Orders -> t_orders
  - Products -> t_products

分析表结构差异...

发现以下差异:
  需要创建的新表: ['t_users', 't_orders']
  现有表: ['t_products']
  数据库中的孤立表: ['t_old_data']

是否继续创建/更新表结构? (y/n): y

是否需要备份现有数据? (y/n): y

正在备份现有数据...
数据已备份到: backup_20231215_143022

正在创建/更新表结构...
表结构创建/更新成功!

正在验证表结构...

================================================================================
表结构验证结果
================================================================================

表: t_users
------------------------------------------------------------
✅ 表存在

列信息 (5个):
  id                   INTEGER         NOT NULL
  username             VARCHAR(50)     NOT NULL
  email                VARCHAR(100)    NOT NULL
  created_at           TIMESTAMP       NOT NULL
  updated_at           TIMESTAMP       NULL

主键: id

✅ 模型导入完成!
您现在可以使用生成的ORM类来操作数据库了。
```

## 主要类和方法

### ModelImporter 类

#### 主要方法：

- `connect()` - 连接到数据库
- `load_models_from_file(model_file)` - 从文件加载ORM模型
- `check_table_differences()` - 检查表结构差异
- `create_tables(drop_existing=False)` - 创建表结构
- `verify_table_structure()` - 验证表结构
- `backup_existing_data(table_names=None)` - 备份现有数据
- `close()` - 关闭数据库连接

## 安全特性

### 1. 数据备份
- 自动备份现有表数据到SQL文件
- 备份文件按时间戳命名，避免冲突
- 支持选择性备份指定表

### 2. 差异分析
- 详细分析模型与数据库的差异
- 清晰显示需要创建的新表和现有表
- 识别数据库中的孤立表

### 3. 交互式确认
- 用户可以选择是否继续操作
- 可以选择是否备份数据
- 避免意外的数据丢失

### 4. 结构验证
- 验证表是否正确创建
- 检查列、主键、外键、索引等
- 详细的验证结果报告

## 错误处理

程序包含完整的错误处理：

- 数据库连接错误
- 模型文件加载错误
- 表创建失败
- 数据备份失败
- 权限不足错误

所有错误都会记录到日志中，方便调试和问题排查。

## 备份文件

备份文件会保存在以时间戳命名的目录中，例如：
```
backup_20231215_143022/
  ├── t_users.sql
  ├── t_orders.sql
  └── t_products.sql
```

每个SQL文件包含对应表的所有数据的INSERT语句，可以用于数据恢复。

## 注意事项

1. 确保数据库用户有足够的权限创建表
2. 备份数据会占用磁盘空间，请定期清理
3. 对于大表，备份操作可能需要较长时间
4. 程序不会自动删除孤立表，需要手动处理
5. 建议在生产环境中使用前先在测试环境中验证

## 故障排除

### 常见问题

1. **模型文件不存在**
   - 确保已经运行`app.py`生成了`models.py`文件

2. **数据库连接失败**
   - 检查数据库服务是否正在运行
   - 验证连接配置（IP、端口、用户名、密码）

3. **权限不足**
   - 确保数据库用户有CREATE TABLE权限
   - 检查是否有足够的磁盘空间

4. **表已存在错误**
   - 程序会自动处理现有表，不应该出现此错误
   - 如果出现，请检查模型定义是否正确

这个工具为SQLAlchemy ORM模型的数据库导入提供了安全、可靠的解决方案。 