#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用SQLAlchemy连接PostgreSQL并生成t_开头的数据表ORM对象
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Text, Boolean, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import text
from sqlalchemy.types import TypeDecorator
import logging
import re
from datetime import datetime

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 数据库连接配置
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'local'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'sourcedb'),
    'username': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '123456')
}

# 创建基础类
Base = declarative_base()

class PostgreSQLORMGenerator:
    """PostgreSQL ORM对象生成器"""
    
    def __init__(self, config=None):
        """初始化数据库连接"""
        if config is None:
            config = DATABASE_CONFIG
        
        self.config = config
        self.engine = None
        self.metadata = None
        self.session = None
        self.table_objects = {}
        self.orm_classes = {}
        
    def connect(self):
        """连接到PostgreSQL数据库"""
        try:
            # 构建数据库连接URL
            db_url = (
                f"postgresql://{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            
            # 创建引擎
            self.engine = create_engine(
                db_url,
                echo=False,  # 设置为True可以看到SQL语句
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True
            )
            
            # 创建会话
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            
            # 创建元数据对象
            self.metadata = MetaData()
            
            # 测试连接
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info("成功连接到PostgreSQL数据库")
            return True
            
        except Exception as e:
            logger.error(f"连接数据库失败: {str(e)}")
            return False
    
    def get_table_names(self, prefix="t_"):
        """获取以指定前缀开头的表名"""
        try:
            with self.engine.connect() as conn:
                # 查询所有以t_开头的表
                query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE :prefix
                    ORDER BY table_name
                """)
                
                result = conn.execute(query, {"prefix": f"{prefix}%"})
                table_names = [row[0] for row in result.fetchall()]
                
                logger.info(f"找到 {len(table_names)} 个以 '{prefix}' 开头的表")
                return table_names
                
        except Exception as e:
            logger.error(f"获取表名失败: {str(e)}")
            return []
    
    def reflect_table(self, table_name):
        """反射单个表结构"""
        try:
            # 反射表结构
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            return table
            
        except Exception as e:
            logger.error(f"反射表 {table_name} 失败: {str(e)}")
            return None
    
    def get_python_type_mapping(self, sqlalchemy_type, column_name=None, target_database=None):
        """将SQLAlchemy类型映射到Python类型字符串，支持跨数据库兼容性"""
        type_str = str(sqlalchemy_type)
        
        # 跨数据库兼容的通用类型映射
        # 使用SQLAlchemy通用类型而不是特定数据库类型
        cross_db_type_mappings = {
            'INTEGER': 'Integer',
            'BIGINT': 'BigInteger', 
            'SMALLINT': 'SmallInteger',
            'VARCHAR': 'String',
            'TEXT': 'Text',
            'CLOB': 'Text',  # Oracle large text
            'NVARCHAR': 'String',  # SQL Server unicode
            'NTEXT': 'Text',  # SQL Server unicode text
            'CHAR': 'String',
            'NCHAR': 'String',  # SQL Server unicode char
            'BOOLEAN': 'Boolean',
            'BIT': 'Boolean',  # SQL Server bit type
            'TINYINT': 'SmallInteger',  # MySQL/SQL Server
            'DATE': 'Date',
            'TIME': 'Time',
            'DATETIME': 'DateTime',
            'DATETIME2': 'DateTime',  # SQL Server
            'TIMESTAMP': 'DateTime',
            'TIMESTAMPTZ': 'DateTime',  # PostgreSQL with timezone
            'NUMERIC': 'Numeric',
            'DECIMAL': 'Numeric',
            'NUMBER': 'Numeric',  # Oracle
            'MONEY': 'Numeric',  # SQL Server
            'REAL': 'Float',
            'FLOAT': 'Float',
            'DOUBLE': 'Float',
            'BINARY_FLOAT': 'Float',  # Oracle
            'BINARY_DOUBLE': 'Float',  # Oracle
            'JSON': 'JSON',
            'JSONB': 'JSON',  # PostgreSQL
            'UUID': 'String',
            'UNIQUEIDENTIFIER': 'String',  # SQL Server GUID
            'ARRAY': 'ARRAY',  # PostgreSQL
            'VARBINARY': 'LargeBinary',
            'BLOB': 'LargeBinary',
            'LONGBLOB': 'LargeBinary',  # MySQL
            'BYTEA': 'LargeBinary',  # PostgreSQL
            'RAW': 'LargeBinary',  # Oracle
            'LONGRAW': 'LargeBinary',  # Oracle
            'IMAGE': 'LargeBinary',  # SQL Server legacy
            'BINARY': 'LargeBinary'
        }
        
        # 检查是否包含长度信息的类型
        if 'VARCHAR' in type_str or 'NVARCHAR' in type_str:
            match = re.search(r'(?:N?VARCHAR)\((\d+)\)', type_str)
            if match:
                length = match.group(1)
                # 对于非常大的VARCHAR，使用Text类型
                if int(length) > 8000:  # SQL Server varchar(max) 等情况
                    return 'Text'
                return f'String({length})'
            else:
                # 没有长度的VARCHAR，根据字段名和目标数据库推测
                return self._get_cross_db_string_type(column_name, target_database)
        
        if 'CHAR' in type_str or 'NCHAR' in type_str:
            match = re.search(r'(?:N?CHAR)\((\d+)\)', type_str)
            if match:
                length = match.group(1)
                return f'String({length})'
            else:
                return 'String(255)'  # 默认长度
        
        if any(keyword in type_str.upper() for keyword in ['NUMERIC', 'DECIMAL', 'NUMBER']):
            match = re.search(r'(?:NUMERIC|DECIMAL|NUMBER)\((\d+)(?:,\s*(\d+))?\)', type_str)
            if match:
                precision = match.group(1)
                scale = match.group(2) if match.group(2) else '0'
                return f'Numeric({precision}, {scale})'
        
        # 处理特殊的数据库特定类型
        if 'ENUM' in type_str.upper():
            return 'String(255)'  # MySQL ENUM转换为String
        
        if 'SET' in type_str.upper():
            return 'String(255)'  # MySQL SET转换为String
        
        # 查找基础类型映射
        for sql_type, python_type in cross_db_type_mappings.items():
            if sql_type in type_str.upper():
                if python_type == 'String':
                    return self._get_cross_db_string_type(column_name, target_database)
                return python_type
        
        # 默认类型，给一个跨数据库兼容的长度
        return self._get_cross_db_string_type(column_name, target_database)
    
    def _get_cross_db_string_type(self, column_name, target_database=None):
        """根据字段名和目标数据库推测跨数据库兼容的字符串类型"""
        if not column_name:
            return 'String(255)'  # 安全的默认长度
        
        column_lower = column_name.lower()
        
        # 根据字段名推测长度，考虑不同数据库的限制
        if any(keyword in column_lower for keyword in ['uuid', 'guid', 'uniqueidentifier']):
            return 'String(36)'  # UUID标准长度
        elif any(keyword in column_lower for keyword in ['ip', 'addr', 'address']):
            return 'String(45)'  # IPv6最大长度
        elif any(keyword in column_lower for keyword in ['email', 'mail']):
            return 'String(320)'  # 邮箱地址最大长度 (64@256)
        elif any(keyword in column_lower for keyword in ['phone', 'tel', 'mobile', 'fax']):
            return 'String(20)'  # 电话号码
        elif any(keyword in column_lower for keyword in ['name', 'title', 'label']):
            return 'String(100)'  # 名称类
        elif any(keyword in column_lower for keyword in ['code', 'no', 'number', 'id']):
            return 'String(50)'  # 编号类
        elif any(keyword in column_lower for keyword in ['url', 'uri', 'link']):
            return 'String(2048)'  # URL类，考虑长URL
        elif any(keyword in column_lower for keyword in ['path', 'file', 'directory']):
            return 'String(500)'  # 路径类
        elif any(keyword in column_lower for keyword in ['desc', 'description', 'remark', 'note', 'comment']):
            return 'Text'  # 描述类使用Text
        elif any(keyword in column_lower for keyword in ['content', 'data', 'value', 'param']):
            return 'Text'  # 内容类使用Text
        elif any(keyword in column_lower for keyword in ['json', 'xml', 'config', 'setting']):
            return 'Text'  # 结构化数据使用Text
        else:
            # 针对不同数据库的默认长度优化
            if target_database == 'oracle':
                return 'String(4000)'  # Oracle VARCHAR2最大长度
            elif target_database == 'mssql':
                return 'String(4000)'  # SQL Server nvarchar安全长度
            elif target_database == 'mysql':
                return 'String(255)'  # MySQL varchar默认长度
            else:
                return 'String(255)'  # 通用默认长度
    
    def _get_cross_db_engine_args(self, target_database=None):
        """获取跨数据库兼容的引擎参数"""
        if target_database == 'mysql':
            return {
                'mysql_engine': 'InnoDB',
                'mysql_charset': 'utf8mb4',
                'mysql_collate': 'utf8mb4_unicode_ci'
            }
        elif target_database == 'oracle':
            return {
                'oracle_tablespace': None,  # 可以后续指定
            }
        elif target_database == 'mssql':
            return {
                # SQL Server 特定选项
            }
        else:
            return {}
    
    def _is_cross_db_large_data_type(self, python_type):
        """判断是否为跨数据库的大型数据类型（不适合作为主键）"""
        large_types = ['Text', 'LargeBinary', 'JSON', 'ARRAY', 'CLOB', 'BLOB']
        return any(large_type in python_type for large_type in large_types)
    
    def generate_cross_db_compatible_orm_file(self, prefix="t_", output_file="models.py", target_database=None):
        """生成跨数据库兼容的ORM文件"""
        orm_codes = self.generate_orm_classes(prefix, target_database)
        
        if not orm_codes:
            logger.warning("没有生成任何ORM类")
            return None
        
        # 构建完整的文件内容
        file_content = '#!/usr/bin/env python3\n'
        file_content += '# -*- coding: utf-8 -*-\n'
        file_content += '"""\n'
        file_content += f'由PostgreSQL表结构自动生成的跨数据库兼容SQLAlchemy ORM模型\n'
        file_content += f'目标数据库: {target_database or "通用"}\n'
        file_content += f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        file_content += '\n'
        file_content += '本文件使用SQLAlchemy通用类型，支持以下数据库:\n'
        file_content += '- MySQL / MariaDB\n'
        file_content += '- Oracle Database\n'
        file_content += '- Microsoft SQL Server\n'
        file_content += '- PostgreSQL\n'
        file_content += '- SQLite\n'
        file_content += '- 其他支持SQLAlchemy的数据库\n'
        file_content += '"""\n\n'
        
        # 导入语句 - 使用通用类型
        file_content += 'from sqlalchemy import (\n'
        file_content += '    Column, Integer, BigInteger, SmallInteger, String, Text, Boolean, \n'
        file_content += '    Numeric, Float, Date, Time, DateTime, LargeBinary, JSON, ForeignKey, Index\n'
        file_content += ')\n'
        file_content += 'from sqlalchemy.ext.declarative import declarative_base\n'
        file_content += 'from sqlalchemy.orm import relationship\n'
        file_content += 'from sqlalchemy.dialects import mysql, oracle, mssql, postgresql, sqlite\n'
        file_content += '\n'
        file_content += '# 跨数据库兼容性说明:\n'
        file_content += '# - 使用通用SQLAlchemy类型确保跨数据库兼容性\n'
        file_content += '# - 包含索引定义以优化查询性能\n'
        file_content += '# - 智能处理主键，避免MySQL键长度限制\n'
        file_content += '\n'
        
        # Base类
        file_content += 'Base = declarative_base()\n\n'
        
        # 添加跨数据库兼容性配置类
        file_content += 'class CrossDatabaseMixin:\n'
        file_content += '    """跨数据库兼容性混入类"""\n'
        file_content += '    \n'
        file_content += '    @classmethod\n'
        file_content += '    def configure_for_database(cls, engine):\n'
        file_content += '        """根据数据库类型进行配置调整"""\n'
        file_content += '        dialect_name = engine.dialect.name\n'
        file_content += '        \n'
        file_content += '        if dialect_name == "mysql":\n'
        file_content += '            # MySQL特定配置\n'
        file_content += '            if hasattr(cls.__table__, "kwargs"):\n'
        file_content += '                cls.__table__.kwargs.update({\n'
        file_content += '                    "mysql_engine": "InnoDB",\n'
        file_content += '                    "mysql_charset": "utf8mb4",\n'
        file_content += '                    "mysql_collate": "utf8mb4_unicode_ci"\n'
        file_content += '                })\n'
        file_content += '        elif dialect_name == "oracle":\n'
        file_content += '            # Oracle特定配置\n'
        file_content += '            pass\n'
        file_content += '        elif dialect_name == "mssql":\n'
        file_content += '            # SQL Server特定配置\n'
        file_content += '            pass\n'
        file_content += '        \n'
        file_content += '        return cls\n\n'
        
        # 添加所有ORM类
        for table_name, orm_code in orm_codes.items():
            file_content += orm_code + '\n\n'
        
        # 添加跨数据库配置函数
        file_content += '\n# 跨数据库配置函数\n'
        file_content += 'def configure_all_models_for_database(engine):\n'
        file_content += '    """为所有模型配置数据库特定设置"""\n'
        file_content += '    for cls in Base.registry._class_registry.values():\n'
        file_content += '        if hasattr(cls, "configure_for_database"):\n'
        file_content += '            cls.configure_for_database(engine)\n'
        file_content += '\n'
        file_content += '# 使用示例:\n'
        file_content += '# from sqlalchemy import create_engine\n'
        file_content += '# engine = create_engine("mysql://user:password@localhost/database")\n'
        file_content += '# configure_all_models_for_database(engine)\n'
        file_content += '# Base.metadata.create_all(engine)\n'
        
        # 写入文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(file_content)
        
        logger.info(f"跨数据库兼容ORM文件已生成: {output_file}")
        return output_file
    
    def to_class_name(self, table_name):
        """将表名转换为类名"""
        # 移除前缀（如t_）
        if table_name.startswith('t_'):
            table_name = table_name[2:]
        
        # 转换为PascalCase
        parts = table_name.split('_')
        class_name = ''.join(word.capitalize() for word in parts)
        
        return class_name
    
    def generate_orm_class_code(self, table, target_database=None):
        """生成跨数据库兼容的ORM类的Python代码"""
        class_name = self.to_class_name(table.name)
        
        # 开始构建类定义，添加跨数据库混入
        class_code = f"class {class_name}(CrossDatabaseMixin, Base):\n"
        class_code += f"    __tablename__ = '{table.name}'\n\n"
        
        # 检查是否有主键
        primary_key_columns = [col for col in table.columns if col.primary_key]
        
        # 如果没有主键，智能选择主键列
        if not primary_key_columns:
            logger.warning(f"表 {table.name} 没有主键，将智能选择主键列")
            primary_key_columns = self._select_smart_primary_key(table, target_database)
        
        # 添加列定义
        for column in table.columns:
            # 构建Column参数列表
            column_args = []
            column_kwargs = []
            
            # 类型（位置参数）- 使用跨数据库兼容的类型映射
            python_type = self.get_python_type_mapping(column.type, column.name, target_database)
            column_args.append(python_type)
            
            # 外键（位置参数）
            for fk in column.foreign_keys:
                column_args.append(f"ForeignKey('{fk.column.table.name}.{fk.column.name}')")
            
            # 主键（关键字参数）- 检查是否适合作为主键
            if column.primary_key:
                # 对于大型数据字段，即使被设置为主键，也要移除主键属性
                if self._is_cross_db_large_data_type(python_type):
                    logger.warning(f"表 {table.name} 的列 {column.name} 是大型数据类型，不适合作为主键")
                    column.primary_key = False
                else:
                    column_kwargs.append("primary_key=True")
            
            # 可空性（关键字参数）
            if not column.nullable and not column.primary_key:
                column_kwargs.append("nullable=False")
            
            # 自增（关键字参数）
            if column.autoincrement and column.primary_key and len(primary_key_columns) == 1:
                column_kwargs.append("autoincrement=True")
            
            # 默认值（关键字参数）
            if column.default is not None:
                if hasattr(column.default, 'arg'):
                    default_value = column.default.arg
                    if isinstance(default_value, str):
                        column_kwargs.append(f"default='{default_value}'")
                    else:
                        column_kwargs.append(f"default={default_value}")
            
            # 组合所有参数
            all_args = column_args + column_kwargs
            column_def = f"    {column.name} = Column({', '.join(all_args)})\n"
            class_code += column_def
        
        # 添加索引定义
        indexes = self._get_table_indexes(table, target_database)
        if indexes:
            class_code += "\n"
            for index_def in indexes:
                class_code += f"    {index_def}\n"
        
        # 添加关系定义（如果有外键）
        relationships = []
        for fk in table.foreign_keys:
            parent_table = fk.column.table.name
            parent_class = self.to_class_name(parent_table)
            rel_name = parent_table.replace('t_', '') if parent_table.startswith('t_') else parent_table
            relationships.append(f"    {rel_name} = relationship('{parent_class}')")
        
        if relationships:
            class_code += "\n"
            class_code += "\n".join(relationships)
        
        # 添加__repr__方法
        class_code += f"\n\n    def __repr__(self):\n"
        primary_key_cols = [col.name for col in table.columns if col.primary_key]
        if primary_key_cols:
            pk_col = primary_key_cols[0]
            class_code += f"        return f'<{class_name}({{self.{pk_col}}})>'"
        else:
            class_code += f"        return f'<{class_name}>'"
        
        return class_code
    
    def _select_smart_primary_key(self, table, target_database=None):
        """智能选择主键列，避免创建过长的复合主键"""
        # 优先选择合适的单列主键
        suitable_single_columns = []
        for column in table.columns:
            python_type = self.get_python_type_mapping(column.type, column.name, target_database)
            
            # 排除大型数据类型
            if self._is_cross_db_large_data_type(python_type):
                continue
                
            # 优先选择ID类型的列
            if any(keyword in column.name.lower() for keyword in ['id', 'uuid', 'guid', 'key']):
                suitable_single_columns.insert(0, column)  # 插入到前面
            else:
                suitable_single_columns.append(column)
        
        # 如果有合适的单列，选择第一个作为主键
        if suitable_single_columns:
            best_column = suitable_single_columns[0]
            best_column.primary_key = True
            logger.info(f"表 {table.name} 选择单列主键: {best_column.name}")
            return [best_column]
        
        # 如果没有合适的单列，尝试创建复合主键
        suitable_columns = []
        estimated_key_length = 0
        max_key_length = 3000 if target_database == 'mysql' else 8000  # MySQL限制更严格
        
        for column in table.columns:
            python_type = self.get_python_type_mapping(column.type, column.name, target_database)
            
            # 排除大型数据类型
            if self._is_cross_db_large_data_type(python_type):
                continue
            
            # 估算列的键长度
            column_length = self._estimate_column_key_length(column, python_type)
            
            # 检查是否会超过最大键长度
            if estimated_key_length + column_length <= max_key_length:
                suitable_columns.append(column)
                estimated_key_length += column_length
                logger.debug(f"表 {table.name} 添加复合主键列: {column.name} (估算长度: {column_length})")
            else:
                logger.debug(f"表 {table.name} 跳过列 {column.name}，会导致键长度超限")
                break
        
        # 设置复合主键
        if suitable_columns:
            for column in suitable_columns:
                column.primary_key = True
            logger.info(f"表 {table.name} 设置复合主键: {[col.name for col in suitable_columns]} (估算总长度: {estimated_key_length})")
            return suitable_columns
        
        # 如果实在没有合适的列，不设置主键，让表没有主键
        logger.warning(f"表 {table.name} 无法找到合适的主键列，将创建无主键表")
        return []
    
    def _estimate_column_key_length(self, column, python_type):
        """估算列在键中的长度（字节）"""
        # 根据Python类型估算字节长度
        if 'String(' in python_type:
            # 提取字符串长度
            import re
            match = re.search(r'String\((\d+)\)', python_type)
            if match:
                char_length = int(match.group(1))
                # 假设UTF-8编码，每个字符最多4字节
                return char_length * 4
            else:
                return 255 * 4  # 默认长度
        elif python_type in ['Integer', 'BigInteger', 'SmallInteger']:
            return 4 if python_type == 'Integer' else 8 if python_type == 'BigInteger' else 2
        elif python_type in ['Float', 'Numeric']:
            return 8
        elif python_type in ['DateTime', 'Date', 'Time']:
            return 8
        elif python_type == 'Boolean':
            return 1
        else:
            return 255 * 4  # 默认估算
    
    def _get_table_indexes(self, table, target_database=None):
        """获取表的索引定义"""
        indexes = []
        
        # 检查数据库中的现有索引
        try:
            from sqlalchemy import inspect
            inspector = inspect(self.engine)
            
            # 获取表的索引信息
            db_indexes = inspector.get_indexes(table.name)
            
            for idx in db_indexes:
                # 跳过主键索引
                if idx.get('name', '').lower().startswith('primary'):
                    continue
                
                # 构建索引定义
                index_name = idx.get('name', f"idx_{table.name}_{idx['column_names'][0]}")
                columns = idx['column_names']
                is_unique = idx.get('unique', False)
                
                # 生成索引定义（不是完整的__table_args__）
                if len(columns) == 1:
                    if is_unique:
                        index_def = f"Index('{index_name}', '{columns[0]}', unique=True)"
                    else:
                        index_def = f"Index('{index_name}', '{columns[0]}')"
                else:
                    columns_str = ', '.join([f"'{col}'" for col in columns])
                    if is_unique:
                        index_def = f"Index('{index_name}', {columns_str}, unique=True)"
                    else:
                        index_def = f"Index('{index_name}', {columns_str})"
                
                indexes.append(index_def)
                logger.info(f"表 {table.name} 检测到索引: {index_name} on {columns} (unique: {is_unique})")
        
        except Exception as e:
            logger.debug(f"获取表 {table.name} 的索引信息失败: {str(e)}")
        
        # 如果没有检测到索引，基于列名推荐一些索引
        if not indexes:
            recommended_indexes = self._recommend_indexes(table, target_database)
            indexes.extend(recommended_indexes)
        
        # 如果有索引，生成__table_args__定义
        if indexes:
            # 合并所有索引到一个__table_args__中
            if len(indexes) == 1:
                return [f"__table_args__ = ({indexes[0]},)"]
            else:
                indexes_str = ',\n        '.join(indexes)
                return [f"__table_args__ = (\n        {indexes_str}\n    )"]
        
        return []
    
    def _recommend_indexes(self, table, target_database=None):
        """基于列名推荐索引"""
        recommended = []
        
        # 常见的应该建索引的列名模式
        index_patterns = [
            # 外键列
            ('fk_', 'Foreign key columns'),
            ('_id', 'ID reference columns'),
            # 时间列
            ('create_time', 'Create time columns'),
            ('update_time', 'Update time columns'),
            ('modified_time', 'Modified time columns'),
            ('date', 'Date columns'),
            ('time', 'Time columns'),
            # 状态列
            ('status', 'Status columns'),
            ('state', 'State columns'),
            ('flag', 'Flag columns'),
            # 用户相关
            ('user_id', 'User ID columns'),
            ('username', 'Username columns'),
            # 其他常见
            ('code', 'Code columns'),
            ('name', 'Name columns'),
            ('type', 'Type columns')
        ]
        
        for column in table.columns:
            column_name = column.name.lower()
            
            # 跳过主键列
            if column.primary_key:
                continue
            
            # 跳过大型数据类型
            python_type = self.get_python_type_mapping(column.type, column.name, target_database)
            if self._is_cross_db_large_data_type(python_type):
                continue
            
            # 检查是否匹配索引模式
            for pattern, description in index_patterns:
                if pattern in column_name:
                    index_name = f"idx_{table.name}_{column.name}"
                    index_def = f"Index('{index_name}', '{column.name}')"
                    recommended.append(index_def)
                    logger.info(f"表 {table.name} 推荐索引: {index_name} on {column.name} ({description})")
                    break
        
        return recommended
    
    def analyze_table_dependencies(self, tables):
        """分析表的依赖关系"""
        dependencies = {}
        
        for table_name, table in tables.items():
            deps = set()
            for fk in table.foreign_keys:
                ref_table = fk.column.table.name
                if ref_table in tables and ref_table != table_name:
                    deps.add(ref_table)
            dependencies[table_name] = deps
        
        return dependencies
    
    def topological_sort(self, dependencies):
        """拓扑排序，按依赖关系排序表"""
        # 创建入度表
        in_degree = {table: 0 for table in dependencies}
        for table in dependencies:
            for dep in dependencies[table]:
                if dep in in_degree:
                    in_degree[dep] += 1
        
        # 找出所有入度为0的表
        queue = [table for table, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            current = queue.pop(0)
            result.append(current)
            
            # 更新依赖当前表的其他表的入度
            for table in dependencies:
                if current in dependencies[table]:
                    in_degree[table] -= 1
                    if in_degree[table] == 0:
                        queue.append(table)
        
        # 如果还有表没有处理（循环依赖），将其添加到结果中
        remaining = set(dependencies.keys()) - set(result)
        result.extend(remaining)
        
        return result
    
    def generate_orm_classes(self, prefix="t_", target_database=None):
        """生成以指定前缀开头的跨数据库兼容ORM类"""
        if not self.engine:
            logger.error("请先连接数据库")
            return {}
        
        table_names = self.get_table_names(prefix)
        tables = {}
        
        # 先反射所有表
        for table_name in table_names:
            try:
                table = self.reflect_table(table_name)
                if table is not None:
                    tables[table_name] = table
                    self.table_objects[table_name] = table
            except Exception as e:
                logger.error(f"反射表 {table_name} 失败: {str(e)}")
                continue
        
        # 分析依赖关系并排序
        dependencies = self.analyze_table_dependencies(tables)
        sorted_tables = self.topological_sort(dependencies)
        
        # 按依赖关系顺序生成ORM类
        orm_codes = {}
        for table_name in sorted_tables:
            if table_name in tables:
                try:
                    table = tables[table_name]
                    # 生成跨数据库兼容的ORM类代码
                    orm_code = self.generate_orm_class_code(table, target_database)
                    orm_codes[table_name] = orm_code
                    
                    logger.info(f"成功生成跨数据库兼容ORM类: {self.to_class_name(table_name)}")
                    
                except Exception as e:
                    logger.error(f"生成ORM类 {table_name} 失败: {str(e)}")
                    continue
        
        return orm_codes
    
    def generate_complete_orm_file(self, prefix="t_", output_file="models.py"):
        """生成完整的ORM文件"""
        orm_codes = self.generate_orm_classes(prefix)
        
        if not orm_codes:
            logger.warning("没有生成任何ORM类")
            return None
        
        # 构建完整的文件内容
        file_content = '#!/usr/bin/env python3\n'
        file_content += '# -*- coding: utf-8 -*-\n'
        file_content += '"""\n'
        file_content += f'由PostgreSQL表结构自动生成的SQLAlchemy ORM模型\n'
        file_content += f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        file_content += '"""\n\n'
        
        # 导入语句
        file_content += 'from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Numeric, ForeignKey, BigInteger, SmallInteger, Float, Date, Time, JSON, Index\n'
        file_content += 'from sqlalchemy.ext.declarative import declarative_base\n'
        file_content += 'from sqlalchemy.orm import relationship\n\n'
        
        # Base类
        file_content += 'Base = declarative_base()\n\n'
        
        # 添加所有ORM类
        for table_name, orm_code in orm_codes.items():
            file_content += orm_code + '\n\n'
        
        # 写入文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(file_content)
        
        logger.info(f"ORM文件已生成: {output_file}")
        return output_file
    
    def print_orm_classes(self, prefix="t_"):
        """打印ORM类定义"""
        orm_codes = self.generate_orm_classes(prefix)
        
        if not orm_codes:
            print("没有找到任何表或生成失败")
            return
        
        print("="*80)
        print("PostgreSQL表结构对应的SQLAlchemy ORM类定义")
        print("="*80)
        
        # 打印导入语句
        print("\n# 导入语句:")
        print("from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Numeric, ForeignKey, BigInteger, SmallInteger, Float, Date, Time, JSON")
        print("from sqlalchemy.ext.declarative import declarative_base")
        print("from sqlalchemy.orm import relationship")
        print("\nBase = declarative_base()\n")
        
        # 打印每个ORM类
        for table_name, orm_code in orm_codes.items():
            print(f"\n# 表: {table_name}")
            print("-" * 60)
            print(orm_code)
            print()
    
    def close(self):
        """关闭数据库连接"""
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()
        logger.info("数据库连接已关闭")
    
    def _is_large_data_type(self, python_type):
        """判断是否为大型数据类型（不适合作为主键）"""
        large_types = ['Text', 'BLOB', 'JSON', 'ARRAY']
        return any(large_type in python_type for large_type in large_types)


def main():
    """主函数 - 演示如何使用跨数据库兼容功能"""
    # 创建ORM生成器
    generator = PostgreSQLORMGenerator()
    
    # 连接数据库
    if not generator.connect():
        print("无法连接到数据库，请检查配置")
        return
    
    try:
        # 生成并打印ORM类
        print("正在生成以 't_' 开头的跨数据库兼容ORM类...")
        generator.print_orm_classes("t_")
        
        # 生成跨数据库兼容的ORM文件
        print("\n正在生成跨数据库兼容的ORM文件...")
        
        # 可以指定目标数据库类型
        target_db = None  # 或者 'mysql', 'oracle', 'mssql' 等
        
        output_file = generator.generate_cross_db_compatible_orm_file(
            "t_", 
            "cross_db_models.py",
            target_db
        )
        
        if output_file:
            print(f"✅ 跨数据库兼容ORM文件已生成: {output_file}")
            print("该文件支持以下数据库:")
            print("  - MySQL / MariaDB")
            print("  - Oracle Database") 
            print("  - Microsoft SQL Server")
            print("  - PostgreSQL")
            print("  - SQLite")
            print("  - 其他支持SQLAlchemy的数据库")
            
            # 显示使用示例
            print("\n" + "="*70)
            print("跨数据库使用示例:")
            print("="*70)
            print("from cross_db_models import Base, configure_all_models_for_database")
            print("from sqlalchemy import create_engine")
            print("from sqlalchemy.orm import sessionmaker")
            print("")
            print("# 连接到MySQL")
            print("engine = create_engine('mysql+pymysql://user:password@localhost/database')")
            print("configure_all_models_for_database(engine)")
            print("Base.metadata.create_all(engine)")
            print("")
            print("# 连接到Oracle")
            print("engine = create_engine('oracle://user:password@localhost:1521/database')")
            print("configure_all_models_for_database(engine)")
            print("Base.metadata.create_all(engine)")
            print("")
            print("# 连接到SQL Server")
            print("engine = create_engine('mssql+pyodbc://user:password@localhost/database')")
            print("configure_all_models_for_database(engine)")
            print("Base.metadata.create_all(engine)")
    
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    
    finally:
        # 关闭连接
        generator.close()


if __name__ == "__main__":
    main()
