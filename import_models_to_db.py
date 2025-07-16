#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实现将SQLAlchemy ORM模型导入数据库
支持从models.py文件读取ORM模型并在数据库中创建对应的表结构
针对MySQL/MariaDB、Oracle、SQL Server等多种数据库自动优化
"""

import os
import sys
import importlib.util
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import logging
from datetime import datetime
from contextlib import contextmanager

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
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '3308'),
    'database': os.getenv('DB_NAME', 'dbtest'),
    'username': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '123456'),
    'type': os.getenv('DB_TYPE', 'mysql')  # mysql, oracle, mssql, postgresql, sqlite
}

class CrossDatabaseModelImporter:
    """跨数据库兼容的SQLAlchemy ORM模型导入器"""
    
    def __init__(self, config=None):
        """初始化跨数据库模型导入器"""
        if config is None:
            config = DATABASE_CONFIG
        
        self.config = config
        self.engine = None
        self.session = None
        self.metadata = None
        self.models = {}
        self.Base = None
        self.db_type = None
        self.db_dialect = None
        
    def connect(self):
        """连接到数据库"""
        try:
            # 根据数据库类型构建连接URL
            db_url = self._build_database_url()
            
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
            
            # 测试连接并检测数据库类型
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                # 检测数据库类型
                self.db_type = self.engine.dialect.name
                self.db_dialect = self.engine.dialect
                
                # 获取更详细的版本信息
                version_info = self._get_database_version_info(conn)
                
                logger.info(f"成功连接到数据库")
                logger.info(f"数据库类型: {self.db_type}")
                logger.info(f"数据库版本: {version_info}")
                
            return True
            
        except Exception as e:
            logger.error(f"连接数据库失败: {str(e)}")
            return False
    
    def _build_database_url(self):
        """根据数据库类型构建连接URL"""
        db_type = self.config.get('type', 'mysql').lower()
        host = self.config['host']
        port = self.config['port']
        database = self.config['database']
        username = self.config['username']
        password = self.config['password']
        
        if db_type == 'mysql':
            return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        elif db_type == 'oracle':
            return f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}"
        elif db_type == 'mssql':
            return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        elif db_type == 'postgresql':
            return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        elif db_type == 'sqlite':
            return f"sqlite:///{database}"
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")
    
    def _get_database_version_info(self, conn):
        """获取数据库版本信息"""
        try:
            if self.db_type == 'mysql':
                result = conn.execute(text("SELECT VERSION()"))
                return result.scalar()
            elif self.db_type == 'oracle':
                result = conn.execute(text("SELECT * FROM V$VERSION WHERE ROWNUM = 1"))
                return result.scalar()
            elif self.db_type == 'mssql':
                result = conn.execute(text("SELECT @@VERSION"))
                return result.scalar()
            elif self.db_type == 'postgresql':
                result = conn.execute(text("SELECT version()"))
                return result.scalar()
            elif self.db_type == 'sqlite':
                result = conn.execute(text("SELECT sqlite_version()"))
                return f"SQLite {result.scalar()}"
            else:
                return "Unknown"
        except Exception as e:
            logger.warning(f"获取数据库版本信息失败: {str(e)}")
            return "Unknown"
    
    def _is_mysql_family(self):
        """判断是否为MySQL家族数据库（MySQL或MariaDB）"""
        return self.db_type in ['mysql', 'mariadb']
    
    def _is_oracle_family(self):
        """判断是否为Oracle数据库"""
        return self.db_type == 'oracle'
    
    def _is_mssql_family(self):
        """判断是否为SQL Server数据库"""
        return self.db_type == 'mssql'
    
    def _is_postgresql_family(self):
        """判断是否为PostgreSQL数据库"""
        return self.db_type == 'postgresql'
    
    def disable_foreign_key_checks(self):
        """禁用外键检查（针对不同数据库）"""
        try:
            with self.engine.connect() as conn:
                if self._is_mysql_family():
                    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                elif self._is_oracle_family():
                    # Oracle 禁用外键约束
                    conn.execute(text("ALTER SESSION SET CONSTRAINT_CHECK=FALSE"))
                elif self._is_mssql_family():
                    # SQL Server 禁用外键检查
                    conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'"))
                elif self._is_postgresql_family():
                    # PostgreSQL 需要单独禁用每个约束
                    logger.info("PostgreSQL 需要单独处理外键约束")
                
                conn.commit()
                logger.info(f"已禁用{self.db_type}数据库的外键检查")
                return True
        except Exception as e:
            logger.error(f"禁用外键检查失败: {str(e)}")
            return False
    
    def enable_foreign_key_checks(self):
        """启用外键检查（针对不同数据库）"""
        try:
            with self.engine.connect() as conn:
                if self._is_mysql_family():
                    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                elif self._is_oracle_family():
                    # Oracle 启用外键约束
                    conn.execute(text("ALTER SESSION SET CONSTRAINT_CHECK=TRUE"))
                elif self._is_mssql_family():
                    # SQL Server 启用外键检查
                    conn.execute(text("EXEC sp_msforeachtable 'ALTER TABLE ? CHECK CONSTRAINT all'"))
                elif self._is_postgresql_family():
                    # PostgreSQL 需要单独启用每个约束
                    logger.info("PostgreSQL 需要单独处理外键约束")
                
                conn.commit()
                logger.info(f"已启用{self.db_type}数据库的外键检查")
                return True
        except Exception as e:
            logger.error(f"启用外键检查失败: {str(e)}")
            return False
    
    @contextmanager
    def foreign_key_disabled(self):
        """上下文管理器：临时禁用外键检查"""
        if self.db_type in ['mysql', 'mariadb', 'oracle', 'mssql']:
            logger.info(f"进入{self.db_type}数据库外键检查禁用上下文")
            self.disable_foreign_key_checks()
            try:
                yield
            finally:
                self.enable_foreign_key_checks()
                logger.info(f"退出{self.db_type}数据库外键检查禁用上下文")
        else:
            # 对于PostgreSQL和SQLite，直接执行
            yield
    
    def set_database_session_variables(self):
        """设置数据库特定的会话变量以优化导入性能"""
        try:
            with self.engine.connect() as conn:
                if self._is_mysql_family():
                    # MySQL/MariaDB 优化变量
                    session_vars = [
                        "SET FOREIGN_KEY_CHECKS = 0",
                        "SET UNIQUE_CHECKS = 0",
                        "SET AUTOCOMMIT = 0",
                        "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'",
                        "SET SESSION innodb_lock_wait_timeout = 300"
                    ]
                elif self._is_oracle_family():
                    # Oracle 优化变量
                    session_vars = [
                        "ALTER SESSION SET CONSTRAINT_CHECK=FALSE",
                        "ALTER SESSION SET DDL_LOCK_TIMEOUT=300"
                    ]
                elif self._is_mssql_family():
                    # SQL Server 优化变量
                    session_vars = [
                        "SET ARITHABORT OFF",
                        "SET ANSI_WARNINGS OFF"
                    ]
                elif self._is_postgresql_family():
                    # PostgreSQL 优化变量
                    session_vars = [
                        "SET synchronous_commit = off",
                        "SET checkpoint_segments = 32"
                    ]
                else:
                    session_vars = []
                
                for var in session_vars:
                    try:
                        conn.execute(text(var))
                        logger.debug(f"设置会话变量: {var}")
                    except Exception as e:
                        logger.warning(f"设置会话变量失败: {var} - {str(e)}")
                
                conn.commit()
                logger.info(f"{self.db_type}数据库会话变量设置完成")
                return True
        except Exception as e:
            logger.error(f"设置{self.db_type}数据库会话变量失败: {str(e)}")
            return False
    
    def reset_database_session_variables(self):
        """重置数据库会话变量"""
        try:
            with self.engine.connect() as conn:
                if self._is_mysql_family():
                    # 重置MySQL/MariaDB变量
                    reset_vars = [
                        "SET FOREIGN_KEY_CHECKS = 1",
                        "SET UNIQUE_CHECKS = 1",
                        "SET AUTOCOMMIT = 1"
                    ]
                elif self._is_oracle_family():
                    # 重置Oracle变量
                    reset_vars = [
                        "ALTER SESSION SET CONSTRAINT_CHECK=TRUE"
                    ]
                elif self._is_mssql_family():
                    # 重置SQL Server变量
                    reset_vars = [
                        "SET ARITHABORT ON",
                        "SET ANSI_WARNINGS ON"
                    ]
                elif self._is_postgresql_family():
                    # 重置PostgreSQL变量
                    reset_vars = [
                        "SET synchronous_commit = on"
                    ]
                else:
                    reset_vars = []
                
                for var in reset_vars:
                    try:
                        conn.execute(text(var))
                        logger.debug(f"重置会话变量: {var}")
                    except Exception as e:
                        logger.warning(f"重置会话变量失败: {var} - {str(e)}")
                
                conn.commit()
                logger.info(f"{self.db_type}数据库会话变量重置完成")
                return True
        except Exception as e:
            logger.error(f"重置{self.db_type}数据库会话变量失败: {str(e)}")
            return False
    
    def load_models_from_file(self, model_file="models.py"):
        """从文件加载ORM模型"""
        try:
            # 检查文件是否存在
            if not os.path.exists(model_file):
                logger.error(f"模型文件 {model_file} 不存在")
                return False
            
            # 动态导入模型文件
            spec = importlib.util.spec_from_file_location("models", model_file)
            models_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(models_module)
            
            # 获取Base类
            if hasattr(models_module, 'Base'):
                self.Base = models_module.Base
                self.metadata = self.Base.metadata
                logger.info("成功加载Base类")
            else:
                logger.error("模型文件中未找到Base类")
                return False
            
            # 获取所有模型类
            model_classes = []
            for name in dir(models_module):
                obj = getattr(models_module, name)
                if (isinstance(obj, type) and 
                    hasattr(obj, '__tablename__') and 
                    obj != self.Base):
                    model_classes.append(obj)
                    self.models[name] = obj
            
            logger.info(f"成功加载 {len(model_classes)} 个模型类:")
            for model_class in model_classes:
                logger.info(f"  - {model_class.__name__} -> {model_class.__tablename__}")
            
            return True
            
        except Exception as e:
            logger.error(f"加载模型文件失败: {str(e)}")
            return False
    
    def get_existing_tables(self):
        """获取数据库中现有的表"""
        try:
            inspector = inspect(self.engine)
            existing_tables = inspector.get_table_names()
            logger.info(f"数据库中现有 {len(existing_tables)} 个表")
            return existing_tables
            
        except Exception as e:
            logger.error(f"获取现有表列表失败: {str(e)}")
            return []
    
    def check_table_differences(self):
        """检查模型和数据库表之间的差异"""
        if not self.metadata:
            logger.error("请先加载模型")
            return None
        
        existing_tables = self.get_existing_tables()
        model_tables = list(self.metadata.tables.keys())
        
        # 分析差异
        differences = {
            'new_tables': [],      # 模型中有但数据库中没有的表
            'existing_tables': [], # 模型和数据库中都有的表
            'orphaned_tables': []  # 数据库中有但模型中没有的表
        }
        
        for table_name in model_tables:
            if table_name not in existing_tables:
                differences['new_tables'].append(table_name)
            else:
                differences['existing_tables'].append(table_name)
        
        for table_name in existing_tables:
            if table_name not in model_tables:
                differences['orphaned_tables'].append(table_name)
        
        return differences
    
    def analyze_table_dependencies(self):
        """分析表的依赖关系"""
        if not self.metadata:
            return {}
        
        dependencies = {}
        
        for table_name, table in self.metadata.tables.items():
            deps = set()
            for fk in table.foreign_keys:
                ref_table = fk.column.table.name
                if ref_table in self.metadata.tables and ref_table != table_name:
                    deps.add(ref_table)
            dependencies[table_name] = deps
        
        return dependencies
    
    def topological_sort_tables(self, dependencies):
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
    
    def create_tables_ordered(self, drop_existing=False):
        """按依赖关系排序创建表"""
        if not self.engine or not self.metadata:
            logger.error("请先连接数据库并加载模型")
            return False
        
        try:
            # 分析依赖关系
            dependencies = self.analyze_table_dependencies()
            sorted_tables = self.topological_sort_tables(dependencies)
            
            logger.info("表创建顺序:")
            for i, table_name in enumerate(sorted_tables, 1):
                deps = dependencies.get(table_name, set())
                deps_str = f" (依赖: {', '.join(deps)})" if deps else " (无依赖)"
                logger.info(f"  {i}. {table_name}{deps_str}")
            
            # 使用外键检查禁用上下文管理器
            with self.foreign_key_disabled():
                # 设置数据库特定的优化变量
                self.set_database_session_variables()
                
                try:
                    # 如果需要删除现有表（按反向顺序删除）
                    if drop_existing:
                        logger.warning("正在删除现有表...")
                        # 先删除索引，再删除表
                        for table_name in reversed(sorted_tables):
                            if table_name in self.metadata.tables:
                                self._drop_table_indexes(table_name)
                                table = self.metadata.tables[table_name]
                                table.drop(bind=self.engine, checkfirst=True)
                                logger.info(f"  已删除表: {table_name}")
                        logger.info("现有表已删除")
                    
                    # 设置数据库特定的引擎参数
                    engine_args = self._get_database_engine_args()
                    
                    # 为所有表设置数据库特定的引擎参数
                    if engine_args:
                        for table in self.metadata.tables.values():
                            table.kwargs.update(engine_args)
                    
                    # 分阶段创建：先创建表结构，再创建索引
                    logger.info("阶段1: 创建表结构...")
                    
                    created_tables = []
                    failed_tables = []
                    
                    # 第一阶段：创建表结构（不包括索引）
                    for table_name in sorted_tables:
                        if table_name in self.metadata.tables:
                            try:
                                table = self.metadata.tables[table_name]
                                # 临时移除索引定义
                                original_indexes = self._temporarily_remove_indexes(table)
                                
                                # 创建表结构
                                table.create(bind=self.engine, checkfirst=True)
                                created_tables.append(table_name)
                                logger.info(f"  ✅ 成功创建表结构: {table_name}")
                                
                                # 恢复索引定义
                                self._restore_indexes(table, original_indexes)
                                
                            except Exception as e:
                                failed_tables.append((table_name, str(e)))
                                logger.error(f"  ❌ 创建表结构失败: {table_name} - {str(e)}")
                    
                    # 提交表结构创建
                    if self.session:
                        self.session.commit()
                    
                    # 第二阶段：创建索引
                    if created_tables:
                        logger.info("阶段2: 创建索引...")
                        index_created_count = 0
                        index_failed_count = 0
                        
                        for table_name in created_tables:
                            if table_name in self.metadata.tables:
                                try:
                                    table = self.metadata.tables[table_name]
                                    index_count = self._create_table_indexes(table)
                                    if index_count > 0:
                                        index_created_count += index_count
                                        logger.info(f"  ✅ 为表 {table_name} 创建了 {index_count} 个索引")
                                    
                                except Exception as e:
                                    index_failed_count += 1
                                    logger.error(f"  ❌ 为表 {table_name} 创建索引失败: {str(e)}")
                        
                        if index_created_count > 0:
                            logger.info(f"索引创建完成 - 成功: {index_created_count}, 失败: {index_failed_count}")
                    
                    # 验证创建结果
                    new_tables = self.get_existing_tables()
                    
                    logger.info(f"表创建完成 - 成功: {len(created_tables)}, 失败: {len(failed_tables)}")
                    
                    if failed_tables:
                        logger.error("创建失败的表:")
                        for table_name, error in failed_tables:
                            logger.error(f"  - {table_name}: {error}")
                    
                    return len(failed_tables) == 0
                    
                finally:
                    # 重置数据库会话变量
                    self.reset_database_session_variables()
            
        except Exception as e:
            logger.error(f"创建表失败: {str(e)}")
            return False
    
    def _temporarily_remove_indexes(self, table):
        """临时移除表的索引定义"""
        original_indexes = []
        
        # 备份原始索引
        if hasattr(table, 'indexes'):
            original_indexes = list(table.indexes)
            # 清空索引
            table.indexes.clear()
        
        return original_indexes
    
    def _restore_indexes(self, table, original_indexes):
        """恢复表的索引定义"""
        if hasattr(table, 'indexes') and original_indexes:
            table.indexes.update(original_indexes)
    
    def _create_table_indexes(self, table):
        """为表创建索引"""
        index_count = 0
        
        try:
            # 检查表是否有索引定义
            if hasattr(table, 'indexes') and table.indexes:
                for index in table.indexes:
                    try:
                        # 检查索引是否已存在
                        if not self._index_exists(table.name, index.name):
                            index.create(bind=self.engine)
                            index_count += 1
                            logger.debug(f"创建索引: {index.name} on {table.name}")
                        else:
                            logger.debug(f"索引已存在: {index.name} on {table.name}")
                    except Exception as e:
                        logger.warning(f"创建索引 {index.name} 失败: {str(e)}")
            
            return index_count
            
        except Exception as e:
            logger.error(f"为表 {table.name} 创建索引失败: {str(e)}")
            return 0
    
    def _index_exists(self, table_name, index_name):
        """检查索引是否已存在"""
        try:
            inspector = inspect(self.engine)
            existing_indexes = inspector.get_indexes(table_name)
            return any(idx.get('name') == index_name for idx in existing_indexes)
        except Exception:
            return False
    
    def _drop_table_indexes(self, table_name):
        """删除表的所有索引"""
        try:
            inspector = inspect(self.engine)
            indexes = inspector.get_indexes(table_name)
            
            with self.engine.connect() as conn:
                for idx in indexes:
                    index_name = idx.get('name')
                    if index_name and not index_name.lower().startswith('primary'):
                        try:
                            if self._is_mysql_family():
                                conn.execute(text(f"DROP INDEX {index_name} ON {table_name}"))
                            elif self._is_oracle_family():
                                conn.execute(text(f"DROP INDEX {index_name}"))
                            elif self._is_mssql_family():
                                conn.execute(text(f"DROP INDEX {index_name} ON {table_name}"))
                            elif self._is_postgresql_family():
                                conn.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
                            
                            logger.debug(f"删除索引: {index_name} on {table_name}")
                        except Exception as e:
                            logger.warning(f"删除索引 {index_name} 失败: {str(e)}")
                conn.commit()
                
        except Exception as e:
            logger.debug(f"删除表 {table_name} 的索引失败: {str(e)}")
    
    def _get_database_engine_args(self):
        """获取数据库特定的引擎参数"""
        if self._is_mysql_family():
            return {
                'mysql_engine': 'InnoDB',
                'mysql_charset': 'utf8mb4',
                'mysql_collate': 'utf8mb4_unicode_ci'
            }
        elif self._is_oracle_family():
            return {
                # Oracle 特定参数
            }
        elif self._is_mssql_family():
            return {
                # SQL Server 特定参数  
            }
        elif self._is_postgresql_family():
            return {
                # PostgreSQL 特定参数
            }
        else:
            return {}
    
    def create_tables(self, drop_existing=False):
        """创建数据库表"""
        if not self.engine or not self.metadata:
            logger.error("请先连接数据库并加载模型")
            return False
        
        try:
            # 检查差异
            differences = self.check_table_differences()
            if differences:
                logger.info("表结构差异分析:")
                logger.info(f"  新表 ({len(differences['new_tables'])}): {differences['new_tables']}")
                logger.info(f"  现有表 ({len(differences['existing_tables'])}): {differences['existing_tables']}")
                logger.info(f"  孤立表 ({len(differences['orphaned_tables'])}): {differences['orphaned_tables']}")
            
            # 特别提示MySQL/MariaDB的外键检查处理
            if self._is_mysql_family():
                logger.info(f"检测到{self.db_type}数据库，将自动禁用外键检查以确保导入成功")
            
            # 使用排序创建表
            return self.create_tables_ordered(drop_existing)
            
        except Exception as e:
            logger.error(f"创建表失败: {str(e)}")
            return False
    
    def verify_table_structure(self):
        """验证表结构是否正确创建"""
        if not self.engine or not self.metadata:
            logger.error("请先连接数据库并加载模型")
            return False
        
        try:
            inspector = inspect(self.engine)
            verification_results = {}
            
            for table_name, table in self.metadata.tables.items():
                result = {
                    'exists': False,
                    'columns': {},
                    'primary_keys': [],
                    'foreign_keys': [],
                    'indexes': []
                }
                
                # 检查表是否存在
                if inspector.has_table(table_name):
                    result['exists'] = True
                    
                    # 检查列
                    db_columns = inspector.get_columns(table_name)
                    for col in db_columns:
                        result['columns'][col['name']] = {
                            'type': str(col['type']),
                            'nullable': col['nullable'],
                            'default': col['default']
                        }
                    
                    # 检查主键
                    pk_constraint = inspector.get_pk_constraint(table_name)
                    if pk_constraint:
                        result['primary_keys'] = pk_constraint['constrained_columns']
                    
                    # 检查外键
                    fk_constraints = inspector.get_foreign_keys(table_name)
                    for fk in fk_constraints:
                        result['foreign_keys'].append({
                            'constrained_columns': fk['constrained_columns'],
                            'referred_table': fk['referred_table'],
                            'referred_columns': fk['referred_columns']
                        })
                    
                    # 检查索引
                    indexes = inspector.get_indexes(table_name)
                    for idx in indexes:
                        result['indexes'].append({
                            'name': idx['name'],
                            'columns': idx['column_names'],
                            'unique': idx['unique']
                        })
                
                verification_results[table_name] = result
            
            return verification_results
            
        except Exception as e:
            logger.error(f"验证表结构失败: {str(e)}")
            return None
    
    def print_verification_results(self, results):
        """打印验证结果"""
        if not results:
            print("没有验证结果")
            return
        
        print("\n" + "="*80)
        print("表结构验证结果")
        print("="*80)
        
        for table_name, result in results.items():
            print(f"\n表: {table_name}")
            print("-" * 60)
            
            if result['exists']:
                print("✅ 表存在")
                
                print(f"\n列信息 ({len(result['columns'])}个):")
                for col_name, col_info in result['columns'].items():
                    nullable = "NULL" if col_info['nullable'] else "NOT NULL"
                    default = f" DEFAULT {col_info['default']}" if col_info['default'] else ""
                    print(f"  {col_name:<20} {col_info['type']:<15} {nullable}{default}")
                
                if result['primary_keys']:
                    print(f"\n主键: {', '.join(result['primary_keys'])}")
                
                if result['foreign_keys']:
                    print("\n外键:")
                    for fk in result['foreign_keys']:
                        print(f"  {', '.join(fk['constrained_columns'])} -> {fk['referred_table']}.{', '.join(fk['referred_columns'])}")
                
                if result['indexes']:
                    print("\n索引:")
                    for idx in result['indexes']:
                        unique = " (UNIQUE)" if idx['unique'] else ""
                        print(f"  {idx['name']}: {', '.join(idx['columns'])}{unique}")
            else:
                print("❌ 表不存在")
    
    def backup_existing_data(self, table_names=None):
        """备份现有数据"""
        if not self.session:
            logger.error("请先连接数据库")
            return False
        
        try:
            backup_dir = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.makedirs(backup_dir, exist_ok=True)
            
            existing_tables = self.get_existing_tables()
            tables_to_backup = table_names or existing_tables
            
            # 在备份过程中也禁用外键检查（如果是MySQL/MariaDB）
            with self.foreign_key_disabled():
                for table_name in tables_to_backup:
                    if table_name in existing_tables:
                        # 导出数据到SQL文件
                        backup_file = os.path.join(backup_dir, f"{table_name}.sql")
                        
                        with open(backup_file, 'w', encoding='utf-8') as f:
                            # 写入备份信息
                            f.write(f"-- 表 {table_name} 备份\n")
                            f.write(f"-- 备份时间: {datetime.now()}\n")
                            f.write(f"-- 数据库类型: {self.db_type}\n\n")
                            
                            # 如果是MySQL/MariaDB，添加外键检查禁用
                            if self._is_mysql_family():
                                f.write("SET FOREIGN_KEY_CHECKS = 0;\n\n")
                            
                            # 获取表数据
                            result = self.session.execute(text(f"SELECT * FROM {table_name}"))
                            rows = result.fetchall()
                            
                            if rows:
                                columns = result.keys()
                                
                                # 生成INSERT语句
                                for row in rows:
                                    values = []
                                    for value in row:
                                        if value is None:
                                            values.append('NULL')
                                        elif isinstance(value, str):
                                            values.append(f"'{value.replace("'", "''")}'")
                                        else:
                                            values.append(str(value))
                                    
                                    f.write(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n")
                            
                            # 如果是MySQL/MariaDB，重新启用外键检查
                            if self._is_mysql_family():
                                f.write("\nSET FOREIGN_KEY_CHECKS = 1;\n")
                            
                            logger.info(f"已备份表 {table_name} 到 {backup_file}")
            
            logger.info(f"数据备份完成，备份目录: {backup_dir}")
            return backup_dir
            
        except Exception as e:
            logger.error(f"备份数据失败: {str(e)}")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()
        logger.info("数据库连接已关闭")


def main():
    """主函数"""
    importer = CrossDatabaseModelImporter()
    
    # 连接数据库
    if not importer.connect():
        print("无法连接到数据库，请检查配置")
        return
    
    try:
        # 显示数据库信息
        print(f"数据库类型: {importer.db_type}")
        print(f"✅ 检测到{importer.db_type}数据库，将自动处理数据库特定的优化")
        
        # 加载模型
        print("正在加载ORM模型...")
        if not importer.load_models_from_file("cross_db_models.py"):
            print("加载跨数据库模型失败，尝试加载标准模型...")
            if not importer.load_models_from_file("models.py"):
                print("加载模型失败")
                return
        
        # 检查表差异
        print("\n分析表结构差异...")
        differences = importer.check_table_differences()
        
        if differences:
            print(f"\n发现以下差异:")
            print(f"  需要创建的新表: {differences['new_tables']}")
            print(f"  现有表: {differences['existing_tables']}")
            print(f"  数据库中的孤立表: {differences['orphaned_tables']}")
            
            # 询问用户是否继续
            if differences['new_tables'] or differences['existing_tables']:
                choice = input("\n是否继续创建/更新表结构? (y/n): ").lower()
                if choice != 'y':
                    print("操作已取消")
                    return
                
                # 询问是否需要备份
                if differences['existing_tables']:
                    backup_choice = input("是否需要备份现有数据? (y/n): ").lower()
                    if backup_choice == 'y':
                        print("正在备份现有数据...")
                        backup_dir = importer.backup_existing_data()
                        if backup_dir:
                            print(f"数据已备份到: {backup_dir}")
                
                # 创建表
                print("\n正在创建/更新表结构...")
                print(f"注意：将自动处理{importer.db_type}数据库的特定优化")
                
                if importer.create_tables():
                    print("表结构创建/更新成功!")
                    
                    # 验证表结构
                    print("\n正在验证表结构...")
                    verification_results = importer.verify_table_structure()
                    if verification_results:
                        importer.print_verification_results(verification_results)
                    
                    print("\n✅ 跨数据库模型导入完成!")
                    print("您现在可以使用生成的ORM类来操作数据库了。")
                    
                    # 显示使用示例
                    print("\n" + "="*70)
                    print("使用示例:")
                    print("="*70)
                    print("from cross_db_models import Base, configure_all_models_for_database")
                    print("from sqlalchemy import create_engine")
                    print("from sqlalchemy.orm import sessionmaker")
                    print("")
                    print(f"# 连接到{importer.db_type}数据库")
                    print(f"engine = create_engine('{importer._build_database_url()}')")
                    print("configure_all_models_for_database(engine)")
                    print("")
                    print("# 现在可以使用ORM类进行数据库操作")
                    print("# 例如: Session = sessionmaker(bind=engine)")
                    print("#      session = Session()")
                    print("#      results = session.query(YourModel).all()")
                else:
                    print("表结构创建/更新失败")
        else:
            print("没有需要创建或更新的表")
    
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    
    finally:
        importer.close()


if __name__ == "__main__":
    main()

