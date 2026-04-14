"""
Data Reconciliation Module
Extracted from data_reconciliation_tool.ipynb
Module for data reconciliation between Oracle and PostgreSQL databases.
"""

import sys
import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import QueuePool
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Database library placeholders (will be imported if available)
ORACLE_LIBRARY = None
POSTGRES_LIBRARY = None

try:
    import oracledb
    ORACLE_LIBRARY = 'oracledb'
except ImportError:
    pass

try:
    import psycopg2
    POSTGRES_LIBRARY = 'psycopg2'
except ImportError:
    pass

# Visualization libraries (optional)
try:
    import seaborn as sns
    import matplotlib.pyplot as plt
    plt.rcParams['figure.figsize'] = (14, 8)
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False
    sns = None
    plt = None


def add_system_python_db_libraries():
    """
    Добавляет пути к библиотекам oracledb и psycopg2 из системного Python,
    если они не найдены в текущем окружении.
    """
    system_python_paths = [
        '/usr/local/lib/python3.12/site-packages',
        '/usr/local/lib/python3.11/site-packages',
        '/usr/local/lib/python3.10/site-packages',
        '/usr/lib/python3/dist-packages',
    ]
    
    need_oracledb = False
    need_psycopg2 = False
    
    try:
        import oracledb
    except ImportError:
        need_oracledb = True
    
    try:
        import psycopg2
    except ImportError:
        need_psycopg2 = True
    
    if not (need_oracledb or need_psycopg2):
        return
    
    for sys_path in system_python_paths:
        if not os.path.exists(sys_path):
            continue
            
        oracledb_path = os.path.join(sys_path, 'oracledb')
        psycopg2_path = os.path.join(sys_path, 'psycopg2')
        psycopg2_binary_path = os.path.join(sys_path, 'psycopg2_binary')
        
        if need_oracledb and os.path.exists(oracledb_path):
            if sys_path not in sys.path:
                sys.path.insert(1, sys_path)
            need_oracledb = False
        
        if need_psycopg2 and (os.path.exists(psycopg2_path) or os.path.exists(psycopg2_binary_path)):
            if sys_path not in sys.path:
                sys.path.insert(1, sys_path)
            need_psycopg2 = False
        
        if not need_oracledb and not need_psycopg2:
            break



import sys
import os

# Автоматическое добавление путей к системным библиотекам для БД
def add_system_python_db_libraries():
    """
    Добавляет пути к библиотекам oracledb и psycopg2 из системного Python,
    если они не найдены в текущем окружении.
    """
    # Определяем возможные расположения системного Python
    system_python_paths = [
        '/usr/local/lib/python3.12/site-packages',
        '/usr/local/lib/python3.11/site-packages',
        '/usr/local/lib/python3.10/site-packages',
        '/usr/lib/python3/dist-packages',
    ]
    
    # Проверяем, нужны ли нам библиотеки
    need_oracledb = False
    need_psycopg2 = False
    
    try:
        import oracledb
    except ImportError:
        need_oracledb = True
    
    try:
        import psycopg2
    except ImportError:
        need_psycopg2 = True
    
    if not (need_oracledb or need_psycopg2):
        print("✓ Все библиотеки для БД уже доступны")
        return
    
    # Ищем и добавляем пути к системным библиотекам
    for sys_path in system_python_paths:
        if not os.path.exists(sys_path):
            continue
            
        # Проверяем наличие нужных библиотек
        oracledb_path = os.path.join(sys_path, 'oracledb')
        psycopg2_path = os.path.join(sys_path, 'psycopg2')
        psycopg2_binary_path = os.path.join(sys_path, 'psycopg2_binary')
        
        if need_oracledb and os.path.exists(oracledb_path):
            if sys_path not in sys.path:
                sys.path.insert(1, sys_path)  # Вставляем после пустой строки ''
                print(f"✓ Добавлен путь к oracledb: {sys_path}")
            need_oracledb = False
        
        if need_psycopg2 and (os.path.exists(psycopg2_path) or os.path.exists(psycopg2_binary_path)):
            if sys_path not in sys.path:
                sys.path.insert(1, sys_path)
                print(f"✓ Добавлен путь к psycopg2: {sys_path}")
            need_psycopg2 = False
        
        if not need_oracledb and not need_psycopg2:
            break
    
    # Финальная проверка
    if need_oracledb:
        print("⚠ Предупреждение: oracledb не найден в системном Python")
    if need_psycopg2:
        print("⚠ Предупреждение: psycopg2 не найден в системном Python")

# Выполняем автоматическое подключение
add_system_python_db_libraries()

# Теперь импортируем основные библиотеки (из текущего окружения)
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import QueuePool
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Для визуализации
import seaborn as sns
import matplotlib.pyplot as plt
plt.rcParams['figure.figsize'] = (14, 8)

# Проверка доступных библиотек для Oracle
try:
    import oracledb
    print(f"✓ oracledb версии {oracledb.__version__} доступен (рекомендуемая библиотека)")
    ORACLE_LIBRARY = 'oracledb'
except ImportError:
    print("✗ Библиотека oracledb не найдена. Установите: pip install oracledb")
    ORACLE_LIBRARY = None

# Проверка доступных библиотек для PostgreSQL
try:
    import psycopg2
    print(f"✓ psycopg2 версии {psycopg2.__version__} доступен")
    POSTGRES_LIBRARY = 'psycopg2'
except ImportError:
    print("✗ Библиотека psycopg2 не найдена. Установите: pip install psycopg2-binary")
    POSTGRES_LIBRARY = None

print("\nБиблиотеки успешно импортированы")

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple

@dataclass
class ReconciliationConfig:
    """
    Конфигурация для инструмента сверки данных.
    
    Attributes:
        oracle_connection_string: Строка подключения к Oracle
        postgres_connection_string: Строка подключения к Postgres
        oracle_schema: Схема Oracle (например, 'ORA_SCHEMA')
        oracle_table: Имя таблицы в Oracle (например, 'TABLE_V1')
        postgres_schema: Схема Postgres (например, 'PG_SCHEMA')
        postgres_table: Имя таблицы в Postgres (например, 'TABLE_V2')
        composite_keys: Список полей бизнес-ключа для группировки
        report_date_column: Название колонки с датой отчета для анализа по годам
        exclusions: Поля, исключаемые из сверки (технические поля)
        sample_size: Количество случайных ключей для проверки (None = все данные)
        specific_keys: Конкретные значения ключей для точечного анализа
        batch_size: Размер пакета для обработки (оптимизация памяти)
        decimal_precision: Точность округления числовых полей
        year_from: Начальный год для фильтрации (None = без ограничений)
        year_to: Конечный год для фильтрации (None = без ограничений)
    """
    oracle_connection_string: str
    postgres_connection_string: str
    oracle_schema: str
    oracle_table: str
    postgres_schema: str
    postgres_table: str
    composite_keys: List[str]
    report_date_column: Optional[str] = None
    exclusions: Optional[List[str]] = None
    sample_size: Optional[int] = None
    specific_keys: Optional[Dict[str, Any]] = None
    batch_size: int = 100000
    decimal_precision: int = 2
    year_from: Optional[int] = None
    year_to: Optional[int] = None

    def __post_init__(self):
        if self.exclusions is None:
            self.exclusions = []
        if not self.composite_keys:
            raise ValueError("composite_keys не может быть пустым")

# Пример конфигурации (замените на свои данные)
# config = ReconciliationConfig(
#     oracle_connection_string="oracle+oracledb://user:pass@host:1521/service",
#     postgres_connection_string="postgresql://user:pass@host:5432/dbname",
#     oracle_schema="ORA_SCHEMA",
#     oracle_table="TABLE_V1",
#     postgres_schema="PG_SCHEMA",
#     postgres_table="TABLE_V2",
#     composite_keys=['BANK_CODE', 'REPORT_DATE'],
#     report_date_column='REPORT_DATE',  # Новый параметр для быстрой проверки по годам
#     exclusions=['ID', 'LOAD_DATE', 'CREATED_AT'],
#     sample_size=1000,
#     specific_keys=None,
#     batch_size=100000,
#     decimal_precision=2
# )

print("Класс конфигурации создан")

class MetadataInspector:
    """
    Инспектор метаданных для автоматического определения типов полей.
    """
    
    NUMERIC_TYPES = ['numeric', 'decimal', 'number', 'float', 'double', 'int', 'integer', 'smallint', 'bigint', 'real']
    STRING_TYPES = ['varchar', 'char', 'text', 'string', 'character varying', 'character']
    DATE_TYPES = ['date', 'timestamp', 'datetime', 'time']
    
    def __init__(self, engine):
        self.engine = engine
        self.inspector = inspect(engine)
    
    def get_table_columns(self, schema: str, table: str) -> List[Dict]:
        """Получить информацию о колонках таблицы."""
        columns = []
        for col in self.inspector.get_columns(table, schema=schema):
            col_info = {
                'name': col['name'],
                'type': str(col['type']).lower(),
                'nullable': col.get('nullable', True)
            }
            columns.append(col_info)
        return columns
    
    def classify_columns(self, columns: List[Dict], exclusions: List[str]) -> Dict[str, List[str]]:
        """
        Классифицировать колонки по типам.
        
        Returns:
            Dict с категориями: numeric_fields, string_fields, date_fields, excluded_fields
        """
        classification = {
            'numeric_fields': [],
            'string_fields': [],
            'date_fields': [],
            'excluded_fields': []
        }
        
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            
            # Проверка исключений
            if col_name.upper() in [e.upper() for e in exclusions]:
                classification['excluded_fields'].append(col_name)
                continue
            
            # Классификация по типам
            if any(nt in col_type for nt in self.NUMERIC_TYPES):
                classification['numeric_fields'].append(col_name)
            elif any(st in col_type for st in self.STRING_TYPES):
                classification['string_fields'].append(col_name)
            elif any(dt in col_type for dt in self.DATE_TYPES):
                classification['date_fields'].append(col_name)
            else:
                # По умолчанию считаем строковым
                classification['string_fields'].append(col_name)
        
        return classification
    
    def get_all_columns(self, schema: str, table: str, exclusions: List[str]) -> Tuple[List[str], Dict]:
        """
        Получить все колонки и их классификацию.
        
        Returns:
            Tuple (список всех колонок, классификация)
        """
        columns = self.get_table_columns(schema, table)
        all_col_names = [col['name'] for col in columns]
        classification = self.classify_columns(columns, exclusions)
        
        return all_col_names, classification

print("Класс MetadataInspector создан")

class DataReconciliator:
    """
    Основной класс для выполнения сверки данных между Oracle и Postgres.
    """
    
    def __init__(self, config: ReconciliationConfig):
        self.config = config
        self.oracle_engine = None
        self.postgres_engine = None
        self.oracle_metadata = None
        self.postgres_metadata = None
        self.results = {}
        
    @staticmethod
    def safe_compare(df1_col, df2_col):
        """
        Безопасное сравнение ключевых полей с обработкой NULL значений.
        
        Args:
            df1_col: Колонка из первого DataFrame
            df2_col: Колонка из второго DataFrame
            
        Returns:
            Boolean Series: True где значения совпадают (включая NULL == NULL)
        """
        return (df1_col.fillna('NULL') == df2_col.fillna('NULL'))
    
    def connect(self):
        """Установить подключения к базам данных."""
        print("Подключение к Oracle...")
        # Проверка доступности библиотеки для Oracle
        if ORACLE_LIBRARY is None:
            raise ImportError(
                "Библиотека для Oracle не найдена!\n"
                "Установите oracledb: pip install oracledb\n"
                "Или убедитесь, что Jupyter запущен из окружения с установленными библиотеками."
            )
        # Дополнительная проверка psycopg2 для Postgres
        if POSTGRES_LIBRARY is None:
            raise ImportError(
                "Библиотека для PostgreSQL не найдена!\n"
                "Установите psycopg2-binary: pip install psycopg2-binary\n"
                "Или убедитесь, что Jupyter запущен из окружения с установленными библиотеками."
            )
        self.oracle_engine = create_engine(
            self.config.oracle_connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        
        print("Подключение к Postgres...")
        self.postgres_engine = create_engine(
            self.config.postgres_connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        
        print("Подключения установлены успешно")
        # ========================================================================
        # ДОПОЛНИТЕЛЬНЫЕ ПРОВЕРКИ ПОСЛЕ ПОДКЛЮЧЕНИЯ
        # ========================================================================
        print("\n=== ВЫПОЛНЕНИЕ ПРОВЕРОК ПОДКЛЮЧЕНИЙ ===")
        # Проверка 1: Убедимся, что engine не None
        if self.oracle_engine is None:
            raise RuntimeError("Критическая ошибка: oracle_engine остался None после create_engine!")
        if self.postgres_engine is None:
            raise RuntimeError("Критическая ошибка: postgres_engine остался None после create_engine!")
        print("✓ Engine объекты созданы успешно")
        # Проверка 2: Тестовое подключение к Oracle
        try:
            print("Тестирование подключения к Oracle...")
            with self.oracle_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 FROM DUAL")).scalar()
                if result == 1:
                    print("✓ Подключение к Oracle работает корректно")
                else:
                    raise RuntimeError("Неожиданный результат от Oracle")
        except Exception as e:
            error_msg = (
                f"❌ ОШИБКА ПРИ ПРОВЕРКЕ ORACLE: {str(e)}\n"
                "Возможные причины:\n"
                "  1. Неверное имя пользователя или пароль\n"
                "  2. Недоступен сервер Oracle (сеть, порт, сервис)\n"
                "  3. Проблемы с библиотекой oracledb\n"
                "  4. Истекло время ожидания подключения\n"
                "\n"
                "РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:\n"
                "  1. Проверьте строку подключения в конфигурации\n"
                "  2. Убедитесь, что сервер Oracle доступен из этой машины\n"
                "  3. Проверьте учетные данные (логин/пароль)\n"
                "  4. Убедитесь, что oracledb установлен: pip install oracledb\n"
            )
            print(error_msg)
            raise RuntimeError(f"Проверка подключения к Oracle не пройдена: {e}")
        # Проверка 3: Тестовое подключение к Postgres
        try:
            print("Тестирование подключения к Postgres...")
            with self.postgres_engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                if result == 1:
                    print("✓ Подключение к Postgres работает корректно")
                else:
                    raise RuntimeError("Неожиданный результат от Postgres")
        except Exception as e:
            error_msg = (
                f"❌ ОШИБКА ПРИ ПРОВЕРКЕ POSTGRES: {str(e)}\n"
                "Возможные причины:\n"
                "  1. Неверное имя пользователя или пароль\n"
                "  2. Недоступен сервер Postgres (сеть, порт, БД)\n"
                "  3. Проблемы с библиотекой psycopg2\n"
                "  4. Истекло время ожидания подключения\n"
                "\n"
                "РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:\n"
                "  1. Проверьте строку подключения в конфигурации\n"
                "  2. Убедитесь, что сервер Postgres доступен из этой машины\n"
                "  3. Проверьте учетные данные (логин/пароль)\n"
                "  4. Убедитесь, что psycopg2 установлен: pip install psycopg2-binary\n"
            )
            print(error_msg)
            raise RuntimeError(f"Проверка подключения к Postgres не пройдена: {e}")
        print("\n✓ Все проверки подключений пройдены успешно!\n")
    
    def disconnect(self):
        """Закрыть подключения."""
        if self.oracle_engine:
            self.oracle_engine.dispose()
        if self.postgres_engine:
            self.postgres_engine.dispose()
        print("Подключения закрыты")
    
    def collect_metadata(self):
        """Собрать метаданные таблиц."""
        print("Сбор метаданных Oracle...")
        oracle_inspector = MetadataInspector(self.oracle_engine)
        oracle_cols, oracle_class = oracle_inspector.get_all_columns(
            self.config.oracle_schema,
            self.config.oracle_table,
            self.config.exclusions
        )
        
        print("Сбор метаданных Postgres...")
        postgres_inspector = MetadataInspector(self.postgres_engine)
        postgres_cols, postgres_class = postgres_inspector.get_all_columns(
            self.config.postgres_schema,
            self.config.postgres_table,
            self.config.exclusions
        )
        
        self.oracle_metadata = {
            'all_columns': oracle_cols,
            'classification': oracle_class
        }
        
        self.postgres_metadata = {
            'all_columns': postgres_cols,
            'classification': postgres_class
        }
        
        print(f"Oracle: {len(oracle_cols)} колонок, числовых: {len(oracle_class['numeric_fields'])}")
        print(f"Postgres: {len(postgres_cols)} колонок, числовых: {len(postgres_class['numeric_fields'])}")
    
    def _build_aggregation_query(self, schema: str, table: str,
                                  classification: Dict, db_type: str) -> str:
        """
        Построить SQL запрос для агрегации.
        ИСПРАВЛЕНИЕ: Унифицировано преобразование ключей (UPPER+TRIM) для корректного сравнения.
        ИСПРАВЛЕНИЕ: Добавлена поддержка specific_keys (WHERE-условие) и sample_size (LIMIT/ROWNUM).
        """
        keys = self.config.composite_keys
        numeric_fields = classification['numeric_fields']
        precision = self.config.decimal_precision
        date_fields = classification.get('date_fields', [])
        
        # Формирование списка агрегаций для числовых полей
        sum_expressions = []
        for field in numeric_fields:
            if db_type == 'oracle':
                sum_expr = f"ROUND(NVL(SUM({field}), 0), {precision}) AS SUM_{field}"
            else:  # postgres
                sum_expr = f"ROUND(COALESCE(SUM({field}), 0), {precision}) AS SUM_{field}"
            sum_expressions.append(sum_expr)
        
        # COUNT
        count_expr = "COUNT(*) AS row_cnt"
        
        # Группировка по ключам с унифицированной обработкой NULL и типов данных
        group_by_keys = []
        select_keys = []
        for key in keys:
            # Проверка, является ли ключ полем даты
            if key in date_fields:
                # Для дат используем явный формат YYYY-MM-DD
                if db_type == 'oracle':
                    group_key = f"NVL(TO_CHAR({key}, 'YYYY-MM-DD'), '__NULL__')"
                    select_key = f"NVL(TO_CHAR({key}, 'YYYY-MM-DD'), '__NULL__') AS {key}"
                else:
                    group_key = f"COALESCE(TO_CHAR({key}::DATE, 'YYYY-MM-DD'), '__NULL__')"
                    select_key = f"COALESCE(TO_CHAR({key}::DATE, 'YYYY-MM-DD'), '__NULL__') AS {key}"
            else:
                # Унифицированное преобразование: UPPER + TRIM для строк
                if db_type == 'oracle':
                    group_key = f"NVL(UPPER(TRIM(TO_CHAR({key}))), '__NULL__')"
                    select_key = f"NVL(UPPER(TRIM(TO_CHAR({key}))), '__NULL__') AS {key}"
                else:
                    group_key = f"COALESCE(UPPER(TRIM({key}::TEXT)), '__NULL__')"
                    select_key = f"COALESCE(UPPER(TRIM({key}::TEXT)), '__NULL__') AS {key}"
            group_by_keys.append(group_key)
            select_keys.append(select_key)
        
        # Полный запрос
        select_clause = ", ".join(select_keys)
        aggregations = ", ".join([count_expr] + sum_expressions)
        group_by_clause = ", ".join(group_by_keys)
        
        # Построение WHERE-условия для specific_keys
        where_clauses = []
        if self.config.specific_keys:
            for key, value in self.config.specific_keys.items():
                # Нормализация имени ключа для сравнения
                key_upper = key.upper()
                
                # Определение типа ключа (дата или нет)
                is_date_key = key_upper in [k.upper() for k in date_fields]
                
                # Обработка значения (может быть строкой, списком или кортежем)
                if isinstance(value, (list, tuple)):
                    # Список значений - используем IN
                    if is_date_key:
                        # Для дат преобразуем значения в формат YYYY-MM-DD
                        formatted_values = []
                        for v in value:
                            if db_type == 'oracle':
                                formatted_values.append(f"TO_DATE('{v}', 'YYYY-MM-DD')")
                            else:
                                formatted_values.append(f"'{v}'::DATE")
                        values_str = ", ".join(formatted_values)
                        where_clauses.append(f"{key} IN ({values_str})")
                    else:
                        # Для строк/чисел
                        formatted_values = []
                        for v in value:
                            if isinstance(v, str):
                                formatted_values.append(f"'{v}'")
                            else:
                                formatted_values.append(str(v))
                        values_str = ", ".join(formatted_values)
                        where_clauses.append(f"{key} IN ({values_str})")
                elif isinstance(value, tuple) and len(value) == 2:
                    # Кортеж из двух элементов - диапазон (для дат)
                    if is_date_key:
                        start_val, end_val = value
                        if db_type == 'oracle':
                            where_clauses.append(f"{key} BETWEEN TO_DATE('{start_val}', 'YYYY-MM-DD') AND TO_DATE('{end_val}', 'YYYY-MM-DD')")
                        else:
                            where_clauses.append(f"{key} BETWEEN '{start_val}'::DATE AND '{end_val}'::DATE")
                else:
                    # Одиночное значение
                    if is_date_key:
                        if db_type == 'oracle':
                            where_clauses.append(f"{key} = TO_DATE('{value}', 'YYYY-MM-DD')")
                        else:
                            where_clauses.append(f"{key} = '{value}'::DATE")
                    elif isinstance(value, str):
                        where_clauses.append(f"{key} = '{value}'")
                    else:
                        where_clauses.append(f"{key} = {value}")
        
        # Добавление фильтрации по годам (year_from/year_to)
        if self.config.report_date_column and (self.config.year_from is not None or self.config.year_to is not None):
            if db_type == 'oracle':
                if self.config.year_from is not None and self.config.year_to is not None:
                    where_clauses.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}) BETWEEN {self.config.year_from} AND {self.config.year_to}")
                elif self.config.year_from is not None:
                    where_clauses.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}) >= {self.config.year_from}")
                elif self.config.year_to is not None:
                    where_clauses.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}) <= {self.config.year_to}")
            else:  # postgres
                if self.config.year_from is not None and self.config.year_to is not None:
                    where_clauses.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}::DATE) BETWEEN {self.config.year_from} AND {self.config.year_to}")
                elif self.config.year_from is not None:
                    where_clauses.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}::DATE) >= {self.config.year_from}")
                elif self.config.year_to is not None:
                    where_clauses.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}::DATE) <= {self.config.year_to}")

        # Формирование базовой части запроса
        base_query = f"""
        SELECT {select_clause}, {aggregations}
        FROM {schema}.{table}
        """
        
        # Добавление WHERE-условия
        if where_clauses:
            where_clause = " AND ".join(where_clauses)
            base_query += f"WHERE {where_clause}\n"
        
        base_query += f"GROUP BY {group_by_clause}"
        
        # Добавление ограничения sample_size
        if self.config.sample_size is not None and self.config.sample_size > 0:
            if db_type == 'oracle':
                # Oracle: используем подзапрос с ROWNUM
                query = f"""
                SELECT * FROM (
                    {base_query}
                ) WHERE ROWNUM <= {self.config.sample_size}
                """
            else:
                # PostgreSQL: используем LIMIT
                query = f"""
                {base_query}
                LIMIT {self.config.sample_size}
                """
        else:
            query = base_query
        
        return query.strip()
    def run_full_reconciliation(self):
        """
        Выполнить полную сверку данных между Oracle и Postgres.
        
        Returns:
            dict: Результаты сверки с этапами и расхождениями
        """
        print("=" * 60)
        print("ЗАПУСК ПОЛНОЙ СВЕРКИ ДАННЫХ")
        print("=" * 60)
        
        try:
            # Шаг 1: Сбор метаданных
            print("\n[Шаг 1/4] Сбор метаданных таблиц...")
            self.collect_metadata()
            
            # Шаг 2: Построение и выполнение запросов агрегации
            print("\n[Шаг 2/4] Выполнение агрегированной сверки (Этап 1)...")
            
            # Построить запросы
            oracle_query = self._build_aggregation_query(
                self.config.oracle_schema,
                self.config.oracle_table,
                self.oracle_metadata['classification'],
                'oracle'
            )
            
            postgres_query = self._build_aggregation_query(
                self.config.postgres_schema,
                self.config.postgres_table,
                self.postgres_metadata['classification'],
                'postgres'
            )
            
            print(f"Oracle query:\n{oracle_query}\n")
            print(f"Postgres query:\n{postgres_query}\n")
            
            # Выполнить запросы
            print("Выполнение запроса к Oracle...")
            with self.oracle_engine.connect() as conn:
                oracle_df = pd.read_sql_query(text(oracle_query), conn)
            print(f"Получено строк из Oracle: {len(oracle_df)}")
            
            print("Выполнение запроса к Postgres...")
            with self.postgres_engine.connect() as conn:
                postgres_df = pd.read_sql_query(text(postgres_query), conn)
            print(f"Получено строк из Postgres: {len(postgres_df)}")
            
            # ========================================================================
            # ЛОГИРОВАНИЕ ДАННЫХ ДЛЯ ВИЗУАЛЬНОЙ ПРОВЕРКИ
            # ========================================================================
            print("\n" + "=" * 80)
            print("📊 ЛОГИРОВАНИЕ НАБОРОВ ДАННЫХ ДЛЯ ВИЗУАЛЬНОЙ ПРОВЕРКИ")
            print("=" * 80)
            
            # --- Oracle DataFrame ---
            print("\n" + "-" * 80)
            print("🔶 НАБОР ДАННЫХ ORACLE (df_oracle)")
            print("-" * 80)
            print(f"Количество строк: {len(oracle_df)}")
            print(f"Количество колонок: {len(oracle_df.columns)}")
            print(f"\nИмена колонок: {list(oracle_df.columns)}")
            print(f"\nТипы данных:\n{oracle_df.dtypes}")
            print(f"\nПервые 10 строк:")
            display(oracle_df.head(10))
            print(f"\nПоследние 5 строк:")
            display(oracle_df.tail(5))
            print(f"\nОбщая статистика:")
            display(oracle_df.describe(include='all'))
            
            # --- Postgres DataFrame ---
            print("\n" + "-" * 80)
            print("🔵 НАБОР ДАННЫХ POSTGRES (df_postgres)")
            print("-" * 80)
            print(f"Количество строк: {len(postgres_df)}")
            print(f"Количество колонок: {len(postgres_df.columns)}")
            print(f"\nИмена колонок: {list(postgres_df.columns)}")
            print(f"\nТипы данных:\n{postgres_df.dtypes}")
            print(f"\nПервые 10 строк:")
            display(postgres_df.head(10))
            print(f"\nПоследние 5 строк:")
            display(postgres_df.tail(5))
            print(f"\nОбщая статистика:")
            display(postgres_df.describe(include='all'))
            
            # --- Сравнительная информация ---
            print("\n" + "-" * 80)
            print("📈 СРАВНИТЕЛЬНАЯ ИНФОРМАЦИЯ")
            print("-" * 80)
            print(f"Разница в количестве строк: {len(oracle_df) - len(postgres_df)}")
            print(f"Колонки только в Oracle: {set(oracle_df.columns) - set(postgres_df.columns)}")
            print(f"Колонки только в Postgres: {set(postgres_df.columns) - set(oracle_df.columns)}")
            print(f"Общие колонки: {set(oracle_df.columns) & set(postgres_df.columns)}")
            print("=" * 80)
            # ========================================================================
            
            
            # Шаг 3: Сравнение агрегированных данных
            print("\n[Шаг 3/4] Сравнение результатов...")
            keys = self.config.composite_keys
            
            # Merge данных
            merged = pd.merge(
                oracle_df,
                postgres_df,
                on=keys,
                how='outer',
                suffixes=('_ORA', '_PG'),
                indicator=True
            )
            
            # Расчет дельт для числовых полей
            numeric_fields = self.oracle_metadata['classification']['numeric_fields']
            for field in numeric_fields:
                ora_col = f'SUM_{field}_ORA'
                pg_col = f'SUM_{field}_PG'
                if ora_col in merged.columns and pg_col in merged.columns:
                    merged[f'DELTA_{field}'] = merged[ora_col].fillna(0) - merged[pg_col].fillna(0)
            
            # Дельта для COUNT
            if 'row_cnt_ORA' in merged.columns and 'row_cnt_PG' in merged.columns:
                merged['DELTA_ROW_COUNT'] = merged['row_cnt_ORA'].fillna(0) - merged['row_cnt_PG'].fillna(0)
            
            # Определение расхождений
            delta_cols = [c for c in merged.columns if c.startswith('DELTA_')]
            if delta_cols:
                merged['HAS_DISCREPANCY'] = merged[delta_cols].abs().sum(axis=1) > 0.01
            else:
                merged['HAS_DISCREPANCY'] = False
            
            # Статусы
            def assign_status(row):
                if row['_merge'] == 'left_only':
                    return 'ONLY_IN_ORACLE'
                elif row['_merge'] == 'right_only':
                    return 'ONLY_IN_POSTGRES'
                elif row['HAS_DISCREPANCY']:
                    return 'MISMATCH'
                else:
                    return 'OK'
            
            merged['STATUS'] = merged.apply(assign_status, axis=1)
            
            # Фильтрация расхождений
            discrepancies = merged[merged['HAS_DISCREPANCY'] | (merged['_merge'] != 'both')].copy()
            
            print(f"\nВсего записей: {len(merged)}")
            print(f"Расхождения найдены: {len(discrepancies)}")
            print(f"\nСтатусы:")
            print(merged['STATUS'].value_counts())
            
            
            # ========================================================================
            # ЭТАП 2: Детальная сверка для расхождений
            # ========================================================================
            print("\n[Этап 2] Детальная сверка расхождений...")
            
            stage2_details = pd.DataFrame()  # По умолчанию пустой
            
            if len(discrepancies) > 0:
                # Берем ключи расхождений для детальной проверки
                discrepancy_keys = discrepancies[keys].drop_duplicates()
                
                print(f"Найдено {len(discrepancy_keys)} уникальных ключей с расхождениями")
                
                # Построение запроса для детальных данных
                detail_columns = self.oracle_metadata['all_columns'].copy()  # Все колонки из Oracle
                
                # Формирование условия WHERE для ключей
                key_conditions = []
                for _, row in discrepancy_keys.iterrows():
                    cond_parts = []
                    for key in keys:
                        value = row[key]
                        if pd.isna(value) or value == '__NULL__':
                            cond_parts.append(f"{key} IS NULL")
                        elif isinstance(value, str):
                            escaped_value = value.replace("'", "''")  # Escape single quotes
                            cond_parts.append(f"{key} = '{escaped_value}'")
                        else:
                            cond_parts.append(f"{key} = {value}")
                    key_conditions.append(" AND ".join(cond_parts))
                
                if key_conditions:
                    where_clause = " OR ".join(key_conditions)
                    
                    # Добавление фильтра по году для детальных запросов (если задан в конфиге)
                    # Это предотвращает ошибку ORA-01861 при неявном преобразовании дат
                    # и обеспечивает согласованность с основным этапом сверки
                    year_filter_oracle = ""
                    year_filter_postgres = ""
                    
                    if self.config.report_date_column and (self.config.year_from is not None or self.config.year_to is not None):
                        year_conditions_oracle = []
                        year_conditions_postgres = []
                        
                        if self.config.year_from is not None:
                            year_conditions_oracle.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}) >= {self.config.year_from}")
                            year_conditions_postgres.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}::DATE) >= {self.config.year_from}")
                        
                        if self.config.year_to is not None:
                            year_conditions_oracle.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}) <= {self.config.year_to}")
                            year_conditions_postgres.append(f"EXTRACT(YEAR FROM {self.config.report_date_column}::DATE) <= {self.config.year_to}")
                        
                        if year_conditions_oracle:
                            year_filter_oracle = " AND " + " AND ".join(year_conditions_oracle)
                        if year_conditions_postgres:
                            year_filter_postgres = " AND " + " AND ".join(year_conditions_postgres)
                    
                    # Запрос детальных данных из Oracle
                    oracle_detail_query = f"""
                        SELECT {", ".join(detail_columns)}
                        FROM {self.config.oracle_schema}.{self.config.oracle_table}
                        WHERE ({where_clause}){year_filter_oracle}
                    """
                    
                    # Запрос детальных данных из Postgres
                    postgres_detail_query = f"""
                        SELECT {", ".join(detail_columns)}
                        FROM {self.config.postgres_schema}.{self.config.postgres_table}
                        WHERE ({where_clause}){year_filter_postgres}
                    """
                    
                    print("Загрузка детальных данных из Oracle...")
                    with self.oracle_engine.connect() as conn:
                        oracle_detail_df = pd.read_sql_query(text(oracle_detail_query), conn)
                    
                    print("Загрузка детальных данных из Postgres...")
                    with self.postgres_engine.connect() as conn:
                        postgres_detail_df = pd.read_sql_query(text(postgres_detail_query), conn)
                    
                    # Сравнение деталей
                    print("Сравнение детальных записей...")
                    
                    # Merge для поиска различий
                    oracle_detail_df['_source'] = 'ORACLE'
                    postgres_detail_df['_source'] = 'POSTGRES'
                    
                    # Объединяем для сравнения
                    stage2_details = pd.concat([oracle_detail_df, postgres_detail_df], ignore_index=True)
                    stage2_details['HAS_ISSUE'] = stage2_details.apply(
                        lambda row: 'DISCREPANCY_FOUND' if any(
                            row[key] in discrepancies[keys].values.flatten() 
                            for key in keys if key in row
                        ) else 'OK', 
                        axis=1
                    )
                    
                    print(f"Детально проверено записей: {len(stage2_details)}")
                else:
                    print("Нет ключей для детальной проверки")
            else:
                print("Расхождений не найдено, этап 2 пропускается")
            
            # ========================================================================
            # Шаг 4: Формирование результатов
            # ========================================================================
            print("\n[Шаг 4/4] Формирование результатов...")
            
            self.results = {
                'stage1_merged': merged,
                'stage1_discrepancies': discrepancies,
                'stage2_details': stage2_details,
                'summary': {
                    'total_records': len(merged),
                    'discrepancies_count': len(discrepancies),
                    'stage2_records_count': len(stage2_details),
                    'status_counts': merged['STATUS'].value_counts().to_dict()
                }
            }
            print("\n[Шаг 4/4] Формирование результатов...")
            
            self.results = {
                'stage1_merged': merged,
                'stage1_discrepancies': discrepancies,
                'summary': {
                    'total_records': len(merged),
                    'discrepancies_count': len(discrepancies),
                    'status_counts': merged['STATUS'].value_counts().to_dict()
                }
            }
            
            print("\n" + "=" * 60)
            print("СВЕРКА ЗАВЕРШЕНА УСПЕШНО")
            print("=" * 60)
            
            return self.results
            
        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ВЫПОЛНЕНИИ СВЕРКИ: {str(e)}")
            raise
    
    def quick_yearly_report(self):
        """
        Быстрая проверка распределения данных по годам.
        
        Returns:
            dict: Результаты проверки по годам
        """
        print("=" * 60)
        print("БЫСТРАЯ ПРОВЕРКА РАСПРЕДЕЛЕНИЯ ПО ГОДАМ")
        print("=" * 60)
        
        if not hasattr(self.config, 'report_date_column') or not self.config.report_date_column:
            raise ValueError("Для быстрой проверки необходимо указать report_date_column в конфигурации")
        
        date_col = self.config.report_date_column
        
        # Построение WHERE-условия для фильтрации по годам
        year_where_clause = ""
        if self.config.year_from is not None or self.config.year_to is not None:
            year_conditions = []
            if self.config.year_from is not None:
                year_conditions.append(f"EXTRACT(YEAR FROM {date_col}) >= {self.config.year_from}")
            if self.config.year_to is not None:
                year_conditions.append(f"EXTRACT(YEAR FROM {date_col}) <= {self.config.year_to}")
            if year_conditions:
                year_where_clause = " WHERE " + " AND ".join(year_conditions)
        
        
        try:
            # Запросы для получения распределения по годам
            oracle_query = f"""
            SELECT EXTRACT(YEAR FROM {date_col}) AS year, COUNT(*) AS row_count
            FROM {self.config.oracle_schema}.{self.config.oracle_table}" + year_where_clause + "
            GROUP BY EXTRACT(YEAR FROM {date_col})
            ORDER BY year
            """
            
            postgres_query = f"""
            SELECT EXTRACT(YEAR FROM {date_col}::DATE) AS year, COUNT(*) AS row_count
            FROM {self.config.postgres_schema}.{self.config.postgres_table}" + year_where_clause + "
            GROUP BY EXTRACT(YEAR FROM {date_col}::DATE)
            ORDER BY year
            """
            
            print(f"\nOracle query:\n{oracle_query}\n")
            print(f"Postgres query:\n{postgres_query}\n")
            
            # Выполнить запросы
            print("Выполнение запроса к Oracle...")
            with self.oracle_engine.connect() as conn:
                oracle_yearly = pd.read_sql_query(text(oracle_query), conn)
            
            print("Выполнение запроса к Postgres...")
            with self.postgres_engine.connect() as conn:
                postgres_yearly = pd.read_sql_query(text(postgres_query), conn)
            
            # Объединение результатов
            yearly_comparison = pd.merge(
                oracle_yearly,
                postgres_yearly,
                on='year',
                how='outer',
                suffixes=('_ORA', '_PG')
            )
            
            yearly_comparison['DIFFERENCE'] = yearly_comparison['row_count_ORA'].fillna(0) - yearly_comparison['row_count_PG'].fillna(0)
            yearly_comparison['MATCH'] = yearly_comparison['DIFFERENCE'] == 0
            
            print("\n=== СРАВНЕНИЕ ПО ГОДАМ ===")
            display(yearly_comparison)
            
            # Визуализация
            try:
                import seaborn as sns
                import matplotlib.pyplot as plt
                
                # Преобразование для графика
                plot_data = pd.melt(
                    yearly_comparison[['year', 'row_count_ORA', 'row_count_PG']],
                    id_vars=['year'],
                    value_vars=['row_count_ORA', 'row_count_PG'],
                    var_name='Source',
                    value_name='Count'
                )
                plot_data['Source'] = plot_data['Source'].map({
                    'row_count_ORA': 'Oracle',
                    'row_count_PG': 'Postgres'
                })
                
                plt.figure(figsize=(12, 6))
                sns.barplot(data=plot_data, x='year', y='Count', hue='Source')
                plt.title('Сравнение количества строк по годам')
                plt.xlabel('Год')
                plt.ylabel('Количество строк')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.show()
            except ImportError:
                print("Seaborn/matplotlib не доступны, пропускаем визуализацию")
            
            # Итоговая статистика
            matching_years = yearly_comparison['MATCH'].sum()
            total_years = len(yearly_comparison)
            
            print(f"\n=== ИТОГИ ===")
            print(f"Всего лет проверено: {total_years}")
            print(f"Совпадений: {matching_years}")
            print(f"Расхождений: {total_years - matching_years}")
            
            return {
                'yearly_comparison': yearly_comparison,
                'matching_years': matching_years,
                'total_years': total_years
            }
            
        except Exception as e:
            print(f"\n❌ ОШИБКА ПРИ ВЫПОЛНЕНИИ БЫСТРОЙ ПРОВЕРКИ: {str(e)}")
            raise


# ============================================================================
# ПРИМЕР ВЫЗОВА (раскомментируйте и заполните своими данными)
# ============================================================================

# ШАГ 1: Создание конфигурации - ЗАПОЛНИТЕ СВОИМИ ДАННЫМИ
config = ReconciliationConfig(
    oracle_connection_string="oracle+oracledb://USERNAME:PASSWORD@HOST:1521/SERVICE_NAME",
    postgres_connection_string="postgresql://USERNAME:PASSWORD@HOST:5432/DATABASE_NAME",
    oracle_schema="ORA_SCHEMA",
    oracle_table="FORM_110_V1",
    postgres_schema="PG_SCHEMA",
    postgres_table="FORM_110_V2",
    composite_keys=['BANK_CODE', 'REPORT_DATE'],
    exclusions=['ID', 'LOAD_TIMESTAMP', 'ETL_JOB_ID', 'CREATED_AT', 'UPDATED_AT'],
    sample_size=5000,  # Проверить 5000 случайных комбинаций ключей (или None для всех)
    specific_keys={'BANKCODE': '1003', 'REPORTDATE': '2010-12-01'},  # Или {'BANK_CODE': ['001', '002', '003']} для точечной проверки
    batch_size=100000,
    decimal_precision=2,
    year_from=None,  # Фильтр по начальному году (например, 2020)
    year_to=None,    # Фильтр по конечному году (например, 2024)
)

# ШАГ 2: Создание экземпляра реконсилятора
reconciliator = DataReconciliator(config)

# ШАГ 3: Подключение к базам данных (ОБЯЗАТЕЛЬНО перед использованием!)
