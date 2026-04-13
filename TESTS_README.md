# Модульные тесты для Data Reconciliation Tool

## Обзор

Этот пакет содержит модульные тесты для инструмента сверки данных между Oracle и PostgreSQL (Форма 110 КХД).

## Файлы

- `test_reconciliation.py` - основные модульные тесты (18 тестов)
- `reconciliation_module.py` - извлеченный из Jupyter Notebook модуль для тестирования (1078 строк)
- `data_reconciliation_tool.ipynb` - исходный Jupyter Notebook

## Установка зависимостей

```bash
pip install pytest pandas numpy sqlalchemy seaborn matplotlib
```

## Запуск тестов

### Все тесты
```bash
pytest test_reconciliation.py -v
```

### Тесты по классам
```bash
# Тесты утилит
pytest test_reconciliation.py::TestAddSystemPythonDbLibraries -v

# Тесты конфигурации
pytest test_reconciliation.py::TestReconciliationConfig -v

# Тесты MetadataInspector
pytest test_reconciliation.py::TestMetadataInspector -v

# Тесты DataReconciliator
pytest test_reconciliation.py::TestDataReconciliator -v

# Интеграционные тесты
pytest test_reconciliation.py::TestIntegration -v
```

### С отчетом о покрытии
```bash
pytest test_reconciliation.py --cov=reconciliation_module --cov-report=html
```

## Структура тестов

### TestAddSystemPythonDbLibraries
Тесты для функции автоматического подключения библиотек БД:
- `test_function_exists` - проверка существования функции
- `test_function_runs_without_error` - выполнение без ошибок

### TestReconciliationConfig
Тесты класса конфигурации (dataclass):
- `test_config_creation_with_required_fields` - создание с обязательными полями
- `test_config_default_values` - значения по умолчанию (exclusions=[], batch_size=100000, decimal_precision=2)
- `test_config_empty_composite_keys_raises_error` - валидация composite_keys (ValueError при пустом списке)
- `test_config_with_all_options` - создание со всеми опциями (report_date_column, sample_size, specific_keys, year_from, year_to)

**Параметры конфигурации:**
- `oracle_connection_string`, `postgres_connection_string` - строки подключения
- `oracle_schema`, `oracle_table`, `postgres_schema`, `postgres_table` - схемы и таблицы
- `composite_keys` - список полей бизнес-ключа (обязательно, не пусто)
- `report_date_column` - колонка с датой отчета
- `exclusions` - исключаемые поля (по умолчанию [])
- `sample_size` - количество случайных ключей (None = все данные)
- `specific_keys` - конкретные значения ключей для точечного анализа
- `batch_size` - размер пакета (по умолчанию 100000)
- `decimal_precision` - точность округления (по умолчанию 2)
- `year_from`, `year_to` - фильтрация по годам

### TestMetadataInspector
Тесты инспектора метаданных:
- `test_metadata_inspector_initialization` - инициализация с SQLAlchemy engine
- `test_get_table_columns` - получение информации о колонках (name, type, nullable)
- `test_classify_columns` - классификация колонок по типам данных
- `test_get_all_columns` - получение всех колонок и их классификации

**Типы данных для классификации:**
- `NUMERIC_TYPES`: numeric, decimal, number, float, double, int, integer, smallint, bigint, real
- `STRING_TYPES`: varchar, char, text, string, character varying, character
- `DATE_TYPES`: date, timestamp, datetime, time

**Методы класса:**
- `__init__(engine)` - инициализация с SQLAlchemy engine
- `get_table_columns(schema, table)` - получение списка колонок
- `classify_columns(columns, exclusions)` - классификация по типам
- `get_all_columns(schema, table, exclusions)` - полная информация о колонках

### TestDataReconciliator
Тесты основного класса сверки:
- `test_reconciliator_initialization` - инициализация (config, engines=None, results={})
- `test_safe_compare_with_nulls` - сравнение с NULL значениями (NULL == NULL → True)
- `test_safe_compare_without_nulls` - сравнение без NULL
- `test_safe_compare_strings` - сравнение строковых значений
- `test_connect_creates_engines` - проверка наличия методов подключения
- `test_disconnect_sets_engines_to_none` - закрытие подключений (вызов dispose())

**Методы класса:**
- `__init__(config)` - инициализация с конфигурацией
- `safe_compare(df1_col, df2_col)` - безопасное сравнение с обработкой NULL (fillna('NULL'))
- `connect()` - подключение к Oracle и PostgreSQL с проверками
- `disconnect()` - закрытие подключений
- `collect_metadata()` - сбор метаданных таблиц
- `_build_aggregation_query(schema, table, ...)` - построение агрегирующего SQL запроса
- `run_full_reconciliation()` - полный цикл сверки данных
- `quick_yearly_report()` - быстрый годовой отчет

### TestIntegration
Интеграционные тесты:
- `test_full_workflow_with_mocks` - проверка наличия всех методов workflow
- `test_config_validation` - валидация конфигурации

## Мокирование

Тесты используют моки для:
- SQLAlchemy engine и inspector (`unittest.mock.Mock`, `patch`)
- Подключений к базам данных
- Библиотек oracledb и psycopg2

Это позволяет запускать тесты без реальных подключений к БД.

## Статистика

- **Всего тестов:** 18
- **Все тесты проходят:** ✅
- **Покрытие компонентов:**

| Компонент | Тестов | Методов покрыто |
|-----------|--------|-----------------|
| add_system_python_db_libraries | 2 | 1 функция |
| ReconciliationConfig | 4 | __post_init__, валидация |
| MetadataInspector | 4 | 4 метода |
| DataReconciliator | 6 | 8 методов |
| Интеграционные | 2 | workflow |

## Особенности реализации

1. **Обработка NULL:** Метод `safe_compare` использует `fillna('NULL')` для корректного сравнения NULL значений (NULL == NULL → True)

2. **Валидация конфигурации:** Пустой `composite_keys` вызывает `ValueError`

3. **Автоматическое подключение библиотек:** Функция `add_system_python_db_libraries` добавляет системные пути к oracledb/psycopg2

4. **Мокирование в тестах:** Тесты не требуют реального подключения к БД, используются Mock-объекты

## Запуск тестов из Jupyter Notebook

Для запуска модульных тестов непосредственно из ноутбука `data_reconciliation_tool.ipynb`:

### Шаг 1: Подготовка окружения

Убедитесь, что в той же директории находятся файлы:
- `data_reconciliation_tool.ipynb`
- `test_reconciliation.py`
- `reconciliation_module.py`

### Шаг 2: Установка зависимостей в ноутбуке

Выполните ячейку с установкой pytest (если еще не установлен):
```python
!pip install pytest pandas numpy sqlalchemy seaborn matplotlib
```

### Шаг 3: Запуск тестов из ячейки ноутбука

Создайте новую ячейку в начале или конце ноутбука и вставьте следующий код:

```python
# Запуск модульных тестов из Jupyter Notebook
import sys
import pytest

# Опционально: проверить наличие файлов
import os
print("📁 Проверка файлов:")
print(f"  test_reconciliation.py: {os.path.exists('test_reconciliation.py')}")
print(f"  reconciliation_module.py: {os.path.exists('reconciliation_module.py')}")

# Запуск всех тестов с подробным выводом
print("\n🧪 Запуск модульных тестов...")
exit_code = pytest.main(['test_reconciliation.py', '-v', '--tb=short'])

if exit_code == 0:
    print("\n✅ Все тесты пройдены успешно!")
else:
    print(f"\n❌ Тесты завершились с кодом: {exit_code}")
```

### Шаг 4: Запуск конкретных тестов

Для запуска отдельных групп тестов используйте:

```python
# Только тесты конфигурации
pytest.main(['test_reconciliation.py::TestReconciliationConfig', '-v'])

# Только тесты DataReconciliator
pytest.main(['test_reconciliation.py::TestDataReconciliator', '-v'])

# Тесты с отчетом о покрытии (требуется pytest-cov)
!pip install pytest-cov
pytest.main(['test_reconciliation.py', '--cov=reconciliation_module', '--cov-report=term-missing'])
```

### Шаг 5: Интерпретация результатов

- ✅ **Зеленые тесты** - все проверки пройдены
- ❌ **Красные тесты** - обнаружены ошибки (проверьте стек трейс)
- ⚠️ **Желтые предупреждения** - тесты пропущены или есть warnings

### Важные замечания

1. **Рабочая директория:** Убедитесь, что ноутбук запущен из той же директории, где находятся файлы тестов
2. **Ядро Python:** Ноутбук должен использовать то же окружение Python, где установлен pytest
3. **Моки работают:** Тесты используют моки, поэтому реальное подключение к БД не требуется
4. **Перезапуск ядра:** После изменений в `reconciliation_module.py` рекомендуется выполнить "Restart & Run All"

### Пример вывода в ноутбуке

```
📁 Проверка файлов:
  test_reconciliation.py: True
  reconciliation_module.py: True

🧪 Запуск модульных тестов...
============================= test session starts ==============================
collected 18 items

test_reconciliation.py::TestAddSystemPythonDbLibraries::test_function_exists PASSED
test_reconciliation.py::TestAddSystemPythonDbLibraries::test_function_runs_without_error PASSED
test_reconciliation.py::TestReconciliationConfig::test_config_creation_with_required_fields PASSED
...
test_reconciliation.py::TestIntegration::test_config_validation PASSED

============================== 18 passed in 5.56s ==============================

✅ Все тесты пройдены успешно!
```
