# Модульные тесты для Data Reconciliation Tool

## Обзор

Этот пакет содержит модульные тесты для инструмента сверки данных между Oracle и PostgreSQL (Форма 110 КХД).

## Файлы

- `test_reconciliation.py` - основные модульные тесты
- `reconciliation_module.py` - извлеченный из Jupyter Notebook модуль для тестирования
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
Тесты класса конфигурации:
- `test_config_creation_with_required_fields` - создание с обязательными полями
- `test_config_default_values` - значения по умолчанию
- `test_config_empty_composite_keys_raises_error` - валидация composite_keys
- `test_config_with_all_options` - создание со всеми опциями

### TestMetadataInspector
Тесты инспектора метаданных:
- `test_metadata_inspector_initialization` - инициализация
- `test_get_table_columns` - получение информации о колонках
- `test_classify_columns` - классификация колонок по типам
- `test_get_all_columns` - получение всех колонок

### TestDataReconciliator
Тесты основного класса сверки:
- `test_reconciliator_initialization` - инициализация
- `test_safe_compare_with_nulls` - сравнение с NULL значениями
- `test_safe_compare_without_nulls` - сравнение без NULL
- `test_safe_compare_strings` - сравнение строк
- `test_connect_creates_engines` - создание подключений (с моками)
- `test_disconnect_sets_engines_to_none` - закрытие подключений

### TestIntegration
Интеграционные тесты:
- `test_full_workflow_with_mocks` - полный рабочий процесс
- `test_config_validation` - валидация конфигурации

## Мокирование

Тесты используют моки для:
- SQLAlchemy engine и inspector
- Подключений к базам данных
- Библиотек oracledb и psycopg2

Это позволяет запускать тесты без реальных подключений к БД.

## Статистика

- Всего тестов: 18
- Покрытие компонентов:
  - ReconciliationConfig: 4 теста
  - MetadataInspector: 4 теста
  - DataReconciliator: 6 тестов
  - Интеграционные: 2 теста
  - Утилиты: 2 теста
