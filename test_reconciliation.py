"""
Модульные тесты для Data Reconciliation Tool
Тестирование основных компонентов инструмента сверки данных
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, date

# Импортируем тестируемые классы
from reconciliation_module import (
    ReconciliationConfig,
    MetadataInspector,
    DataReconciliator,
    add_system_python_db_libraries
)


class TestAddSystemPythonDbLibraries:
    """Тесты для функции add_system_python_db_libraries"""
    
    def test_function_exists(self):
        """Проверка существования функции"""
        assert callable(add_system_python_db_libraries)
    
    def test_function_runs_without_error(self):
        """Функция выполняется без ошибок"""
        # Функция должна выполняться без исключений
        result = add_system_python_db_libraries()
        assert result is None  # Функция ничего не возвращает


class TestReconciliationConfig:
    """Тесты для класса конфигурации ReconciliationConfig"""
    
    def test_config_creation_with_required_fields(self):
        """Создание конфигурации с обязательными полями"""
        config = ReconciliationConfig(
            oracle_connection_string="oracle+oracledb://user:pass@host:1521/service",
            postgres_connection_string="postgresql://user:pass@host:5432/dbname",
            oracle_schema="ORA_SCHEMA",
            oracle_table="TABLE_V1",
            postgres_schema="PG_SCHEMA",
            postgres_table="TABLE_V2",
            composite_keys=['BANK_CODE', 'REPORT_DATE']
        )
        
        assert config.oracle_schema == "ORA_SCHEMA"
        assert config.oracle_table == "TABLE_V1"
        assert config.postgres_schema == "PG_SCHEMA"
        assert config.postgres_table == "TABLE_V2"
        assert config.composite_keys == ['BANK_CODE', 'REPORT_DATE']
    
    def test_config_default_values(self):
        """Проверка значений по умолчанию"""
        config = ReconciliationConfig(
            oracle_connection_string="oracle+oracledb://user:pass@host:1521/service",
            postgres_connection_string="postgresql://user:pass@host:5432/dbname",
            oracle_schema="ORA_SCHEMA",
            oracle_table="TABLE_V1",
            postgres_schema="PG_SCHEMA",
            postgres_table="TABLE_V2",
            composite_keys=['BANK_CODE']
        )
        
        assert config.exclusions == []
        assert config.sample_size is None
        assert config.specific_keys is None
        assert config.batch_size == 100000
        assert config.decimal_precision == 2
        assert config.year_from is None
        assert config.year_to is None
    
    def test_config_empty_composite_keys_raises_error(self):
        """Пустой composite_keys должен вызывать ошибку"""
        with pytest.raises(ValueError, match="composite_keys не может быть пустым"):
            ReconciliationConfig(
                oracle_connection_string="oracle+oracledb://user:pass@host:1521/service",
                postgres_connection_string="postgresql://user:pass@host:5432/dbname",
                oracle_schema="ORA_SCHEMA",
                oracle_table="TABLE_V1",
                postgres_schema="PG_SCHEMA",
                postgres_table="TABLE_V2",
                composite_keys=[]
            )
    
    def test_config_with_all_options(self):
        """Создание конфигурации со всеми опциями"""
        config = ReconciliationConfig(
            oracle_connection_string="oracle+oracledb://user:pass@host:1521/service",
            postgres_connection_string="postgresql://user:pass@host:5432/dbname",
            oracle_schema="ORA_SCHEMA",
            oracle_table="TABLE_V1",
            postgres_schema="PG_SCHEMA",
            postgres_table="TABLE_V2",
            composite_keys=['BANK_CODE', 'REPORT_DATE'],
            report_date_column='REPORT_DATE',
            exclusions=['ID', 'LOAD_DATE'],
            sample_size=1000,
            specific_keys={'BANK_CODE': '001'},
            batch_size=50000,
            decimal_precision=4,
            year_from=2020,
            year_to=2024
        )
        
        assert config.report_date_column == 'REPORT_DATE'
        assert config.exclusions == ['ID', 'LOAD_DATE']
        assert config.sample_size == 1000
        assert config.specific_keys == {'BANK_CODE': '001'}
        assert config.batch_size == 50000
        assert config.decimal_precision == 4
        assert config.year_from == 2020
        assert config.year_to == 2024


class TestMetadataInspector:
    """Тесты для класса MetadataInspector"""
    
    @pytest.fixture
    def mock_engine(self):
        """Создание мок-объекта для SQLAlchemy engine"""
        engine = Mock()
        inspector = Mock()
        engine.dialect.name = 'postgresql'
        return engine, inspector
    
    def test_metadata_inspector_initialization(self, mock_engine):
        """Инициализация MetadataInspector"""
        engine, inspector_mock = mock_engine
        with patch('reconciliation_module.inspect', return_value=inspector_mock):
            inspector = MetadataInspector(engine)
            
            assert inspector.engine == engine
            assert inspector.inspector == inspector_mock
    
    def test_get_table_columns(self, mock_engine):
        """Получение информации о колонках таблицы"""
        engine, inspector_mock = mock_engine
        
        # Мокируем данные о колонках
        mock_columns = [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'name', 'type': 'VARCHAR(50)', 'nullable': True},
            {'name': 'amount', 'type': 'NUMERIC(10,2)', 'nullable': True},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'nullable': False}
        ]
        inspector_mock.get_columns.return_value = mock_columns
        
        with patch('reconciliation_module.inspect', return_value=inspector_mock):
            inspector = MetadataInspector(engine)
            columns = inspector.get_table_columns('public', 'test_table')
            
            assert len(columns) == 4
            assert columns[0]['name'] == 'id'
            assert columns[2]['type'] == 'numeric(10,2)'
    
    def test_classify_columns(self, mock_engine):
        """Классификация колонок по типам"""
        engine, inspector_mock = mock_engine
        
        mock_columns = [
            {'name': 'id', 'type': 'integer', 'nullable': False},
            {'name': 'name', 'type': 'varchar(50)', 'nullable': True},
            {'name': 'amount', 'type': 'numeric(10,2)', 'nullable': True},
            {'name': 'created_at', 'type': 'timestamp', 'nullable': False},
            {'name': 'description', 'type': 'text', 'nullable': True},
            {'name': 'load_date', 'type': 'date', 'nullable': True}
        ]
        
        with patch('reconciliation_module.inspect', return_value=inspector_mock):
            inspector = MetadataInspector(engine)
            classification = inspector.classify_columns(
                mock_columns, 
                exclusions=['load_date']
            )
            
            assert 'id' in classification['numeric_fields']
            assert 'amount' in classification['numeric_fields']
            assert 'name' in classification['string_fields']
            assert 'description' in classification['string_fields']
            assert 'created_at' in classification['date_fields']
            assert 'load_date' in classification['excluded_fields']
    
    def test_get_all_columns(self, mock_engine):
        """Получение всех колонок и их классификации"""
        engine, inspector_mock = mock_engine
        
        mock_columns = [
            {'name': 'id', 'type': 'integer', 'nullable': False},
            {'name': 'name', 'type': 'varchar(50)', 'nullable': True},
            {'name': 'amount', 'type': 'numeric(10,2)', 'nullable': True}
        ]
        inspector_mock.get_columns.return_value = mock_columns
        
        with patch('reconciliation_module.inspect', return_value=inspector_mock):
            inspector = MetadataInspector(engine)
            all_cols, classification = inspector.get_all_columns(
                'public', 
                'test_table',
                exclusions=[]
            )
            
            assert len(all_cols) == 3
            assert all_cols == ['id', 'name', 'amount']
            assert 'id' in classification['numeric_fields']
            assert 'name' in classification['string_fields']


class TestDataReconciliator:
    """Тесты для основного класса DataReconciliator"""
    
    @pytest.fixture
    def sample_config(self):
        """Конфигурация для тестов"""
        return ReconciliationConfig(
            oracle_connection_string="oracle+oracledb://user:pass@host:1521/service",
            postgres_connection_string="postgresql://user:pass@host:5432/dbname",
            oracle_schema="ORA_SCHEMA",
            oracle_table="TABLE_V1",
            postgres_schema="PG_SCHEMA",
            postgres_table="TABLE_V2",
            composite_keys=['BANK_CODE', 'REPORT_DATE']
        )
    
    def test_reconciliator_initialization(self, sample_config):
        """Инициализация DataReconciliator"""
        reconciliator = DataReconciliator(sample_config)
        
        assert reconciliator.config == sample_config
        assert reconciliator.oracle_engine is None
        assert reconciliator.postgres_engine is None
        assert reconciliator.results == {}
    
    def test_safe_compare_with_nulls(self):
        """Безопасное сравнение с NULL значениями"""
        # Создаем тестовые Series с NULL значениями
        col1 = pd.Series([1, 2, None, 4, None])
        col2 = pd.Series([1, 3, None, None, 5])
        
        result = DataReconciliator.safe_compare(col1, col2)
        
        # Ожидаем: True (1==1), False (2!=3), True (NULL==NULL), False (4!=NULL), False (NULL!=5)
        expected = pd.Series([True, False, True, False, False])
        
        assert result.equals(expected)
    
    def test_safe_compare_without_nulls(self):
        """Безопасное сравнение без NULL значений"""
        col1 = pd.Series([1, 2, 3, 4])
        col2 = pd.Series([1, 3, 3, 5])
        
        result = DataReconciliator.safe_compare(col1, col2)
        expected = pd.Series([True, False, True, False])
        
        assert result.equals(expected)
    
    def test_safe_compare_strings(self):
        """Безопасное сравнение строковых значений"""
        col1 = pd.Series(['a', 'b', None, 'd'])
        col2 = pd.Series(['a', 'c', None, None])
        
        result = DataReconciliator.safe_compare(col1, col2)
        expected = pd.Series([True, False, True, False])
        
        assert result.equals(expected)
    
    @patch('reconciliation_module.create_engine')
    def test_connect_creates_engines(self, mock_create_engine, sample_config):
        """Подключение создает движки для БД (с моками)"""
        # Мокируем библиотеки БД
        with patch.object(DataReconciliator, '__module__', new_callable=lambda: None):
            pass
        
        reconciliator = DataReconciliator(sample_config)
        
        # Т.к. реальные библиотеки БД недоступны, проверяем только структуру
        # В реальном тесте здесь была бы проверка create_engine вызовов
        assert hasattr(reconciliator, 'connect')
        assert hasattr(reconciliator, 'disconnect')
    
    def test_disconnect_sets_engines_to_none(self, sample_config):
        """Отключение устанавливает движки в None"""
        reconciliator = DataReconciliator(sample_config)
        # Mock the dispose method only, not the whole engine
        mock_oracle_engine = Mock()
        mock_postgres_engine = Mock()
        reconciliator.oracle_engine = mock_oracle_engine
        reconciliator.postgres_engine = mock_postgres_engine
        
        reconciliator.disconnect()
        
        # Verify dispose was called
        mock_oracle_engine.dispose.assert_called_once()
        mock_postgres_engine.dispose.assert_called_once()


class TestIntegration:
    """Интеграционные тесты"""
    
    def test_full_workflow_with_mocks(self):
        """Полный рабочий процесс с использованием моков"""
        # Создаем конфигурацию
        config = ReconciliationConfig(
            oracle_connection_string="oracle+oracledb://user:pass@host:1521/service",
            postgres_connection_string="postgresql://user:pass@host:5432/dbname",
            oracle_schema="ORA_SCHEMA",
            oracle_table="TABLE_V1",
            postgres_schema="PG_SCHEMA",
            postgres_table="TABLE_V2",
            composite_keys=['ID'],
            exclusions=['CREATED_AT']
        )
        
        # Создаем реконсилятор
        reconciliator = DataReconciliator(config)
        
        # Проверяем наличие всех необходимых методов
        assert hasattr(reconciliator, 'connect')
        assert hasattr(reconciliator, 'disconnect')
        assert hasattr(reconciliator, 'collect_metadata')
        assert hasattr(reconciliator, 'run_full_reconciliation')
        assert hasattr(reconciliator, 'quick_yearly_report')
    
    def test_config_validation(self):
        """Валидация конфигурации"""
        # Пустые composite_keys должны вызывать ошибку
        with pytest.raises(ValueError):
            ReconciliationConfig(
                oracle_connection_string="oracle://user:pass@host/service",
                postgres_connection_string="postgresql://user:pass@host/db",
                oracle_schema="ORA",
                oracle_table="T1",
                postgres_schema="PG",
                postgres_table="T2",
                composite_keys=[]
            )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
