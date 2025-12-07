"""
資料庫工具函數
統一處理日期轉換、資料類型轉換等常見問題
"""

from datetime import datetime
from typing import Any, Optional, Union
import pandas as pd


def to_python_datetime(value: Any) -> Optional[datetime]:
    """
    將各種日期格式轉換為 Python datetime
    
    Args:
        value: 可能是 Pandas Timestamp、字串、datetime 或 None
    
    Returns:
        Python datetime 物件或 None
    """
    if value is None or pd.isna(value):
        return None
    
    # 優先檢查 Pandas Timestamp（在檢查 datetime 之前）
    # 因為 Pandas Timestamp 繼承自 datetime，所以 isinstance(ts, datetime) 會返回 True
    type_str = str(type(value))
    if 'pandas._libs.tslibs.timestamps.Timestamp' in type_str or 'Timestamp' in type_str:
        try:
            result = value.to_pydatetime()
            return result
        except Exception as e:
            # 回退方法：使用基本的 datetime 建構器
            try:
                return datetime(value.year, value.month, value.day, value.hour, value.minute, value.second, value.microsecond)
            except:
                pass
    
    # 如果已經是純 Python datetime，直接返回
    if isinstance(value, datetime):
        return value
    
    # 如果是 Pandas Timestamp（通用檢查）
    if hasattr(value, 'to_pydatetime'):
        try:
            result = value.to_pydatetime()
            return result
        except:
            pass
    
    # 如果是字串
    if isinstance(value, str):
        try:
            # 嘗試 ISO 格式
            return datetime.fromisoformat(value.replace('T', ' ').replace('Z', ''))
        except:
            try:
                # 嘗試其他格式
                return pd.to_datetime(value).to_pydatetime()
            except:
                return None
    
    # 嘗試用 pandas 轉換
    try:
        result = pd.to_datetime(value)
        if hasattr(result, 'to_pydatetime'):
            return result.to_pydatetime()
        return result
    except:
        return None


def safe_float(value: Any) -> Optional[float]:
    """
    安全地轉換為浮點數
    
    Args:
        value: 任何值
    
    Returns:
        float 或 None
    """
    if value is None or pd.isna(value):
        return None
    
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    """
    安全地轉換為整數
    
    Args:
        value: 任何值
    
    Returns:
        int 或 None
    """
    if value is None or pd.isna(value):
        return None
    
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def escape_sql_identifier(identifier: str) -> str:
    """
    為 SQL 識別符加上引號（處理保留字）
    
    Args:
        identifier: SQL 識別符（欄位名、表名等）
    
    Returns:
        加上引號的識別符
    """
    # SQLite 保留字列表（部分）
    SQLITE_RESERVED_WORDS = {
        'foreign', 'key', 'primary', 'unique', 'check', 'default',
        'index', 'table', 'view', 'trigger', 'transaction', 'database',
        'select', 'insert', 'update', 'delete', 'from', 'where',
        'group', 'order', 'by', 'limit', 'offset', 'join', 'on',
        'union', 'all', 'distinct', 'having', 'between', 'like',
        'in', 'exists', 'case', 'when', 'then', 'else', 'end'
    }
    
    if identifier.lower() in SQLITE_RESERVED_WORDS:
        return f'"{identifier}"'
    return identifier


def prepare_dataframe_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    準備 DataFrame 以儲存到資料庫
    
    Args:
        df: 原始 DataFrame
    
    Returns:
        處理過的 DataFrame
    """
    # 複製以避免修改原始資料
    df = df.copy()
    
    # 處理所有欄位，確保沒有 Pandas Timestamp
    for col in df.columns:
        # 檢查是否為日期相關欄位或是 datetime 類型
        if 'date' in col.lower() or 'time' in col.lower() or pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                # 轉換為 Python datetime
                df[col] = pd.to_datetime(df[col])
                df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) and hasattr(x, 'to_pydatetime') else x)
            except:
                pass
    
    # 處理數值欄位
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        # 將 inf 和 -inf 轉換為 None
        df[col] = df[col].replace([float('inf'), float('-inf')], None)
    
    return df


def get_db_compatible_sql(sql: str, db_type: str = 'sqlite') -> str:
    """
    根據資料庫類型調整 SQL 語法
    
    Args:
        sql: 原始 SQL
        db_type: 資料庫類型 ('sqlite', 'postgresql', 'mysql')
    
    Returns:
        調整後的 SQL
    """
    if db_type == 'sqlite':
        # SQLite 使用 INSERT OR REPLACE
        sql = sql.replace('ON CONFLICT', 'OR REPLACE INTO')
        sql = sql.replace('DO UPDATE SET', '')
        sql = sql.replace('ON DUPLICATE KEY UPDATE', 'OR REPLACE INTO')
    elif db_type == 'postgresql':
        # PostgreSQL 使用 ON CONFLICT
        sql = sql.replace('INSERT OR REPLACE', 'INSERT')
        sql = sql.replace('OR REPLACE INTO', 'ON CONFLICT DO UPDATE SET')
    elif db_type == 'mysql':
        # MySQL 使用 ON DUPLICATE KEY UPDATE
        sql = sql.replace('INSERT OR REPLACE', 'INSERT')
        sql = sql.replace('ON CONFLICT', 'ON DUPLICATE KEY UPDATE')
    
    return sql


class DatabaseError(Exception):
    """資料庫相關錯誤的基礎類別"""
    pass


class DataConversionError(DatabaseError):
    """資料轉換錯誤"""
    pass


class SQLExecutionError(DatabaseError):
    """SQL 執行錯誤"""
    pass