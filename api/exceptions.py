"""
自定義異常類別
提供更精確的錯誤處理和類型安全
"""


class TwstockAPIException(Exception):
    """API 異常基類"""
    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class StockNotFoundError(TwstockAPIException):
    """股票不存在異常"""
    def __init__(self, stock_id: str):
        super().__init__(
            message=f"Stock {stock_id} not found",
            status_code=404
        )
        self.stock_id = stock_id


class InvalidStockIdError(TwstockAPIException):
    """無效的股票代碼異常"""
    def __init__(self, stock_id: str):
        super().__init__(
            message=f"Invalid stock ID format: {stock_id}",
            status_code=400
        )
        self.stock_id = stock_id


class ValidationError(TwstockAPIException):
    """驗證錯誤異常"""
    def __init__(self, message: str, field: str = None):
        super().__init__(message=message, status_code=400)
        self.field = field


class DataNotAvailableError(TwstockAPIException):
    """數據不可用異常"""
    def __init__(self, resource: str, stock_id: str = None):
        message = f"{resource} not available"
        if stock_id:
            message += f" for stock {stock_id}"
        super().__init__(message=message, status_code=404)
        self.resource = resource
        self.stock_id = stock_id


class DatabaseError(TwstockAPIException):
    """資料庫錯誤異常"""
    def __init__(self, message: str, operation: str = None):
        super().__init__(message=message, status_code=500)
        self.operation = operation


class ExternalAPIError(TwstockAPIException):
    """外部 API 錯誤異常"""
    def __init__(self, service: str, message: str):
        super().__init__(
            message=f"External API error ({service}): {message}",
            status_code=502
        )
        self.service = service


class CalculationError(TwstockAPIException):
    """計算錯誤異常"""
    def __init__(self, calculation_type: str, message: str):
        super().__init__(
            message=f"Calculation error ({calculation_type}): {message}",
            status_code=500
        )
        self.calculation_type = calculation_type


class InsufficientDataError(TwstockAPIException):
    """數據不足異常"""
    def __init__(self, required: int, actual: int, data_type: str = "weeks"):
        super().__init__(
            message=f"Insufficient data: required {required} {data_type}, got {actual}",
            status_code=400
        )
        self.required = required
        self.actual = actual
        self.data_type = data_type
