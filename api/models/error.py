"""錯誤回應 Models"""

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    """API 錯誤回應"""

    error: str = Field(..., description="錯誤訊息")
    detail: str = Field(..., description="詳細錯誤資訊")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Resource not found",
                "detail": "Stock ID 9999 not found",
            }
        }
