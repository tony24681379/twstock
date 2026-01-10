/**
 * 籌碼集中度相關閾值常量
 */

/**
 * 籌碼變化顏色閾值
 */
export const CONCENTRATION_THRESHOLDS = {
  MAJOR_CHANGE: 0.2, // 主要變化閾值（顏色加深）
  MINOR_CHANGE: 0.1, // 次要變化閾值
  ZERO: 0, // 零變化
} as const

/**
 * 訊號強度分級閾值
 */
export const SIGNAL_STRENGTH_THRESHOLDS = {
  STRONG: 80, // 強烈訊號
  MODERATE: 60, // 中等訊號
  WEAK: 40, // 弱訊號
} as const

/**
 * 風險等級對應
 */
export const RISK_LEVELS = {
  LOW: "低",
  MEDIUM: "中",
  HIGH: "高",
} as const
