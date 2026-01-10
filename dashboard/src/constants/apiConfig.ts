/**
 * API 配置常量
 */

/**
 * 圖表週期選項
 */
export const CHART_PERIODS = ["1M", "3M", "6M", "1Y"] as const
export type ChartPeriod = (typeof CHART_PERIODS)[number]

/**
 * 技術指標選項
 */
export const INDICATOR_OPTIONS = ["MA", "MACD", "KD", "RSI", "BB"] as const
export type IndicatorType = (typeof INDICATOR_OPTIONS)[number]

/**
 * 排序欄位選項
 */
export const SORT_FIELDS = {
  STOCK_ID: "stock_id",
  SIGNAL_STRENGTH: "signal_strength",
  EXPECTED_RETURN: "expected_return",
  WIN_RATE: "win_rate",
  SIGNAL_COUNT: "signal_count",
} as const

export type SortField = (typeof SORT_FIELDS)[keyof typeof SORT_FIELDS]

/**
 * 排序方向
 */
export const SORT_ORDERS = {
  ASC: "asc",
  DESC: "desc",
} as const

export type SortOrder = (typeof SORT_ORDERS)[keyof typeof SORT_ORDERS]

/**
 * 分頁配置
 */
export const PAGINATION = {
  DEFAULT_PAGE_SIZE: 100,
  MAX_PAGE_SIZE: 10000,
  LOAD_ALL_LIMIT: 5000, // 用於載入所有股票
} as const
