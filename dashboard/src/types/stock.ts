export interface ChipSignal {
  name: string
  triggered: boolean
  score: number
  description?: string
}

export interface StockListItem {
  stock_id: string
  name: string
  close_price: number

  // 兩種強度（籌碼 + 基本面）- 原始分數（可為負數）
  chip_strength: number        // -25 to +89（原始分數）
  fundamental_strength: number // -73 to +93（原始分數）
  overall_strength: number     // 0-100（標準化分數）

  signal_count: number
  expected_return: number
  win_rate: number
  risk_level: string

  // 訊號列表
  chip_signals: ChipSignal[]
  fundamental_signals?: ChipSignal[] // 基本面訊號（optional）

  // 權重配置（optional）
  weights?: {
    chip: number
    fundamental: number
  }

  // 向後相容
  signal_strength: number // 映射為 overall_strength
  signals: ChipSignal[] // 完整訊號列表
  major_signals: string[] // 主要訊號列表

  last_updated: string
}

export interface PaginationMetadata {
  total: number
  page: number
  page_size: number
  total_pages: number
  has_next: boolean
  has_prev: boolean
}

export interface StockListResponse {
  data: StockListItem[]
  pagination: PaginationMetadata
}

export interface BasicInfo {
  stock_id: string
  name: string
  industry?: string
  outstanding_shares?: number
}

export interface PriceInfo {
  close: number
  open: number
  high: number
  low: number
  volume: number
  date: string
  change: number
  change_percent: number
}

export interface ConcentrationSummary {
  large_holders_pct: number
  super_large_holders_pct: number
  retail_pct: number
  latest_date: string
}

export interface StockDetail {
  basic_info: BasicInfo
  price_info: PriceInfo

  // 訊號列表
  chip_signals: ChipSignal[]
  fundamental_signals?: ChipSignal[] // 基本面訊號（optional）

  expected_return: number
  win_rate: number
  concentration_summary: ConcentrationSummary

  // 向後相容
  signal_strength: number
  signals: ChipSignal[]
}

export interface ConcentrationDataPoint {
  week_label: string
  date: string
  moreThan400_pct: number
  moreThan1000_pct: number
  lessThan20_pct: number
  moreThan400_change: number
  moreThan1000_change: number
  lessThan20_change: number
}

export interface StockHistory {
  stock_id: string
  data: ConcentrationDataPoint[]
}

// 🆕 逐季 EPS 詳細資料
export interface EPSDetail {
  year: number
  quarter: number
  eps: number
}

// 🆕 股本與股數資訊
export interface CapitalInfo {
  capital: number | null              // 實收資本額（百萬）
  outstanding_shares: number | null   // 股本（千股）
  stock_dividend: number | null       // 股票股利
}

// 🆕 持股結構資訊
export interface HoldingInfo {
  director_ratio: number | null                    // 董監持股比率 (%)
  foreign_holding_rate: number | null              // 外資持股率 (%)
  investment_trust_holding_rate: number | null     // 投信持股率 (%)
  dealer_holding_rate: number | null               // 自營商持股率 (%)
  latest_date: string | null                       // 最新資料日期
}

// 🆕 月營收詳細資料
export interface MonthlyRevenueDetail {
  year: number                          // 年份
  month: number                         // 月份 (1-12)
  revenue: number                       // 月營收（千元）
  mom_change: number | null             // 月增率 (%)
  yoy_change: number | null             // 年增率 (%)
  cumulative_revenue: number | null     // 累計營收（千元）
  cumulative_yoy_change: number | null  // 累計年增率 (%)
}

// 基本面詳細資訊介面（詳細頁專用）
export interface FundamentalInfo {
  // 獲利能力
  eps_recent_4q: number[]       // 最近 4 季 EPS
  eps_trend: string             // 上升/下降/持平
  eps_stability: number         // 標準差
  eps_avg: number               // 平均 EPS

  // 估值指標
  per: number | null            // 本益比
  dividend_yield: number        // 股利殖利率 (%)
  payout_ratio: number          // 配息率 (%)
  cash_dividend: number         // 現金股利

  // 訊號
  fundamental_signals: ChipSignal[]
  fundamental_strength: number   // -73 to +93（原始分數）

  // 🆕 營收成長
  revenue_recent_12m?: number[]        // 最近12個月營收（千元）
  revenue_yoy_avg?: number             // 平均年增率 (%)
  revenue_trend?: string               // 營收趨勢（上升/下降/持平）

  // 🆕 詳細數據（Tabs 架構用）
  eps_details?: EPSDetail[]             // 逐季 EPS 詳細資料
  capital_info?: CapitalInfo            // 股本資訊
  holding_info?: HoldingInfo            // 持股結構
  revenue_details?: MonthlyRevenueDetail[]  // 月營收詳細資料
}
