export interface ChipSignal {
  name: string
  triggered: boolean
  score: number
  description?: string
}

export interface AlertStatus {
  is_attention_stock: boolean
  is_disposal_stock: boolean

  // 注意股詳情
  attention_count?: number
  attention_reason?: string
  attention_date?: string

  // 處置股詳情
  disposal_type?: string
  disposal_announced_date?: string
  disposal_start_date?: string
  disposal_end_date?: string
  disposal_condition?: string
  disposal_measure?: string
  disposal_content?: string

  // 系統預警
  system_warning: boolean
  warning_score: number
  warning_reasons: string[]
}

export interface StockListItem {
  stock_id: string
  name: string
  close_price: number

  // 警示狀態（新增）
  alert_status?: AlertStatus

  // 三種強度（籌碼 + 技術 + 基本面）- 原始分數（可為負數）
  chip_strength: number        // -25 to +89（原始分數）
  technical_strength: number   // -120 to +164（原始分數）
  fundamental_strength: number // -110 to +139（原始分數）
  overall_strength: number     // 0-100（標準化分數）

  signal_count: number
  expected_return: number
  win_rate: number
  risk_level: string

  // 訊號列表
  chip_signals: ChipSignal[]
  technical_signals?: ChipSignal[]    // 技術訊號（optional）
  fundamental_signals?: ChipSignal[]  // 基本面訊號（optional）
  recent_events?: ChipSignal[]       // 近期事件（漲跌停等）

  // 權重配置（optional）
  weights?: {
    chip: number
    technical: number
    fundamental: number
  }

  // 可轉債套利
  cb_arbitrage_score?: number | null  // 0-100，無 CB 為 null
  cb_signals?: ChipSignal[]           // 可轉債套利訊號

  // 向後相容
  signal_strength: number // 映射為 overall_strength
  signals: ChipSignal[] // 完整訊號列表
  major_signals: string[] // 主要訊號列表

  last_updated: string
}

// 可轉債列表項目
export interface ConvertibleBondListItem {
  bond_id: string
  name: string
  underlying_stock_id: string
  underlying_stock_name: string
  close: number | null
  conversion_price: number | null
  conversion_value: number | null
  premium_rate: number | null
  arbitrage_spread: number | null
  normalized_score: number
  signal_count: number
  signals: CBSignal[]
  risk_level: string
  maturity_date: string | null
  volume: number | null
  last_updated: string | null
}

// 可轉債訊號
export interface CBSignal {
  name: string
  triggered: boolean
  score: number
  description?: string
}

// 可轉債詳情
export interface ConvertibleBondDetail {
  bond_id: string
  name: string
  underlying_stock_id: string
  underlying_stock_name: string
  issue_date: string | null
  maturity_date: string | null
  put_date: string | null
  put_price: number | null
  coupon_rate: number | null
  issued_amount: number | null
  outstanding_amount: number | null
  close: number | null
  volume: number | null
  conversion_price: number | null
  conversion_value: number | null
  premium_rate: number | null
  arbitrage_spread: number | null
  underlying_close: number | null
  underlying_overall_strength: number
  normalized_score: number
  raw_score: number
  signal_count: number
  risk_level: string
  signals: CBSignal[]
}

// 可轉債歷史資料點
export interface ConvertibleBondHistoryPoint {
  date: string
  close: number | null
  volume: number | null
  conversion_value: number | null
  premium_rate: number | null
  underlying_close: number | null
}

// 可轉債列表回應
export interface ConvertibleBondListResponse {
  data: ConvertibleBondListItem[]
  pagination: PaginationMetadata
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

  // 警示狀態（新增）
  alert_status?: AlertStatus

  // 訊號列表
  chip_signals: ChipSignal[]
  technical_signals?: ChipSignal[]    // 技術訊號（optional）
  fundamental_signals?: ChipSignal[]  // 基本面訊號（optional）
  recent_events?: ChipSignal[]       // 近期事件（漲跌停等）

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
  fundamental_strength: number   // -110 to +139（原始分數）

  // 🆕 營收成長
  revenue_recent_12m?: number[]        // 最近12個月營收（千元）
  revenue_yoy_avg?: number             // 平均年增率 (%)
  revenue_trend?: string               // 營收趨勢（上升/下降/持平）

  // 🆕 詳細數據（Tabs 架構用）
  eps_details?: EPSDetail[]             // 逐季 EPS 詳細資料
  capital_info?: CapitalInfo            // 股本資訊
  holding_info?: HoldingInfo            // 持股結構
  revenue_details?: MonthlyRevenueDetail[]  // 月營收詳細資料

  // 🆕 PSR / EPS 預測 / 歷史股利
  psr?: number | null                   // 股價營收比
  eps_prediction?: EPSPrediction | null // EPS 預測
  dividend_history?: DividendDetail[]   // 歷史股利（最近 5 年）
}

// 🆕 EPS 預測結果
export interface EPSPrediction {
  value: number                // 預測 EPS
  target_year: number          // 預測目標年度
  target_quarter: number       // 預測目標季度 (1-4)
  method: string               // 預測方法
  is_prediction: boolean       // 是否為預測值
}

// 🆕 歷史股利資料
export interface DividendDetail {
  year: number                 // 年度（民國年）
  cash_dividend: number        // 現金股利
  stock_dividend: number       // 股票股利
}
