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

  // 三種強度
  chip_strength: number
  technical_strength: number
  overall_strength: number

  signal_count: number
  expected_return: number
  win_rate: number
  risk_level: string

  // 訊號列表
  chip_signals: ChipSignal[] // 新增：籌碼訊號
  technical_signals: ChipSignal[] // 新增：技術訊號

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
  technical_signals: ChipSignal[]

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
