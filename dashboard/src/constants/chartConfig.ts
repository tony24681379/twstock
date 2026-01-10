/**
 * 圖表配置常量
 */

export const CHART_DIMENSIONS = {
  CANDLESTICK_HEIGHT: 400,
  CONCENTRATION_CHART_HEIGHT: 250,
  INDICATOR_CHART_HEIGHT: 200,
} as const

export const CHART_COLORS = {
  light: {
    background: "#FFFFFF",
    gridLine: "#E5E7EB",
    text: "#374151",
    up: "#EF4444",
    down: "#10B981",
    volume: "#3B82F6",
  },
  dark: {
    background: "#1F2937",
    gridLine: "#374151",
    text: "#D1D5DB",
    up: "#EF4444",
    down: "#10B981",
    volume: "#3B82F6",
  },
} as const

/**
 * 技術指標配置（名稱、顏色）
 */
export const INDICATOR_CONFIG = {
  MA5: { name: "MA5 (5日均線)", color: "#FF6B6B" },
  MA10: { name: "MA10 (10日均線)", color: "#4ECDC4" },
  MA20: { name: "MA20 (20日均線)", color: "#FFE66D" },
  MA60: { name: "MA60 (60日均線)", color: "#A8DADC" },
  BB: { name: "布林通道", color: "#EC4899" },
} as const

/**
 * 籌碼集中度線條配置
 */
export const CONCENTRATION_LINE_CONFIG = {
  largeHolders: { name: "大戶 >400 張", color: "#3B82F6" },
  superLargeHolders: { name: "超大戶 >1000 張", color: "#F97316" },
  retail: { name: "散戶 <20 張", color: "#EF4444" },
} as const
