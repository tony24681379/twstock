/**
 * 數字格式化工具函數
 * 提供統一的數字格式化邏輯，避免重複代碼
 */

/**
 * 格式化成交量（K/M 單位）
 * @param volume - 成交量數值
 * @returns 格式化後的成交量字符串
 * @example
 * formatVolume(1500000) // "1.50M"
 * formatVolume(5000) // "5K"
 * formatVolume(500) // "500"
 */
export const formatVolume = (volume: number): string => {
  if (volume >= 1000000) {
    return `${(volume / 1000000).toFixed(2)}M`
  } else if (volume >= 1000) {
    return `${(volume / 1000).toFixed(0)}K`
  }
  return volume.toString()
}

/**
 * 格式化數字（處理 null 值）
 * @param num - 數字或 null
 * @returns 格式化後的數字字符串
 * @example
 * formatNumber(1234.56) // "1,234.56"
 * formatNumber(null) // "-"
 */
export const formatNumber = (num: number | null): string => {
  if (num === null || num === undefined) return "-"
  return num.toLocaleString("zh-TW")
}

/**
 * 格式化數字（帶正負號）
 * @param num - 數字或 null
 * @returns 格式化後的數字字符串（帶正負號）
 * @example
 * formatNumberWithSign(1234.56) // "+1,234.56"
 * formatNumberWithSign(-1234.56) // "-1,234.56"
 * formatNumberWithSign(null) // "-"
 */
export const formatNumberWithSign = (num: number | null): string => {
  if (num === null || num === undefined) return "-"
  const formatted = Math.abs(num).toLocaleString("zh-TW")
  return num >= 0 ? `+${formatted}` : `-${formatted}`
}

/**
 * 格式化百分比
 * @param value - 數值
 * @param decimals - 小數位數（預設 2）
 * @returns 格式化後的百分比字符串
 * @example
 * formatPercent(0.1234) // "12.34%"
 * formatPercent(0.1234, 1) // "12.3%"
 */
export const formatPercent = (value: number, decimals: number = 2): string => {
  return `${(value * 100).toFixed(decimals)}%`
}

/**
 * 格式化價格（固定小數位）
 * @param price - 價格數值
 * @param decimals - 小數位數（預設 2）
 * @returns 格式化後的價格字符串
 * @example
 * formatPrice(1234.567) // "1,234.57"
 * formatPrice(1234.567, 1) // "1,234.6"
 */
export const formatPrice = (price: number, decimals: number = 2): string => {
  return price.toLocaleString("zh-TW", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

/**
 * 格式化大數字（自動使用 K/M/B 單位）
 * @param num - 數字
 * @param decimals - 小數位數（預設 1）
 * @returns 格式化後的字符串
 * @example
 * formatLargeNumber(1500000) // "1.5M"
 * formatLargeNumber(1500000000) // "1.5B"
 */
export const formatLargeNumber = (
  num: number,
  decimals: number = 1
): string => {
  if (num >= 1000000000) {
    return `${(num / 1000000000).toFixed(decimals)}B`
  } else if (num >= 1000000) {
    return `${(num / 1000000).toFixed(decimals)}M`
  } else if (num >= 1000) {
    return `${(num / 1000).toFixed(decimals)}K`
  }
  return num.toString()
}
