/**
 * 日期格式化工具函數
 * 提供統一的日期格式化邏輯，避免重複代碼
 */

/**
 * 提取純日期部分（移除時間）
 * @param dateStr - 日期字符串，可能包含時間部分
 * @returns 純日期字符串 (YYYY-MM-DD)
 * @example
 * extractDateOnly("2025-10-01T00:00:00") // "2025-10-01"
 * extractDateOnly("2025-10-01") // "2025-10-01"
 */
export const extractDateOnly = (dateStr: string): string => {
  return dateStr.split("T")[0]
}

/**
 * 格式化日期為 MM/DD 格式（用於圖表 X 軸）
 * @param dateStr - 日期字符串
 * @returns MM/DD 格式的日期
 * @example
 * formatDateForAxis("2025-10-01") // "10/01"
 */
export const formatDateForAxis = (dateStr: string): string => {
  const dateOnly = extractDateOnly(dateStr)
  const [_, month, day] = dateOnly.split("-")
  return `${month}/${day}`
}

/**
 * 格式化日期為台灣本地格式
 * @param dateStr - 日期字符串
 * @returns 台灣本地格式的日期
 * @example
 * formatDateTW("2025-10-01") // "2025/10/1"
 */
export const formatDateTW = (dateStr: string): string => {
  return new Date(dateStr).toLocaleDateString("zh-TW")
}

/**
 * 格式化日期為短格式（月/日）
 * @param dateStr - 日期字符串
 * @returns 短格式日期
 * @example
 * formatDateShort("2025-10-01T00:00:00") // "10/1"
 */
export const formatDateShort = (dateStr: string): string => {
  const date = new Date(dateStr)
  return `${date.getMonth() + 1}/${date.getDate()}`
}

/**
 * 格式化日期為完整格式（包含星期）
 * @param dateStr - 日期字符串
 * @returns 完整格式日期
 * @example
 * formatDateFull("2025-10-01") // "2025年10月1日 星期三"
 */
export const formatDateFull = (dateStr: string): string => {
  return new Date(dateStr).toLocaleDateString("zh-TW", {
    year: "numeric",
    month: "long",
    day: "numeric",
    weekday: "long",
  })
}
