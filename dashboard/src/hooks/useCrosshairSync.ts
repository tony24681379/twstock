/**
 * Crosshair 同步自定義 Hook
 * 用於 K線圖和籌碼圖之間的十字線同步
 */

import { useCallback, useState } from "react"
import type { ConcentrationDataPoint } from "../types/stock"
import { extractDateOnly } from "../utils/dateFormatters"

export interface SyncedConcentrationData {
  weekLabel: string
  date: string
  largeHolders: number
  superLargeHolders: number
  retail: number
}

/**
 * 使用 Crosshair 同步的 Hook
 * @param concentrationData - 籌碼集中度歷史數據
 * @returns 同步狀態和處理函數
 */
export const useCrosshairSync = (
  concentrationData: ConcentrationDataPoint[] | null
) => {
  const [syncedTime, setSyncedTime] = useState<string | null>(null)
  const [syncedConcentrationData, setSyncedConcentrationData] =
    useState<SyncedConcentrationData | null>(null)

  /**
   * 處理 crosshair 移動事件
   * 查找對應的籌碼數據並更新狀態
   */
  const handleCrosshairMove = useCallback(
    (time: string | null) => {
      setSyncedTime(time)

      if (time && concentrationData) {
        // 找到第一個日期 >= K線日期的籌碼數據
        const matched = concentrationData.find(
          (d) => extractDateOnly(d.date) >= extractDateOnly(time)
        )

        if (matched) {
          setSyncedConcentrationData({
            weekLabel: matched.week_label,
            date: extractDateOnly(matched.date),
            largeHolders: matched.moreThan400_pct,
            superLargeHolders: matched.moreThan1000_pct,
            retail: matched.lessThan20_pct,
          })
        } else {
          setSyncedConcentrationData(null)
        }
      } else {
        setSyncedConcentrationData(null)
      }
    },
    [concentrationData]
  )

  /**
   * 清除同步狀態
   */
  const clearSync = useCallback(() => {
    setSyncedTime(null)
    setSyncedConcentrationData(null)
  }, [])

  return {
    syncedTime,
    syncedConcentrationData,
    handleCrosshairMove,
    clearSync,
  }
}
