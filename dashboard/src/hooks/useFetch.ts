/**
 * API 請求自定義 Hook
 * 封裝 fetch 操作的狀態管理
 */

import { useAsync } from "./useAsync"

const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000"

/**
 * 使用 fetch 的 Hook
 * @param endpoint - API 端點路徑（相對於 base URL）
 * @param dependencies - 依賴數組（當依賴變化時重新請求）
 * @returns 請求狀態和數據
 */
export const useFetch = <T>(endpoint: string, dependencies: any[] = []) => {
  return useAsync<T>(async () => {
    const response = await fetch(`${API_BASE_URL}${endpoint}`)

    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`HTTP ${response.status}: ${errorText}`)
    }

    return response.json()
  }, [endpoint, ...dependencies])
}

/**
 * 使用 POST 請求的 Hook
 * @param endpoint - API 端點路徑
 * @param body - 請求主體
 * @param dependencies - 依賴數組
 * @returns 請求狀態和數據
 */
export const usePost = <T, B = any>(
  endpoint: string,
  body: B,
  dependencies: any[] = []
) => {
  return useAsync<T>(async () => {
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    })

    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`HTTP ${response.status}: ${errorText}`)
    }

    return response.json()
  }, [endpoint, JSON.stringify(body), ...dependencies])
}
