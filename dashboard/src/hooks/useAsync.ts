/**
 * 異步操作自定義 Hook
 * 簡化異步數據載入的狀態管理
 */

import { useCallback, useEffect, useState } from "react"

export interface AsyncState<T> {
  data: T | null
  loading: boolean
  error: Error | null
}

/**
 * 使用異步操作的 Hook
 * @param asyncFn - 異步函數
 * @param dependencies - 依賴數組（當依賴變化時重新執行）
 * @returns 異步狀態和重新執行函數
 */
export const useAsync = <T>(
  asyncFn: () => Promise<T>,
  dependencies: any[] = []
) => {
  const [state, setState] = useState<AsyncState<T>>({
    data: null,
    loading: true,
    error: null,
  })

  const execute = useCallback(async () => {
    setState({ data: null, loading: true, error: null })

    try {
      const data = await asyncFn()
      setState({ data, loading: false, error: null })
    } catch (error) {
      setState({
        data: null,
        loading: false,
        error: error instanceof Error ? error : new Error("Unknown error"),
      })
    }
  }, dependencies)

  useEffect(() => {
    execute()
  }, [execute])

  return { ...state, refetch: execute }
}
