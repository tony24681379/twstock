import { useState, useEffect } from 'react'

interface Weights {
  chip: number
  fundamental: number
}

interface Props {
  onChange: (weights: Weights) => void
  className?: string
}

export function WeightController({ onChange, className = '' }: Props) {
  // 從 localStorage 讀取儲存的權重（持久化）
  const initialWeights: Weights = (() => {
    const saved = localStorage.getItem('strengthWeights')
    if (saved) {
      try {
        const parsed = JSON.parse(saved)
        // 檢查是否為新格式（只有 chip 和 fundamental），且總和為 1.0
        if (parsed.chip !== undefined && parsed.fundamental !== undefined && !parsed.technical) {
          const sum = parsed.chip + parsed.fundamental
          if (Math.abs(sum - 1.0) < 0.01) {
            return parsed
          }
        }
      } catch {
        // fallback to default
      }
    }
    // 使用預設值並儲存到 localStorage
    const defaultWeights = { chip: 0.5, fundamental: 0.5 }
    localStorage.setItem('strengthWeights', JSON.stringify(defaultWeights))
    return defaultWeights
  })()

  // 本地權重狀態（拖動時即時更新）
  const [weights, setWeights] = useState<Weights>(initialWeights)

  // 已套用的權重（按下按鈕後才更新）
  const [appliedWeights, setAppliedWeights] = useState<Weights>(initialWeights)

  // 檢查是否有未套用的變更
  const hasChanges =
    weights.chip !== appliedWeights.chip ||
    weights.fundamental !== appliedWeights.fundamental

  // 首次載入時通知父元件
  useEffect(() => {
    onChange(initialWeights)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // 調整單個權重，自動調整另一個以維持總和 = 1.0
  const handleChange = (type: keyof Weights, value: number) => {
    const newWeights = { ...weights }
    newWeights[type] = value

    // 另一個權重自動調整
    const other: keyof Weights = type === 'chip' ? 'fundamental' : 'chip'
    newWeights[other] = 1.0 - value

    setWeights(newWeights)
  }

  // 套用權重變更
  const handleApply = () => {
    setAppliedWeights(weights)
    localStorage.setItem('strengthWeights', JSON.stringify(weights))
    onChange(weights)
  }

  // 重置到已套用的值
  const handleReset = () => {
    setWeights(appliedWeights)
  }

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg shadow p-4 ${className}`}>
      <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
        綜合強度權重配置
      </h3>

      <div className="space-y-3">
        {/* 籌碼權重 */}
        <div>
          <div className="flex justify-between items-center mb-1">
            <label className="text-xs text-gray-600 dark:text-gray-400">
              籌碼
            </label>
            <span className="text-xs font-mono text-gray-700 dark:text-gray-300">
              {(weights.chip * 100).toFixed(0)}%
            </span>
          </div>
          <input
            type="range"
            min="0"
            max="100"
            value={weights.chip * 100}
            onChange={(e) => handleChange('chip', Number(e.target.value) / 100)}
            className="w-full h-2 bg-gray-200 dark:bg-gray-700 rounded-lg appearance-none cursor-pointer accent-blue-600"
          />
        </div>

        {/* 基本面權重 */}
        <div>
          <div className="flex justify-between items-center mb-1">
            <label className="text-xs text-gray-600 dark:text-gray-400">
              基本面
            </label>
            <span className="text-xs font-mono text-gray-700 dark:text-gray-300">
              {(weights.fundamental * 100).toFixed(0)}%
            </span>
          </div>
          <input
            type="range"
            min="0"
            max="100"
            value={weights.fundamental * 100}
            onChange={(e) => handleChange('fundamental', Number(e.target.value) / 100)}
            className="w-full h-2 bg-gray-200 dark:bg-gray-700 rounded-lg appearance-none cursor-pointer accent-purple-600"
          />
        </div>
      </div>

      {/* 顯示總和（應始終為 100%，用於驗證） */}
      <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-700">
        <div className="flex justify-between items-center text-xs mb-3">
          <span className="text-gray-500 dark:text-gray-400">總和</span>
          <span className="font-mono text-gray-700 dark:text-gray-300">
            {((weights.chip + weights.fundamental) * 100).toFixed(0)}%
          </span>
        </div>

        {/* 操作按鈕 */}
        <div className="flex gap-2">
          <button
            onClick={handleApply}
            disabled={!hasChanges}
            className={`flex-1 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              hasChanges
                ? 'bg-blue-600 hover:bg-blue-700 text-white'
                : 'bg-gray-200 dark:bg-gray-700 text-gray-400 dark:text-gray-500 cursor-not-allowed'
            }`}
          >
            套用權重
          </button>
          <button
            onClick={handleReset}
            disabled={!hasChanges}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              hasChanges
                ? 'bg-gray-200 hover:bg-gray-300 dark:bg-gray-700 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300'
                : 'bg-gray-100 dark:bg-gray-800 text-gray-400 dark:text-gray-600 cursor-not-allowed'
            }`}
          >
            重置
          </button>
        </div>

        {/* 變更提示 */}
        {hasChanges && (
          <div className="mt-2 text-xs text-orange-600 dark:text-orange-400">
            * 權重已變更，按下「套用權重」以重新載入資料
          </div>
        )}
      </div>
    </div>
  )
}
