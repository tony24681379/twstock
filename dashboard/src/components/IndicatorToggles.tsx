interface IndicatorTogglesProps {
  enabled: string[]
  onChange: (indicators: string[]) => void
}

export default function IndicatorToggles({ enabled, onChange }: IndicatorTogglesProps) {
  const indicators = [
    { value: 'MA', label: 'MA 均線', color: 'bg-blue-500' },
    { value: 'MACD', label: 'MACD', color: 'bg-purple-500' },
    { value: 'KD', label: 'KD', color: 'bg-green-500' },
    { value: 'RSI', label: 'RSI', color: 'bg-orange-500' },
    { value: 'BB', label: '布林通道', color: 'bg-pink-500' },
  ]

  const toggleIndicator = (value: string) => {
    if (enabled.includes(value)) {
      onChange(enabled.filter(i => i !== value))
    } else {
      onChange([...enabled, value])
    }
  }

  return (
    <div className="flex flex-wrap gap-2">
      {indicators.map((indicator) => (
        <button
          key={indicator.value}
          onClick={() => toggleIndicator(indicator.value)}
          className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
            enabled.includes(indicator.value)
              ? `${indicator.color} text-white shadow-md`
              : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-600'
          }`}
        >
          {indicator.label}
        </button>
      ))}
    </div>
  )
}
