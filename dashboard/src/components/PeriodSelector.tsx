interface PeriodSelectorProps {
  value: string
  onChange: (period: string) => void
}

export default function PeriodSelector({ value, onChange }: PeriodSelectorProps) {
  const periods = [
    { value: '1M', label: '1 個月' },
    { value: '3M', label: '3 個月' },
    { value: '6M', label: '6 個月' },
    { value: '1Y', label: '1 年' },
  ]

  return (
    <div className="flex gap-2">
      {periods.map((period) => (
        <button
          key={period.value}
          onClick={() => onChange(period.value)}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            value === period.value
              ? 'bg-primary text-white'
              : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
          }`}
        >
          {period.label}
        </button>
      ))}
    </div>
  )
}
