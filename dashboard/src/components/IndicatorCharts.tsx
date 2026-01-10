import { Bar, CartesianGrid, ComposedChart, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

interface IndicatorChartsProps {
  data: any[]
  indicators: {
    MACD?: { MACD: (number | null)[]; signal: (number | null)[]; histogram: (number | null)[] }
    KD?: { k: (number | null)[]; d: (number | null)[] }
    RSI?: (number | null)[]
    BB?: { upper: (number | null)[]; middle: (number | null)[]; lower: (number | null)[] }
  }
  darkMode?: boolean
}

export default function IndicatorCharts({ data, indicators, darkMode = false }: IndicatorChartsProps) {
  // 準備圖表資料
  const chartData = data.map((d, index) => ({
    time: new Date(d.time).toLocaleDateString('zh-TW', { month: '2-digit', day: '2-digit' }),
    macd: indicators.MACD?.MACD[index],  // 注意：MACD 欄位是大寫
    signal: indicators.MACD?.signal[index],
    histogram: indicators.MACD?.histogram[index],
    k: indicators.KD?.k[index],
    d: indicators.KD?.d[index],
    rsi: indicators.RSI?.[index],
  }))

  const gridColor = darkMode ? '#374151' : '#E5E7EB'
  const textColor = darkMode ? '#9CA3AF' : '#6B7280'
  const tooltipBg = darkMode ? '#1F2937' : '#FFFFFF'
  const tooltipBorder = darkMode ? '#374151' : '#E5E7EB'

  return (
    <div className="space-y-6">
      {/* MACD 圖表 */}
      {indicators.MACD && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">MACD（平滑異同移動平均線）</h4>
          <ResponsiveContainer width="100%" height={150}>
            <ComposedChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
              <XAxis dataKey="time" stroke={textColor} tick={{ fontSize: 12 }} />
              <YAxis stroke={textColor} tick={{ fontSize: 12 }} />
              <Tooltip
                contentStyle={{ backgroundColor: tooltipBg, border: `1px solid ${tooltipBorder}` }}
              />
              <Line type="monotone" dataKey="macd" stroke="#3B82F6" strokeWidth={2} dot={false} name="MACD" />
              <Line type="monotone" dataKey="signal" stroke="#F97316" strokeWidth={2} dot={false} name="Signal" />
              <Bar dataKey="histogram" fill="#10B981" opacity={0.6} name="Histogram" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* RSI 圖表 */}
      {indicators.RSI && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">RSI（相對強弱指標）</h4>
          <ResponsiveContainer width="100%" height={120}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
              <XAxis dataKey="time" stroke={textColor} tick={{ fontSize: 12 }} />
              <YAxis domain={[0, 100]} stroke={textColor} tick={{ fontSize: 12 }} />
              <Tooltip
                contentStyle={{ backgroundColor: tooltipBg, border: `1px solid ${tooltipBorder}` }}
              />
              <Line
                type="monotone"
                dataKey="rsi"
                stroke="#8B5CF6"
                strokeWidth={2}
                dot={false}
                name="RSI"
              />
              {/* 超買超賣線 */}
              <Line type="monotone" dataKey={() => 70} stroke="#EF4444" strokeDasharray="5 5" dot={false} name="超買 70" />
              <Line type="monotone" dataKey={() => 30} stroke="#10B981" strokeDasharray="5 5" dot={false} name="超賣 30" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* KD 圖表 */}
      {indicators.KD && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">KD（隨機指標）</h4>
          <ResponsiveContainer width="100%" height={120}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
              <XAxis dataKey="time" stroke={textColor} tick={{ fontSize: 12 }} />
              <YAxis domain={[0, 100]} stroke={textColor} tick={{ fontSize: 12 }} />
              <Tooltip
                contentStyle={{ backgroundColor: tooltipBg, border: `1px solid ${tooltipBorder}` }}
              />
              <Line type="monotone" dataKey="k" stroke="#EC4899" strokeWidth={2} dot={false} name="K" />
              <Line type="monotone" dataKey="d" stroke="#8B5CF6" strokeWidth={2} dot={false} name="D" />
              {/* 超買超賣線 */}
              <Line type="monotone" dataKey={() => 80} stroke="#EF4444" strokeDasharray="5 5" dot={false} name="超買 80" />
              <Line type="monotone" dataKey={() => 20} stroke="#10B981" strokeDasharray="5 5" dot={false} name="超賣 20" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
}
