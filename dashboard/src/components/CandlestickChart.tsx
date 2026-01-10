import { memo, useEffect, useRef, useState } from 'react'
import { INDICATOR_CONFIG } from '../constants/chartConfig'
import { formatVolume } from '../utils/numberUtils'

interface OHLCVData {
  time: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

interface CandlestickChartProps {
  data: OHLCVData[]
  indicators?: {
    MA5?: (number | null)[]
    MA10?: (number | null)[]
    MA20?: (number | null)[]
    MA60?: (number | null)[]
    [key: string]: any
  }
  onCrosshairMove?: (time: string | null) => void
  darkMode?: boolean
}

function CandlestickChart({ data, indicators = {}, onCrosshairMove, darkMode = false }: CandlestickChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<any>(null)
  const seriesRefs = useRef<any>({})

  // Tooltip 數據
  const [tooltipData, setTooltipData] = useState<{
    time: string
    open: number
    high: number
    low: number
    close: number
    volume: number
    change: number
    changePercent: number
    ma5?: number
    ma10?: number
    ma20?: number
    ma60?: number
  } | null>(null)

  useEffect(() => {
    if (!chartContainerRef.current || !data || data.length === 0) return

    let mounted = true

    // 動態載入並建立圖表
    import('lightweight-charts').then((LightweightCharts) => {
      if (!mounted || !chartContainerRef.current) return

      const { createChart, ColorType } = LightweightCharts

      // 建立圖表
      const chart = createChart(chartContainerRef.current, {
        layout: {
          background: { type: ColorType.Solid, color: darkMode ? '#1F2937' : '#FFFFFF' },
          textColor: darkMode ? '#D1D5DB' : '#374151',
        },
        grid: {
          vertLines: { color: darkMode ? '#374151' : '#E5E7EB' },
          horzLines: { color: darkMode ? '#374151' : '#E5E7EB' },
        },
        width: chartContainerRef.current.clientWidth,
        height: 400,
      })

      chartRef.current = chart

      // v5 API: 使用 addSeries
      const candleSeries = (chart as any).addSeries((LightweightCharts as any).CandlestickSeries || (LightweightCharts as any).candlestickSeries, {
        upColor: '#EF4444',
        downColor: '#10B981',
        borderUpColor: '#EF4444',
        borderDownColor: '#10B981',
        wickUpColor: '#EF4444',
        wickDownColor: '#10B981',
      })

      // 轉換資料格式
      const candleData = data.map(d => ({
        time: d.time,
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
      }))

      candleSeries.setData(candleData)
      seriesRefs.current.candle = candleSeries

      // 建立成交量 series
      const volumeSeries = (chart as any).addSeries((LightweightCharts as any).HistogramSeries || (LightweightCharts as any).histogramSeries, {
        color: '#3B82F6',
        priceFormat: {
          type: 'volume',
        },
        priceScaleId: 'volume',
      })

      chart.priceScale('volume').applyOptions({
        scaleMargins: {
          top: 0.7,           // K線佔70%，成交量佔30%
          bottom: 0,
        },
        visible: true,         // 顯示 Y 軸刻度數值
        borderVisible: false,
      })

      const volumeData = data.map(d => ({
        time: d.time,
        value: d.volume,
        color: d.close >= d.open ? '#EF444480' : '#10B98180',
      }))

      volumeSeries.setData(volumeData)
      seriesRefs.current.volume = volumeSeries

      // 加入移動平均線
      if (indicators) {
        Object.entries(indicators).forEach(([key, values]) => {
          if (key.startsWith('MA') && Array.isArray(values)) {
            const config = INDICATOR_CONFIG[key as keyof typeof INDICATOR_CONFIG]
            const maSeries = (chart as any).addSeries((LightweightCharts as any).LineSeries || (LightweightCharts as any).lineSeries, {
              color: config?.color || '#999999',
              lineWidth: 2,
            })

            const maData = values
              .map((value, index) => ({
                time: data[index]?.time,
                value: value === null || value === undefined ? undefined : value,
              }))
              .filter(d => d.value !== undefined && d.time) as any

            if (maData.length > 0) {
              maSeries.setData(maData)
              seriesRefs.current[key.toLowerCase()] = maSeries
            }
          }
        })

        // 布林通道（BB）
        if (indicators.BB && typeof indicators.BB === 'object') {
          const bb = indicators.BB as { upper: (number | null)[]; middle: (number | null)[]; lower: (number | null)[] }

          // 上軌
          if (bb.upper) {
            const upperSeries = (chart as any).addSeries((LightweightCharts as any).LineSeries || (LightweightCharts as any).lineSeries, {
              color: '#EC4899',
              lineWidth: 1,
              lineStyle: 2, // dashed
            })
            const upperData = bb.upper
              .map((value, index) => ({ time: data[index]?.time, value: value ?? undefined }))
              .filter(d => d.value !== undefined && d.time) as any
            if (upperData.length > 0) upperSeries.setData(upperData)
          }

          // 中軌
          if (bb.middle) {
            const middleSeries = (chart as any).addSeries((LightweightCharts as any).LineSeries || (LightweightCharts as any).lineSeries, {
              color: '#A855F7',
              lineWidth: 1,
            })
            const middleData = bb.middle
              .map((value, index) => ({ time: data[index]?.time, value: value ?? undefined }))
              .filter(d => d.value !== undefined && d.time) as any
            if (middleData.length > 0) middleSeries.setData(middleData)
          }

          // 下軌
          if (bb.lower) {
            const lowerSeries = (chart as any).addSeries((LightweightCharts as any).LineSeries || (LightweightCharts as any).lineSeries, {
              color: '#EC4899',
              lineWidth: 1,
              lineStyle: 2, // dashed
            })
            const lowerData = bb.lower
              .map((value, index) => ({ time: data[index]?.time, value: value ?? undefined }))
              .filter(d => d.value !== undefined && d.time) as any
            if (lowerData.length > 0) lowerSeries.setData(lowerData)
          }
        }
      }

      // 自適應大小
      chart.timeScale().fitContent()

      // 訂閱 Crosshair 移動事件
      const handleCrosshairMove = (param: any) => {
        if (!param.time || !param.seriesData || param.seriesData.size === 0) {
          onCrosshairMove?.(null)
          setTooltipData(null)
          return
        }

        // 通知父元件當前時間（用於同步其他圖表）
        onCrosshairMove?.(param.time)

        const candleData = param.seriesData.get(seriesRefs.current.candle)
        const volumeData = param.seriesData.get(seriesRefs.current.volume)

        if (candleData && volumeData) {
          const change = candleData.close - candleData.open
          const changePercent = (change / candleData.open) * 100

          setTooltipData({
            time: param.time,
            open: candleData.open,
            high: candleData.high,
            low: candleData.low,
            close: candleData.close,
            volume: volumeData.value,
            change,
            changePercent,
            ma5: param.seriesData.get(seriesRefs.current.ma5)?.value,
            ma10: param.seriesData.get(seriesRefs.current.ma10)?.value,
            ma20: param.seriesData.get(seriesRefs.current.ma20)?.value,
            ma60: param.seriesData.get(seriesRefs.current.ma60)?.value,
          })
        }
      }

      chart.subscribeCrosshairMove(handleCrosshairMove)

      // 響應式調整
      const handleResize = () => {
        if (chartContainerRef.current && chart) {
          chart.applyOptions({
            width: chartContainerRef.current.clientWidth,
          })
        }
      }

      window.addEventListener('resize', handleResize)

      // Cleanup
      return () => {
        chart.unsubscribeCrosshairMove(handleCrosshairMove)
        window.removeEventListener('resize', handleResize)
      }
    }).catch(err => {
      console.error('Failed to load lightweight-charts:', err)
    })

    return () => {
      mounted = false
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [data, indicators, darkMode, onCrosshairMove])

  return (
    <div className="w-full">
      {/* Tooltip 資訊欄 */}
      {tooltipData && (
        <div className="flex flex-wrap gap-3 mb-2 px-3 py-2 bg-gray-50 dark:bg-gray-800 rounded text-xs border border-gray-200 dark:border-gray-700">
          <span className="font-semibold text-gray-900 dark:text-gray-100">
            {new Date(tooltipData.time).toLocaleDateString('zh-TW', { month: '2-digit', day: '2-digit' })}
          </span>
          <span className="text-gray-700 dark:text-gray-300">開 {tooltipData.open.toFixed(2)}</span>
          <span className="text-gray-700 dark:text-gray-300">高 {tooltipData.high.toFixed(2)}</span>
          <span className="text-gray-700 dark:text-gray-300">低 {tooltipData.low.toFixed(2)}</span>
          <span className="text-gray-700 dark:text-gray-300">收 {tooltipData.close.toFixed(2)}</span>
          <span className={tooltipData.change >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold'}>
            {tooltipData.change >= 0 ? '▲' : '▼'}
            {Math.abs(tooltipData.change).toFixed(2)}
            ({Math.abs(tooltipData.changePercent).toFixed(2)}%)
          </span>
          <span className="text-gray-700 dark:text-gray-300">量 {formatVolume(tooltipData.volume)}</span>
          {tooltipData.ma5 && <span className="text-gray-700 dark:text-gray-300">MA5 {tooltipData.ma5.toFixed(2)}</span>}
          {tooltipData.ma10 && <span className="text-gray-700 dark:text-gray-300">MA10 {tooltipData.ma10.toFixed(2)}</span>}
          {tooltipData.ma20 && <span className="text-gray-700 dark:text-gray-300">MA20 {tooltipData.ma20.toFixed(2)}</span>}
          {tooltipData.ma60 && <span className="text-gray-700 dark:text-gray-300">MA60 {tooltipData.ma60.toFixed(2)}</span>}
        </div>
      )}

      {/* 圖例 */}
      {indicators && Object.keys(indicators).length > 0 && (
        <div className="flex flex-wrap gap-3 mb-3 px-2">
          {Object.keys(indicators).map(key => {
            const config = INDICATOR_CONFIG[key as keyof typeof INDICATOR_CONFIG]
            if (!config) return null

            return (
              <div key={key} className="flex items-center gap-2">
                {/* 顏色指示器 */}
                <div
                  className="w-8 h-0.5"
                  style={{ backgroundColor: config.color }}
                />
                {/* 名稱 */}
                <span className="text-xs font-medium text-gray-700 dark:text-gray-300">
                  {config.name}
                </span>
              </div>
            )
          })}
        </div>
      )}

      {/* 圖表容器 */}
      <div ref={chartContainerRef} className="w-full" />
    </div>
  )
}

// 使用 React.memo 避免不必要的重渲染
export default memo(CandlestickChart)
