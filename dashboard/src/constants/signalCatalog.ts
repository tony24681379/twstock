export interface SignalDef {
  name: string
  score: number
}

export interface SignalCategory {
  key: string
  label: string
  signals: SignalDef[]
}

export const SIGNAL_CATALOG: SignalCategory[] = [
  {
    key: 'chip',
    label: '籌碼',
    signals: [
      { name: '完美結構', score: 20 },
      { name: '超大戶連買2週', score: 15 },
      { name: '大戶連買3週', score: 12 },
      { name: '大戶急買', score: 10 },
      { name: '加速集中', score: 10 },
      { name: '10週持續買', score: 8 },
      { name: '散戶連賣3週', score: 8 },
      { name: '散戶恐慌', score: 6 },
      { name: '出貨訊號', score: -15 },
      { name: '散戶狂熱', score: -10 },
    ],
  },
  {
    key: 'technical_state',
    label: '技術狀態',
    signals: [
      { name: '三線合一向上', score: 20 },
      { name: '四線合一向上', score: 18 },
      { name: '多頭排列', score: 18 },
      { name: 'MACD多頭', score: 12 },
      { name: '創60日新高', score: 12 },
      { name: 'DMI向上', score: 10 },
      { name: 'DMI向下', score: -10 },
      { name: '創60日新低', score: -12 },
      { name: '空頭排列', score: -18 },
      { name: '四線合一向下', score: -18 },
      { name: '三線合一向下', score: -20 },
    ],
  },
  {
    key: 'technical_crossover',
    label: '技術交叉',
    signals: [
      { name: '黃金交叉', score: 15 },
      { name: '長紅吞噬', score: 15 },
      { name: '跳空向上', score: 12 },
      { name: 'KD向上', score: 12 },
      { name: '站上季線', score: 10 },
      { name: '布林突破', score: 10 },
      { name: '跌破季線', score: -10 },
      { name: '跳空向下', score: -12 },
      { name: '死亡交叉', score: -15 },
      { name: '長黑吞噬', score: -15 },
    ],
  },
  {
    key: 'event',
    label: '極端事件',
    signals: [
      { name: '漲停板', score: 10 },
      { name: '大漲訊號', score: 5 },
      { name: '跌停開板', score: 5 },
      { name: '大跌警示', score: -5 },
      { name: '跌停板', score: -10 },
      { name: '開盤跌停', score: -12 },
      { name: '跌停爆量', score: -15 },
      { name: '連續跌停', score: -20 },
    ],
  },
  {
    key: 'fundamental',
    label: '基本面',
    signals: [
      { name: 'EPS連續成長', score: 25 },
      { name: '由虧轉正', score: 18 },
      { name: '營收連續正成長', score: 15 },
      { name: '本益比合理', score: 12 },
      { name: '高股利殖利率', score: 12 },
      { name: '營收加速成長', score: 12 },
      { name: 'PSR偏低', score: 12 },
      { name: '營收由衰轉增', score: 10 },
      { name: '股利連續成長', score: 10 },
      { name: 'EPS穩定', score: 8 },
      { name: '高配息率', score: 5 },
      { name: 'PSR過高', score: -10 },
      { name: '本益比過高', score: -12 },
      { name: '股利大幅削減', score: -12 },
      { name: '營收連續衰退', score: -15 },
      { name: '由正轉虧', score: -18 },
      { name: '營收急凍', score: -18 },
      { name: 'EPS連續衰退', score: -25 },
    ],
  },
  {
    key: 'cb',
    label: '可轉債',
    signals: [
      { name: '折價套利', score: 20 },
      { name: '低溢價率', score: 15 },
      { name: '催換在即', score: 15 },
      { name: '到期賣回保護', score: 12 },
      { name: '標的股強勢', score: 12 },
      { name: '深度價內', score: 10 },
      { name: '低於面額', score: 8 },
      { name: '到期逼近', score: -10 },
      { name: '標的股弱勢', score: -15 },
      { name: '高溢價風險', score: -15 },
    ],
  },
]
