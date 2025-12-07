# -*- coding: utf-8 -*-
import asyncio
import os
import time
from datetime import date

import pandas as pd

from twstock import Stock
from twstock.fetcher_manager import get_global_fetcher

INDEX = [
    "id",
    "最高價",
    "最大漲幅",
    "最低價",
    "最大跌幅",
    "收盤價",
    "漲跌幅",
    "成交量",
    "資本額",
    "波段天數",
    "波段漲跌幅",
    "趨勢天數",
    "趨勢漲跌幅",
    "波段高點",
    "高點差距",
    "波段低點",
    "低點差距",
    "K",
    "D",
    "ADX",
]
SKILL_INDEX = [
    "本日外本比",
    "本週外本比",
    "本月外本比",
    "外資10日買天數",
    "外資10日賣天數",
    "外資佔比",
    "本日投本比",
    "本週投本比",
    "本月投本比",
    "投信10日買天數",
    "投信10日賣天數",
    "投信佔比",
    "本日自本比",
    "本週自本比",
    "本月自本比",
    "自營10日買天數",
    "自營10日賣天數",
    "自營商佔比",
    "三大法人佔比",
    "本日主力買賣超",
    "本週主力買賣超",
    "本月主力買賣超",
    "主力10日買天數",
    "主力10日賣天數",
    "本日買賣家數差",
    "本週籌碼集中度",
    "本月籌碼集中度",
    "本日融資餘額比",
    "本週融資餘額比",
    "本月融資餘額比",
    "本日融券餘額比",
    "本週融券餘額比",
    "本月融券餘額比",
    "本日券資比",
    "本週券資比",
    "本月券資比",
    "近期新高",
    "近期新低",
    "三線合一向上",
    "四線合一向上",
    "跳空向上",
    "長紅吞噬",
    "5 20黃金交叉",
    "KD向上",
    "MACD>0",
    "布林通道上軌",
    "DMI向上",
    "多頭排列",
    "季線以上",
    "三線合一向下",
    "四線合一向下",
    "跳空向下",
    "長黑吞噬",
    "5 20死亡交叉",
    "空頭排列",
    "季線以下",
]

INDEX_COLUMN = {"id": "股票代碼"}
NAME_COLUMN = {
    "name": "股票名稱",
    "industries_name": "上市/櫃",
    "industries_shortName": "產業",
}
INFO_COLUMN = {
    "outstanding_shares": "發行股數",
    "cash_dividend": "現金股利",
    "stock_dividend": "股票股利",
}

ORGANIZATION = "config/organization.csv"


class All:
    def __init__(self):
        self.fetcher = None  # 將在需要時初始化
        self.results = []  # 初始化處理結果列表

    async def _ensure_fetcher(self):
        """確保 fetcher 已初始化"""
        if self.fetcher is None:
            self.fetcher = await get_global_fetcher()

    async def get_all_stock_list(self):
        await self._ensure_fetcher()
        self.list = await self.fetcher.get_all_stock_list()

        # 保存股票清單到資料庫
        if hasattr(self.fetcher, "db_manager") and self.fetcher.db_manager:
            try:
                print("💾 保存股票清單到資料庫...")
                await self.fetcher.db_manager.save_stock_list(self.list)
                print(f"✅ 已保存 {len(self.list)} 支股票到資料庫")
            except Exception as e:
                print(f"❌ 保存股票清單失敗: {e}")

        # 處理 pandas DataFrame（用於 Excel 報表生成）
        try:
            self.stock_list = pd.json_normalize(
                self.list,
                record_path="industries",
                record_prefix="industries_",
                meta=["id", "name", "type"],
            )[["id", "name", "industries_name", "industries_shortName"]]
            self.stock_list = self.stock_list.assign(
                industries_name=self.stock_list.industries_name.str[:2]
            )
            self.stock_list = pd.merge(
                self.stock_list,
                pd.read_csv(ORGANIZATION, dtype={"id": object, "集團": object}),
                how="left",
                on=["id"],
            ).rename(columns=NAME_COLUMN)
        except Exception as e:
            print(f"⚠️  處理股票清單 DataFrame 時發生錯誤: {e}")
            # 如果 pandas 處理失敗，至少保持原始清單可用
            pass

    async def consumer(self, q):
        try:
            while True:
                id = await q.get()
                result = await self.get_stock(id)
                self.results.append(result)
                q.task_done()

        except asyncio.CancelledError:
            pass

    async def get_all_stock_parallel(self):
        """
        批次處理所有股票的主流程

        流程:
        1. 從 API 取得所有股票清單（在 get_all_stock_list 已完成）
        2. 從 DB 批次檢查哪些股票需要從 API 更新（比對 API 最新日期）
        3. 先處理需要從 API 更新的股票，抓取並保存到資料庫
        4. 批次載入所有股票資料（包含剛更新的）
        5. 分析所有股票並收集結果
        6. 生成 Excel 和 HTML 報告
        """
        startTime = time.time()
        self.results = []

        # 步驟1: 取得所有股票 ID（排除指數）
        all_stock_ids = [
            l["id"].lower() for l in self.list if l["id"] not in ("000-", "0000")
        ]
        total_stocks = len(all_stock_ids)
        print(f"📋 共有 {total_stocks} 支股票需要處理")

        # 步驟2: 從 DB 批次檢查哪些股票需要從 API 更新
        print(f"🔍 批次檢查股票更新狀態...")
        needs_update_map = {}
        if hasattr(self.fetcher, "db_manager") and self.fetcher.db_manager:
            needs_update_map = await self.fetcher.db_manager.bulk_check_needs_update(
                all_stock_ids, days_threshold=1
            )
            needs_update_count = sum(1 for v in needs_update_map.values() if v)
            print(f"   ✓ {needs_update_count}/{total_stocks} 支股票需要從 API 更新")
            print(
                f"   ✓ {total_stocks - needs_update_count}/{total_stocks} 支股票從 DB 載入"
            )
        else:
            print("   ⚠️  未啟用資料庫，所有股票將從 API 取得")
            needs_update_map = {sid: True for sid in all_stock_ids}

        # 步驟3: 先處理需要從 API 更新的股票
        stocks_need_update = [
            sid for sid, needs_update in needs_update_map.items() if needs_update
        ]

        if stocks_need_update:
            print(f"\n📡 先更新需要從 API 取得的 {len(stocks_need_update)} 支股票...")
            update_start_time = time.time()

            MAX_API_WORKERS = int(os.getenv("MAX_API_WORKERS", "10"))
            api_semaphore = asyncio.Semaphore(MAX_API_WORKERS)

            async def update_stock_from_api(sid):
                """從 API 更新股票並保存到資料庫"""
                async with api_semaphore:
                    try:
                        stock = Stock(sid, fetcher=self.fetcher)
                        # load_data 會自動從 API 抓取並保存到資料庫
                        await stock.load_data()
                        return sid, True
                    except Exception as e:
                        print(f"   ❌ {sid} 更新失敗: {e}")
                        return sid, False

            # 創建更新任務
            update_tasks = [update_stock_from_api(sid) for sid in stocks_need_update]

            print(f"   並發更新數: {MAX_API_WORKERS}")

            updated_count = 0
            failed_count = 0

            for task in asyncio.as_completed(update_tasks):
                sid, success = await task
                if success:
                    updated_count += 1
                else:
                    failed_count += 1

                # 進度顯示
                if updated_count % 10 == 0 or updated_count <= 50:
                    elapsed = time.time() - update_start_time
                    speed = updated_count / elapsed if elapsed > 0 else 0
                    remaining = len(stocks_need_update) - updated_count - failed_count
                    eta = remaining / speed if speed > 0 else 0
                    print(
                        f"   更新進度: [{updated_count + failed_count}/{len(stocks_need_update)}] "
                        f"成功: {updated_count} 失敗: {failed_count} - "
                        f"速度: {speed:.2f} 支/秒 - 預估剩餘: {int(eta/60)}分{int(eta%60)}秒"
                    )

            update_elapsed = time.time() - update_start_time
            print(f"\n   ✓ API 更新完成！成功: {updated_count} 失敗: {failed_count}")
            print(f"   耗時: {int(update_elapsed/60)}分{int(update_elapsed%60)}秒")

        # 步驟4: 批次載入所有股票資料（包含剛更新的）
        print(f"\n📥 批次載入所有股票資料...")
        bulk_daily_data = {}
        bulk_info_data = {}

        if hasattr(self.fetcher, "db_manager"):
            print(f"   載入 {len(all_stock_ids)} 支股票的資料...")
            # 使用批次 SQL 查詢（只查詢一次！）
            bulk_daily_data = await self.fetcher.db_manager.bulk_load_daily_data(
                all_stock_ids, days=490
            )
            bulk_info_data = await self.fetcher.db_manager.bulk_load_stock_info(
                all_stock_ids
            )
            print(f"   ✓ 已載入 {len(bulk_daily_data)} 支股票的每日資料")
            print(f"   ✓ 已載入 {len(bulk_info_data)} 支股票的基本資訊")

        # 步驟5: 處理所有股票（統一從批次載入的資料處理）
        print(f"\n🚀 開始分析所有股票...")
        start_time = time.time()

        MAX_WORKERS = int(os.getenv("MAX_WORKERS", "20"))
        semaphore = asyncio.Semaphore(MAX_WORKERS)

        async def process_with_semaphore(sid):
            async with semaphore:
                # 使用批次載入的資料
                if sid in bulk_daily_data and sid in bulk_info_data:
                    return await self.get_stock_from_bulk_data(
                        sid, bulk_info_data[sid], bulk_daily_data[sid]
                    )
                else:
                    # 如果批次載入失敗，返回空結果
                    return (
                        sid,
                        pd.Series(index=INDEX + SKILL_INDEX),
                        pd.Series(),
                        pd.DataFrame({}),
                    )

        # 創建所有任務
        tasks = [process_with_semaphore(sid) for sid in all_stock_ids]

        print(f"   並發處理數: {MAX_WORKERS}")

        total_processed = 0
        # 使用 as_completed 處理完成的任務
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                self.results.append(result)
                total_processed += 1

                # 進度顯示
                if total_processed % 10 == 0 or total_processed <= 50:
                    progress_pct = total_processed * 100 / total_stocks
                    elapsed_time = time.time() - start_time
                    avg_speed = (
                        total_processed / elapsed_time if elapsed_time > 0 else 0
                    )
                    remaining = total_stocks - total_processed
                    eta_seconds = remaining / avg_speed if avg_speed > 0 else 0
                    eta_minutes = int(eta_seconds / 60)
                    eta_sec = int(eta_seconds % 60)

                    print(
                        f"   分析進度: [{total_processed}/{total_stocks}] ({progress_pct:.1f}%) "
                        f"- 速度: {avg_speed:.2f} 支/秒 - 預估剩餘: {eta_minutes}分{eta_sec}秒"
                    )

                # 每100支顯示詳細報告
                if total_processed % 100 == 0:
                    elapsed_minutes = int(elapsed_time / 60)
                    elapsed_seconds = int(elapsed_time % 60)
                    print(
                        f"\n   === 已分析 {total_processed}/{total_stocks} 支股票 ==="
                    )
                    print(f"       已用時間: {elapsed_minutes}分{elapsed_seconds}秒")

            except Exception as e:
                print(f"   ❌ 處理失敗: {e}")
                # 創建空結果避免中斷
                self.results.append(
                    (
                        "unknown",
                        pd.Series(index=INDEX + SKILL_INDEX),
                        pd.Series(),
                        pd.DataFrame({}),
                    )
                )

        total_time = time.time() - start_time
        total_minutes = int(total_time / 60)
        total_seconds = int(total_time % 60)

        print(f"\n✅ 股票分析完成！")
        print(f"   總計: {len(self.results)} 支股票")
        print(f"   分析耗時: {total_minutes}分{total_seconds}秒")
        print(f"   分析速度: {total_processed/total_time:.2f} 支/秒")

        # 步驟6: 整理結果並生成 Excel 和 HTML
        print(f"\n📊 整理資料並生成報表...")

        name_list = {}
        skill_list = {}
        info_list = {}
        daily_list = {}

        # 收集所有結果
        for id, skill, info, daily in iter(self.results):
            skill_list[id] = skill
            info_list[id] = info
            daily_list[id] = daily

        for l in self.list:
            name_list[l["id"]] = l

        # 生成 Excel
        if len(skill_list) > 0:
            print(f"   生成 Excel: {len(skill_list)} 支股票")

            # 技術籌碼資料
            skill_df = pd.DataFrame(dict(skill_list)).T
            # 檢查是否已有 'id' 欄位，如果有則先刪除
            if "id" in skill_df.columns:
                skill_df = skill_df.drop(columns=["id"])
            skill_list_excel = skill_df.reset_index().rename(columns={"index": "id"})

            # 基本面資料
            info_df = pd.DataFrame(dict(info_list)).T
            # 檢查是否已有 'id' 欄位，如果有則先刪除
            if "id" in info_df.columns:
                info_df = info_df.drop(columns=["id"])
            info_df = info_df.reset_index().rename(columns={"index": "id"})

            info_list_excel = pd.merge(
                skill_list_excel.iloc[:, :12],  # 只取前12個技術欄位
                info_df,
                on=["id"],
                how="left",
            )

            # 刪除 capital 欄位（如果存在）
            if "capital" in info_list_excel.columns:
                del info_list_excel["capital"]

            # 檢查並移除任何重複的欄位
            duplicate_cols = info_list_excel.columns[
                info_list_excel.columns.duplicated()
            ].tolist()
            if duplicate_cols:
                print(f"   ⚠️  發現重複欄位: {duplicate_cols}，將自動移除")
                info_list_excel = info_list_excel.loc[
                    :, ~info_list_excel.columns.duplicated()
                ]

            # 極端漲跌資料
            extreme_list_excel = skill_list_excel[
                (skill_list_excel["最大漲幅"] > 9) | (skill_list_excel["最大跌幅"] < -9)
            ]

            # 寫入 Excel
            excel_filename = date.today().strftime("%Y%m%d") + ".xlsx"
            with pd.ExcelWriter(excel_filename) as writer:
                skill_list_excel.rename(columns=INDEX_COLUMN).to_excel(
                    writer, sheet_name="技術籌碼", index=False
                )
                info_list_excel.rename(columns=INDEX_COLUMN).to_excel(
                    writer, sheet_name="基本面", index=False
                )
                extreme_list_excel.rename(columns=INDEX_COLUMN).to_excel(
                    writer, sheet_name="極端漲跌", index=False
                )
            print(f"   ✓ Excel 已儲存: {excel_filename}")
        else:
            print("   ⚠️  沒有可用的股票資料")

        endTime = time.time()
        total_minutes = int((endTime - startTime) / 60)
        total_seconds = int((endTime - startTime) % 60)
        print(f"\n⏱️  總執行時間: {total_minutes}分{total_seconds}秒")

    def sum_days(self, data, days):
        result = sum(data[days * -1 :])
        return result if result != 0 else None

    async def get_stock_from_bulk_data(
        self, sid: str, info_data: pd.Series, daily_data: pd.DataFrame
    ):
        """使用批次載入的資料處理股票（不查詢資料庫）"""
        stock = Stock(sid, fetcher=self.fetcher)

        # 直接設置資料，不從資料庫載入
        stock.info_data = info_data
        stock.daily_data = daily_data

        # 處理資料
        if stock.daily_data is not None and len(stock.daily_data) > 0:
            stock.daily_data = stock.daily_data[::-1].copy()  # 反轉順序
            stock.calc_base()

        # 檢查是否有足夠的資料進行分析
        if not hasattr(stock, "close") or len(stock.close) < 60:
            return (
                stock.sid,
                pd.Series(index=INDEX + SKILL_INDEX),
                stock.info,
                pd.DataFrame({}),
            )

        # 計算技術指標（與 get_stock 相同的邏輯）
        wave_days = stock.continuous_trend_days(stock.wave)
        trend_days = stock.continuous_trend_days(stock.trend)

        check = [
            stock.sid,
            stock.high[-1],
            stock.calc_change(stock.high[-1], stock.close[-2]),
            stock.low[-1],
            stock.calc_change(stock.low[-1], stock.close[-2]),
            stock.close[-1],
            stock.change[-1],
            stock.volume[-1],
            stock.info.capital,
            wave_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * (abs(wave_days) + 1)]),
            trend_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * (abs(trend_days) + 1)]),
            stock.season_upper,
            stock.calc_change(stock.close[-1], stock.season_upper),
            stock.season_lower,
            stock.calc_change(stock.close[-1], stock.season_lower),
            stock.k9[-1],
            stock.d9[-1],
            stock.adx[-1],
            stock.foreign[-1] if stock.foreign[-1] != 0 else None,
            self.sum_days(stock.foreign, 5),
            self.sum_days(stock.foreign, 20),
            stock.buy_10days(stock.foreign),
            stock.sell_10days(stock.foreign),
            stock.foreign_holding_rate[-1],
            stock.investment_trust[-1] if stock.investment_trust[-1] != 0 else None,
            self.sum_days(stock.investment_trust, 5),
            self.sum_days(stock.investment_trust, 20),
            stock.buy_10days(stock.investment_trust),
            stock.sell_10days(stock.investment_trust),
            stock.investment_trust_holding_rate[-1],
            stock.dealer[-1] if stock.dealer[-1] != 0 else None,
            self.sum_days(stock.dealer, 5),
            self.sum_days(stock.dealer, 20),
            stock.buy_10days(stock.dealer),
            stock.sell_10days(stock.dealer),
            stock.dealer_holding_rate[-1],
            stock.sum_holding_rate[-1],
            stock.major_investors[-1] if stock.major_investors[-1] != 0 else None,
            self.sum_days(stock.major_investors, 5),
            self.sum_days(stock.major_investors, 20),
            stock.buy_10days(stock.major_investors),
            stock.sell_10days(stock.major_investors),
            stock.agent_diff[-1],
            stock.skp5[-1],
            stock.skp20[-1],
        ]

        if stock.balance_limit != 0:
            lending_balance_today = (
                stock.lending_balance[-1]
                if stock.lending_balance[-1] != 0
                else stock.lending_balance[-2]
            )
            borrowing_balance_today = (
                stock.borrowing_balance[-1]
                if stock.borrowing_balance[-1] != 0
                else stock.borrowing_balance[-2]
            )

            check = check + [
                round(lending_balance_today / stock.balance_limit * 100, 2),
                round(stock.lending_balance[-5] / stock.balance_limit * 100, 2),
                round(stock.lending_balance[-20] / stock.balance_limit * 100, 2),
                round(borrowing_balance_today / stock.balance_limit * 100, 2),
                round(stock.borrowing_balance[-5] / stock.balance_limit * 100, 2),
                round(stock.borrowing_balance[-20] / stock.balance_limit * 100, 2),
                (
                    round(borrowing_balance_today / lending_balance_today * 100, 2)
                    if lending_balance_today != 0
                    else 0
                ),
                (
                    round(
                        stock.borrowing_balance[-5] / stock.lending_balance[-5] * 100, 2
                    )
                    if stock.lending_balance[-5] != 0
                    else 0
                ),
                (
                    round(
                        stock.borrowing_balance[-20] / stock.lending_balance[-20] * 100,
                        2,
                    )
                    if stock.lending_balance[-20] != 0
                    else 0
                ),
            ]
        else:
            check = check + [None, None, None, None, None, None, None, None, None]

        check = check + [
            stock.is_upper(),
            stock.is_lower(),
            stock.up_three_line(),
            stock.up_four_line(),
            stock.up_jump_line(),
            stock.long_up(),
            stock.up_cross_ma5_ma20(),
            stock.up_kd(),
            stock.up_macd(),
            stock.up_bollinger(),
            stock.up_dmi(),
            stock.long(),
            stock.up_session(),
            stock.down_three_line(),
            stock.down_four_line(),
            stock.down_jump_line(),
            stock.long_down(),
            stock.down_cross_ma5_ma20(),
            stock.short(),
            stock.down_session(),
        ]

        daily = pd.DataFrame(
            {
                "date": stock.date[-60:],
                "close": stock.close[-60:],
                "volume": stock.volume[-60:],
            }
        )

        return (
            stock.sid,
            pd.Series(check, index=INDEX + SKILL_INDEX),
            stock.info,
            daily,
        )

    async def get_stock(self, sid: str):
        # 使用共享的 fetcher
        stock = Stock(sid, fetcher=self.fetcher)

        # 載入資料（會自動判斷是否需要從 API 更新）
        await stock.load_data()

        # 移除個別處理的輸出，改用總體進度顯示

        # 檢查是否有足夠的資料進行分析
        if not hasattr(stock, "close") or len(stock.close) < 60:
            return (
                stock.sid,
                pd.Series(index=INDEX + SKILL_INDEX),
                stock.info,
                pd.DataFrame({}),
            )

        wave_days = stock.continuous_trend_days(stock.wave)
        trend_days = stock.continuous_trend_days(stock.trend)

        check = [
            stock.sid,
            stock.high[-1],
            stock.calc_change(stock.high[-1], stock.close[-2]),
            stock.low[-1],
            stock.calc_change(stock.low[-1], stock.close[-2]),
            stock.close[-1],
            stock.change[-1],
            stock.volume[-1],
            stock.info.capital,
            wave_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * (abs(wave_days) + 1)]),
            trend_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * (abs(trend_days) + 1)]),
            stock.season_upper,
            stock.calc_change(stock.close[-1], stock.season_upper),
            stock.season_lower,
            stock.calc_change(stock.close[-1], stock.season_lower),
            stock.k9[-1],
            stock.d9[-1],
            stock.adx[-1],
            stock.foreign[-1] if stock.foreign[-1] != 0 else None,
            self.sum_days(stock.foreign, 5),
            self.sum_days(stock.foreign, 20),
            stock.buy_10days(stock.foreign),
            stock.sell_10days(stock.foreign),
            stock.foreign_holding_rate[-1],
            stock.investment_trust[-1] if stock.investment_trust[-1] != 0 else None,
            self.sum_days(stock.investment_trust, 5),
            self.sum_days(stock.investment_trust, 20),
            stock.buy_10days(stock.investment_trust),
            stock.sell_10days(stock.investment_trust),
            stock.investment_trust_holding_rate[-1],
            stock.dealer[-1] if stock.dealer[-1] != 0 else None,
            self.sum_days(stock.dealer, 5),
            self.sum_days(stock.dealer, 20),
            stock.buy_10days(stock.dealer),
            stock.sell_10days(stock.dealer),
            stock.dealer_holding_rate[-1],
            stock.sum_holding_rate[-1],
            stock.major_investors[-1] if stock.major_investors[-1] != 0 else None,
            self.sum_days(stock.major_investors, 5),
            self.sum_days(stock.major_investors, 20),
            stock.buy_10days(stock.major_investors),
            stock.sell_10days(stock.major_investors),
            stock.agent_diff[-1],
            stock.skp5[-1],
            stock.skp20[-1],
        ]

        if stock.balance_limit != 0:
            # to avoid 0
            lending_balance_today = (
                stock.lending_balance[-1]
                if stock.lending_balance[-1] != 0
                else stock.lending_balance[-2]
            )
            borrowing_balance_today = (
                stock.borrowing_balance[-1]
                if stock.borrowing_balance[-1] != 0
                else stock.borrowing_balance[-2]
            )

            check = check + [
                round(lending_balance_today / stock.balance_limit * 100, 2),
                round(stock.lending_balance[-5] / stock.balance_limit * 100, 2),
                round(stock.lending_balance[-20] / stock.balance_limit * 100, 2),
                round(borrowing_balance_today / stock.balance_limit * 100, 2),
                round(stock.borrowing_balance[-5] / stock.balance_limit * 100, 2),
                round(stock.borrowing_balance[-20] / stock.balance_limit * 100, 2),
                (
                    round(borrowing_balance_today / lending_balance_today * 100, 2)
                    if lending_balance_today != 0
                    else 0
                ),
                (
                    round(
                        stock.borrowing_balance[-5] / stock.lending_balance[-5] * 100, 2
                    )
                    if stock.lending_balance[-5] != 0
                    else 0
                ),
                (
                    round(
                        stock.borrowing_balance[-20] / stock.lending_balance[-20] * 100,
                        2,
                    )
                    if stock.lending_balance[-20] != 0
                    else 0
                ),
            ]
        else:
            check = check + [None, None, None, None, None, None, None, None, None]

        check = check + [
            stock.is_upper(),
            stock.is_lower(),
            stock.up_three_line(),
            stock.up_four_line(),
            stock.up_jump_line(),
            stock.long_up(),
            stock.up_cross_ma5_ma20(),
            stock.up_kd(),
            stock.up_macd(),
            stock.up_bollinger(),
            stock.up_dmi(),
            stock.long(),
            stock.up_session(),
            stock.down_three_line(),
            stock.down_four_line(),
            stock.down_jump_line(),
            stock.long_down(),
            stock.down_cross_ma5_ma20(),
            stock.short(),
            stock.down_session(),
        ]

        daily = pd.DataFrame(
            {
                "date": stock.date[-60:],
                "close": stock.close[-60:],
                "volume": stock.volume[-60:],
            }
        )

        return (
            stock.sid,
            pd.Series(check, index=INDEX + SKILL_INDEX),
            stock.info,
            daily,
        )


def init(l):
    global lock
    lock = l
