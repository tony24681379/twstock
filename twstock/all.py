# -*- coding: utf-8 -*-
import asyncio
import datetime
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

        # 步驟3.5: 批次更新月營收資料
        print(f"\n📈 批次更新所有股票的月營收資料...")
        revenue_start_time = time.time()

        MAX_REVENUE_WORKERS = int(os.getenv("MAX_API_WORKERS", "10"))
        revenue_semaphore = asyncio.Semaphore(MAX_REVENUE_WORKERS)

        async def update_monthly_revenue(sid):
            """更新單支股票的月營收資料"""
            async with revenue_semaphore:
                try:
                    if hasattr(self.fetcher, "fetch_monthly_revenue"):
                        await self.fetcher.fetch_monthly_revenue(
                            sid, months=12, save_to_db=True
                        )
                        return sid, True
                    else:
                        return sid, False
                except Exception as e:
                    # 忽略錯誤，某些股票可能沒有月營收資料
                    return sid, False

        # 創建月營收更新任務（所有股票）
        revenue_tasks = [update_monthly_revenue(sid) for sid in all_stock_ids]

        print(f"   並發更新數: {MAX_REVENUE_WORKERS}")

        revenue_updated_count = 0
        revenue_failed_count = 0

        for task in asyncio.as_completed(revenue_tasks):
            sid, success = await task
            if success:
                revenue_updated_count += 1
            else:
                revenue_failed_count += 1

            # 進度顯示
            if revenue_updated_count % 50 == 0:
                elapsed = time.time() - revenue_start_time
                speed = revenue_updated_count / elapsed if elapsed > 0 else 0
                remaining = total_stocks - revenue_updated_count - revenue_failed_count
                eta = remaining / speed if speed > 0 else 0
                print(
                    f"   月營收更新進度: [{revenue_updated_count + revenue_failed_count}/{total_stocks}] "
                    f"成功: {revenue_updated_count} - "
                    f"速度: {speed:.2f} 支/秒 - 預估剩餘: {int(eta/60)}分{int(eta%60)}秒"
                )

        revenue_elapsed = time.time() - revenue_start_time
        print(
            f"\n   ✓ 月營收更新完成！成功: {revenue_updated_count} 跳過: {revenue_failed_count}"
        )
        print(f"   耗時: {int(revenue_elapsed/60)}分{int(revenue_elapsed%60)}秒")

        # 步驟4: 批次載入所有股票資料（包含剛更新的）
        print(f"\n📥 批次載入所有股票資料...")
        bulk_daily_data = {}
        bulk_info_data = {}

        if hasattr(self.fetcher, "db_manager"):
            print(f"   載入 {len(all_stock_ids)} 支股票的資料...")
            # 使用批次 SQL 查詢（只查詢一次！）
            bulk_daily_data = await self.fetcher.db_manager.bulk_load_daily_data(
                all_stock_ids, days=200  # 200 日曆日 ≈ 137 交易日，dropna 後 78 行，緩衝 18 行
            )
            bulk_info_data = await self.fetcher.db_manager.bulk_load_stock_info(
                all_stock_ids
            )
            print(f"   ✓ 已載入 {len(bulk_daily_data)} 支股票的每日資料")
            print(f"   ✓ 已載入 {len(bulk_info_data)} 支股票的基本資訊")

        # 步驟4.5: 處理籌碼集中度數據（優先從 DB 載入）
        concentration_data = {}
        ENABLE_CONCENTRATION = (
            os.getenv("ENABLE_CONCENTRATION", "true").lower() == "true"
        )

        if ENABLE_CONCENTRATION:
            print(f"\n📊 開始處理籌碼集中度數據...")
            concentration_start = time.time()

            # 過濾掉指數股票（^開頭）和特殊代碼
            stock_ids_for_concentration = [
                sid for sid in all_stock_ids if not sid.startswith("^")
            ]
            print(
                f"   過濾後剩餘: {len(stock_ids_for_concentration)} 支股票（排除 {len(all_stock_ids) - len(stock_ids_for_concentration)} 支指數）"
            )

            # Step 1: 批次從 DB 載入股票的集中度資料
            if hasattr(self.fetcher, "db_manager"):
                print(
                    f"   載入 {len(stock_ids_for_concentration)} 支股票的集中度資料..."
                )
                concentration_data = (
                    await self.fetcher.db_manager.bulk_load_concentration_data(
                        stock_ids_for_concentration, weeks=10
                    )
                )
                print(f"   ✓ 已從 DB 載入 {len(concentration_data)} 支股票的集中度資料")

            # Step 2: 檢查哪些股票需要從 API 更新（DB 沒有或資料過舊）
            stocks_need_api = []
            for sid in stock_ids_for_concentration:
                if sid not in concentration_data:
                    # DB 沒有資料，需要從 API 抓取
                    stocks_need_api.append(sid)
                else:
                    # 檢查資料是否過舊（超過 7 天）
                    df = concentration_data[sid]
                    if (
                        df.empty
                        or (datetime.datetime.now() - df.iloc[0]["date"]).days > 7
                    ):
                        stocks_need_api.append(sid)

            # Step 3: 只對需要更新的股票調用 API
            if stocks_need_api:
                print(f"   {len(stocks_need_api)} 支股票需要從 API 更新...")

                MAX_CONCENTRATION_WORKERS = int(
                    os.getenv("MAX_CONCENTRATION_WORKERS", "10")
                )
                concentration_semaphore = asyncio.Semaphore(MAX_CONCENTRATION_WORKERS)

                async def fetch_concentration(sid):
                    async with concentration_semaphore:
                        try:
                            # save_to_db=True 會自動儲存到資料庫
                            return (
                                sid,
                                await self.fetcher.fetch_concentration_data(
                                    sid, weeks=10, save_to_db=True
                                ),
                            )
                        except Exception as e:
                            print(f"   ❌ {sid} 籌碼集中度抓取失敗: {e}")
                            return sid, pd.DataFrame()

                tasks = [fetch_concentration(sid) for sid in stocks_need_api]

                completed_count = 0
                api_success_count = 0
                for task in asyncio.as_completed(tasks):
                    sid, conc_df = await task
                    completed_count += 1

                    if not conc_df.empty:
                        concentration_data[sid] = conc_df  # 更新字典
                        api_success_count += 1

                    # 進度顯示（每 50 支）
                    if completed_count % 50 == 0:
                        elapsed = time.time() - concentration_start
                        speed = completed_count / elapsed if elapsed > 0 else 0
                        remaining = (
                            (len(stocks_need_api) - completed_count) / speed
                            if speed > 0
                            else 0
                        )
                        print(
                            f"   更新進度: [{completed_count}/{len(stocks_need_api)}] - "
                            f"速度: {speed:.2f} 支/秒 - 預估剩餘: {int(remaining)}秒"
                        )

                print(
                    f"   ✓ 已從 API 更新 {api_success_count}/{len(stocks_need_api)} 支股票"
                )
            else:
                print(f"   ✓ 所有股票資料皆從 DB 載入，無需 API 更新")

            elapsed = time.time() - concentration_start
            print(f"   籌碼集中度處理完成: {len(concentration_data)} 支股票")
            print(f"   耗時: {int(elapsed/60)}分{int(elapsed%60)}秒")

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

            # 統一按股票代碼排序
            skill_list_excel = skill_list_excel.sort_values("id")
            info_list_excel = info_list_excel.sort_values("id")
            extreme_list_excel = extreme_list_excel.sort_values("id")

            # 寫入 Excel（使用 openpyxl 引擎以支援條件格式）
            excel_filename = date.today().strftime("%Y%m%d") + ".xlsx"
            with pd.ExcelWriter(excel_filename, engine="openpyxl") as writer:
                skill_list_excel.rename(columns=INDEX_COLUMN).to_excel(
                    writer, sheet_name="技術籌碼", index=False
                )
                info_list_excel.rename(columns=INDEX_COLUMN).to_excel(
                    writer, sheet_name="基本面", index=False
                )
                extreme_list_excel.rename(columns=INDEX_COLUMN).to_excel(
                    writer, sheet_name="極端漲跌", index=False
                )

                # 新增：籌碼集中度相關 sheets
                if ENABLE_CONCENTRATION and len(concentration_data) > 0:
                    # Sheet 1: 每週籌碼變化（差值顯示）
                    concentration_excel = self.prepare_concentration_excel(
                        concentration_data
                    )
                    concentration_excel.to_excel(
                        writer, sheet_name="每週籌碼變化", index=False
                    )
                    print(f"   ✓ 籌碼變化 sheet: {len(concentration_excel)} 支股票")

                    # Sheet 2: 籌碼訊號（強化版）
                    signal_excel_basic = self.generate_concentration_signals(
                        concentration_data
                    )

                    # 計算訊號績效
                    print("   📊 計算訊號回測績效...")
                    performance_df = self.calculate_signal_performance(
                        concentration_data
                    )

                    # 強化訊號 sheet（加入預期報酬、勝率等）
                    signal_excel = self.enhance_signal_sheet(
                        signal_excel_basic, performance_df
                    )
                    signal_excel.to_excel(writer, sheet_name="籌碼訊號", index=False)
                    print(f"   ✓ 籌碼訊號 sheet: {len(signal_excel)} 支股票")

            # 套用條件格式
            if ENABLE_CONCENTRATION and len(concentration_data) > 0:
                from openpyxl import load_workbook

                wb = load_workbook(excel_filename)
                wb = self.apply_concentration_formatting(
                    wb, "每週籌碼變化", change_threshold=1.0
                )
                wb = self.apply_concentration_formatting(
                    wb, "籌碼訊號", change_threshold=1.0
                )
                wb.save(excel_filename)

            print(f"   ✓ Excel 已儲存: {excel_filename}")
        else:
            print("   ⚠️  沒有可用的股票資料")

        endTime = time.time()
        total_minutes = int((endTime - startTime) / 60)
        total_seconds = int((endTime - startTime) % 60)
        print(f"\n⏱️  總執行時間: {total_minutes}分{total_seconds}秒")

    def prepare_concentration_excel(self, concentration_data):
        """將籌碼集中度數據轉換為 Excel 格式（顯示週變化量）"""
        rows = []

        for stock_id, df in concentration_data.items():
            if df.empty or len(df) < 2:
                continue  # 至少需要 2 週才能計算差值

            row = {"股票代碼": stock_id}

            # 添加股票名稱
            stock_name = stock_id
            for stock_info in self.list:
                if stock_info["id"] == stock_id:
                    stock_name = stock_info.get("name", stock_id)
                    break
            row["股票名稱"] = stock_name

            # 添加收盤價和日期資訊
            row["收盤價"] = round(df.iloc[0]["close"], 2)
            row["最新日期"] = df.iloc[0]["date"].strftime("%Y-%m-%d")
            row["資料週數"] = len(df)

            # 計算週變化量（使用統一的 W1→W2 格式）
            for i in range(len(df) - 1):
                current_week = df.iloc[i]
                next_week = df.iloc[i + 1]

                # 計算差值（正值 = 集中度上升）
                delta_400 = current_week["moreThan400"] - next_week["moreThan400"]
                delta_1000 = current_week["moreThan1000"] - next_week["moreThan1000"]
                delta_20 = current_week["lessThan20"] - next_week["lessThan20"]

                # 使用統一的欄位名稱（W1→W2, W2→W3...）
                row[f"W{i+1}→W{i+2}_大戶變化"] = round(delta_400, 2)
                row[f"W{i+1}→W{i+2}_超大戶變化"] = round(delta_1000, 2)
                row[f"W{i+1}→W{i+2}_散戶變化"] = round(delta_20, 2)

            # 總變化（基於實際週數）
            row["大戶總變化"] = round(
                df.iloc[0]["moreThan400"] - df.iloc[-1]["moreThan400"], 2
            )
            row["散戶總變化"] = round(
                df.iloc[0]["lessThan20"] - df.iloc[-1]["lessThan20"], 2
            )

            rows.append(row)

        result_df = pd.DataFrame(rows)

        # 依股票代碼排序
        if "股票代碼" in result_df.columns:
            result_df = result_df.sort_values("股票代碼", ascending=True)

        return result_df

    def generate_concentration_signals(self, concentration_data):
        """生成籌碼集中度訊號分析"""
        signal_rows = []

        for stock_id, df in concentration_data.items():
            if df.empty or len(df) < 3:
                continue  # 至少需要 3 週才能偵測趨勢

            # 添加股票名稱
            stock_name = stock_id
            for stock_info in self.list:
                if stock_info["id"] == stock_id:
                    stock_name = stock_info.get("name", stock_id)
                    break

            signals = []
            score = 50  # 基礎分數

            # 準備數據（降序排列：df.iloc[0] = 最新週）
            values_400 = df["moreThan400"].tolist()
            values_1000 = df["moreThan1000"].tolist()
            values_20 = df["lessThan20"].tolist()

            # ===== 買進訊號 =====

            # 1. 完美結構 (+20分)
            if len(df) >= 2:
                current = df.iloc[0]
                previous = df.iloc[1]
                if (
                    current["moreThan400"] > previous["moreThan400"]
                    and current["moreThan1000"] > previous["moreThan1000"]
                    and current["lessThan20"] < previous["lessThan20"]
                ):
                    signals.append("🟢完美結構")
                    score += 20

            # 2. 超大戶連買2週 (+15分)
            if len(values_1000) >= 2:
                if values_1000[0] > values_1000[1]:
                    if len(values_1000) < 3 or values_1000[1] > values_1000[2]:
                        signals.append("🟢超大戶連買2週")
                        score += 15

            # 3. 大戶連買3週 (+12分)
            if len(values_400) >= 3:
                if values_400[0] > values_400[1] and values_400[1] > values_400[2]:
                    signals.append("🟢大戶連買3週")
                    score += 12

            # 4. 大戶急買 (+10分)
            if len(df) >= 2:
                delta = df.iloc[0]["moreThan400"] - df.iloc[1]["moreThan400"]
                if delta > 1.0:
                    signals.append(f"🟢大戶急買(+{delta:.1f}%)")
                    score += 10

            # 5. 加速集中 (+10分)
            if len(df) >= 10:
                first_5_avg = (
                    sum(
                        df.iloc[i]["moreThan400"] - df.iloc[i + 1]["moreThan400"]
                        for i in range(4)
                    )
                    / 4
                )
                last_5_avg = (
                    sum(
                        df.iloc[i]["moreThan400"] - df.iloc[i + 1]["moreThan400"]
                        for i in range(5, 9)
                    )
                    / 4
                )
                if first_5_avg > last_5_avg and first_5_avg > 0:
                    signals.append("🟢加速集中")
                    score += 10

            # 6. 10週持續買 (+8分)
            if len(df) >= 10:
                delta_10w = df.iloc[0]["moreThan400"] - df.iloc[-1]["moreThan400"]
                if delta_10w > 3.0:
                    signals.append(f"🟢10週持續買(+{delta_10w:.1f}%)")
                    score += 8

            # 7. 散戶連賣3週 (+8分)
            if len(values_20) >= 3:
                if values_20[0] < values_20[1] and values_20[1] < values_20[2]:
                    signals.append("🟢散戶連賣3週")
                    score += 8

            # 8. 散戶恐慌 (+6分)
            if len(df) >= 2:
                delta = df.iloc[0]["lessThan20"] - df.iloc[1]["lessThan20"]
                if delta < -1.0:
                    signals.append(f"🟢散戶恐慌({delta:.1f}%)")
                    score += 6

            # ===== 風險警示 =====

            # 9. 出貨訊號 (-15分)
            if len(df) >= 2:
                current = df.iloc[0]
                previous = df.iloc[1]
                if (
                    current["moreThan400"] < previous["moreThan400"]
                    and current["lessThan20"] > previous["lessThan20"]
                ):
                    signals.append("🔴出貨訊號")
                    score -= 15

            # 10. 散戶狂熱 (-10分)
            if len(values_20) >= 3:
                if values_20[0] > values_20[1] and values_20[1] > values_20[2]:
                    signals.append("🔴散戶狂熱")
                    score -= 10

            # 正規化分數到 0-100
            normalized_score = max(0, min(100, score))

            # 只保留有訊號的股票
            if len(signals) > 0:
                signal_rows.append(
                    {
                        "股票代碼": stock_id,
                        "股票名稱": stock_name,
                        "收盤價": round(df.iloc[0]["close"], 2),
                        "訊號強度": normalized_score,
                        "訊號數量": len(signals),
                        "主要訊號": ", ".join(signals[:3]),  # 只顯示前 3 個
                        "大戶10週變化": (
                            round(
                                df.iloc[0]["moreThan400"] - df.iloc[-1]["moreThan400"],
                                2,
                            )
                            if len(df) >= 10
                            else None
                        ),
                        "散戶10週變化": (
                            round(
                                df.iloc[0]["lessThan20"] - df.iloc[-1]["lessThan20"], 2
                            )
                            if len(df) >= 10
                            else None
                        ),
                        "最新大戶占比": round(df.iloc[0]["moreThan400"], 2),
                        "最新散戶占比": round(df.iloc[0]["lessThan20"], 2),
                    }
                )

        result_df = pd.DataFrame(signal_rows)

        # 依股票代碼排序
        if len(result_df) > 0:
            result_df = result_df.sort_values("股票代碼", ascending=True)

        return result_df

    def calculate_signal_performance(self, concentration_data):
        """計算每個訊號的歷史回測績效"""
        import numpy as np

        signal_performance = {
            "完美結構": [],
            "超大戶連買2週": [],
            "大戶連買3週": [],
            "大戶急買": [],
            "加速集中": [],
            "10週持續買": [],
            "散戶連賣3週": [],
            "散戶恐慌": [],
            "出貨訊號": [],
            "散戶狂熱": [],
        }

        # 遍歷所有股票，檢測訊號並計算後續報酬
        for stock_id, df in concentration_data.items():
            if len(df) < 4:
                continue

            # 檢查價格資料有效性
            for i in range(len(df) - 3):
                if any(
                    pd.isna(df.iloc[i + j]["close"]) or df.iloc[i + j]["close"] == 0
                    for j in range(4)
                ):
                    continue

                current = df.iloc[i]
                prev1 = df.iloc[i + 1]
                prev2 = df.iloc[i + 2]

                # 計算觸發後 3 週的報酬
                future_return = (
                    (df.iloc[i]["close"] - df.iloc[i + 3]["close"])
                    / df.iloc[i + 3]["close"]
                    * 100
                )
                if abs(future_return) > 50:  # 過濾極端值
                    continue

                # 1. 完美結構
                if (
                    current["moreThan400"] > prev1["moreThan400"]
                    and current["moreThan1000"] > prev1["moreThan1000"]
                    and current["lessThan20"] < prev1["lessThan20"]
                ):
                    signal_performance["完美結構"].append(future_return)

                # 2. 超大戶連買2週
                if (
                    current["moreThan1000"] > prev1["moreThan1000"]
                    and prev1["moreThan1000"] > prev2["moreThan1000"]
                ):
                    signal_performance["超大戶連買2週"].append(future_return)

                # 3. 大戶連買3週
                if (
                    current["moreThan400"] > prev1["moreThan400"]
                    and prev1["moreThan400"] > prev2["moreThan400"]
                ):
                    signal_performance["大戶連買3週"].append(future_return)

                # 4. 大戶急買
                delta_400 = current["moreThan400"] - prev1["moreThan400"]
                if delta_400 > 1.0:
                    signal_performance["大戶急買"].append(future_return)

                # 5. 加速集中（需要 10 週資料）
                if len(df) >= 10 and i <= len(df) - 10:
                    first_5_avg = (
                        sum(
                            df.iloc[i + j]["moreThan400"]
                            - df.iloc[i + j + 1]["moreThan400"]
                            for j in range(4)
                        )
                        / 4
                    )
                    last_5_avg = (
                        sum(
                            df.iloc[i + j]["moreThan400"]
                            - df.iloc[i + j + 1]["moreThan400"]
                            for j in range(5, 9)
                        )
                        / 4
                    )
                    if first_5_avg > last_5_avg and first_5_avg > 0:
                        signal_performance["加速集中"].append(future_return)

                # 6. 10週持續買
                if len(df) >= 10 and i == 0:
                    delta_10w = df.iloc[0]["moreThan400"] - df.iloc[9]["moreThan400"]
                    if delta_10w > 3.0:
                        signal_performance["10週持續買"].append(future_return)

                # 7. 散戶連賣3週
                if (
                    current["lessThan20"] < prev1["lessThan20"]
                    and prev1["lessThan20"] < prev2["lessThan20"]
                ):
                    signal_performance["散戶連賣3週"].append(future_return)

                # 8. 散戶恐慌
                delta_20 = current["lessThan20"] - prev1["lessThan20"]
                if delta_20 < -1.0:
                    signal_performance["散戶恐慌"].append(future_return)

                # 9. 出貨訊號
                if (
                    current["moreThan400"] < prev1["moreThan400"]
                    and current["lessThan20"] > prev1["lessThan20"]
                ):
                    signal_performance["出貨訊號"].append(future_return)

                # 10. 散戶狂熱
                if (
                    current["lessThan20"] > prev1["lessThan20"]
                    and prev1["lessThan20"] > prev2["lessThan20"]
                ):
                    signal_performance["散戶狂熱"].append(future_return)

        # 計算統計數據
        results = []
        for signal_name, returns in signal_performance.items():
            if returns:
                results.append(
                    {
                        "訊號名稱": signal_name,
                        "觸發次數": len(returns),
                        "平均報酬(%)": round(np.mean(returns), 2),
                        "中位數報酬(%)": round(np.median(returns), 2),
                        "勝率(%)": round(
                            len([r for r in returns if r > 0]) / len(returns) * 100, 1
                        ),
                        "最佳報酬(%)": round(max(returns), 2),
                        "最差報酬(%)": round(min(returns), 2),
                        "標準差(%)": round(np.std(returns), 2),
                    }
                )

        return pd.DataFrame(results).sort_values("平均報酬(%)", ascending=False)

    def enhance_signal_sheet(self, signal_df, performance_df):
        """強化籌碼訊號 sheet，加入歷史績效資訊"""
        import numpy as np

        if signal_df.empty or performance_df.empty:
            return signal_df

        enhanced_df = signal_df.copy()

        # 為每支股票的訊號匹配歷史績效
        expected_returns = []
        win_rates = []
        risk_levels = []
        quality_scores = []

        for idx, row in enhanced_df.iterrows():
            # 解析主要訊號
            main_signals = str(row["主要訊號"]).split(", ")

            # 計算該組訊號的預期報酬和勝率
            signal_returns = []
            signal_win_rates = []

            for signal in main_signals:
                # 移除 emoji 並匹配績效資料
                clean_signal = signal.replace("🟢", "").replace("🔴", "").split("(")[0]

                match = performance_df[performance_df["訊號名稱"] == clean_signal]
                if not match.empty:
                    signal_returns.append(match.iloc[0]["平均報酬(%)"])
                    signal_win_rates.append(match.iloc[0]["勝率(%)"])

            # 計算加權平均
            expected_return = (
                round(np.mean(signal_returns), 2) if signal_returns else None
            )
            win_rate = round(np.mean(signal_win_rates), 1) if signal_win_rates else None

            expected_returns.append(expected_return)
            win_rates.append(win_rate)

            # 風險等級
            if row["訊號強度"] >= 80:
                risk_level = "低"
            elif row["訊號強度"] >= 60:
                risk_level = "中"
            else:
                risk_level = "高"
            risk_levels.append(risk_level)

            # 訊號品質評分（綜合考慮強度和勝率）
            quality = row["訊號強度"] * 0.6 + (win_rate or 50) * 0.4
            quality_scores.append(round(quality, 1))

        # 新增欄位（注意：因為新增了「收盤價」欄位，insert 位置需調整）
        enhanced_df.insert(6, "預期報酬(%)", expected_returns)
        enhanced_df.insert(7, "歷史勝率(%)", win_rates)
        enhanced_df.insert(8, "風險等級", risk_levels)
        enhanced_df.insert(9, "訊號品質", quality_scores)

        return enhanced_df

    def apply_concentration_formatting(
        self, workbook, sheet_name, change_threshold=0.2
    ):
        """為籌碼集中度 sheet 套用條件格式（漸層顏色）"""
        from openpyxl.styles import Font, PatternFill

        ws = workbook[sheet_name]

        # 定義漸層顏色（從淺到深）
        # 淺粉紅 → 粉紅 → 紅色
        very_light_pink = PatternFill(
            start_color="ffe6e6", end_color="ffe6e6", fill_type="solid"
        )
        light_pink = PatternFill(
            start_color="ffcccc", end_color="ffcccc", fill_type="solid"
        )
        pink = PatternFill(start_color="ffb3b3", end_color="ffb3b3", fill_type="solid")
        red = PatternFill(start_color="ff9999", end_color="ff9999", fill_type="solid")
        dark_red = PatternFill(
            start_color="ff8080", end_color="ff8080", fill_type="solid"
        )

        # 訊號強度顏色
        green_fill = PatternFill(
            start_color="d4edda", end_color="d4edda", fill_type="solid"
        )
        yellow_fill = PatternFill(
            start_color="fff3cd", end_color="fff3cd", fill_type="solid"
        )
        light_red_fill = PatternFill(
            start_color="f8d7da", end_color="f8d7da", fill_type="solid"
        )

        # 找到特殊欄位的索引
        header_row = [cell.value for cell in ws[1]]
        signal_strength_col = None
        close_price_col = None
        change_cols = []  # 籌碼變化欄位

        if "訊號強度" in header_row:
            signal_strength_col = header_row.index("訊號強度") + 1
        if "收盤價" in header_row:
            close_price_col = header_row.index("收盤價") + 1

        # 找出所有「變化」欄位
        for idx, col_name in enumerate(header_row):
            if col_name and ("變化" in str(col_name) or "→" in str(col_name)):
                change_cols.append(idx + 1)

        # 遍歷所有數據行
        for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
            # 處理訊號強度欄位（籌碼訊號 sheet）
            if signal_strength_col and row[signal_strength_col - 1].value:
                strength = row[signal_strength_col - 1].value
                if strength >= 80:
                    row[signal_strength_col - 1].fill = green_fill
                elif strength >= 60:
                    row[signal_strength_col - 1].fill = yellow_fill
                elif strength < 40:
                    row[signal_strength_col - 1].fill = light_red_fill

            # 處理籌碼變化欄位（漸層顏色 + 連續上升檢測）
            if change_cols:
                # 收集該行所有變化值，檢測連續上升
                change_values = []
                for col_idx in change_cols:
                    cell_val = row[col_idx - 1].value
                    if isinstance(cell_val, (int, float)):
                        change_values.append((col_idx, cell_val))

                # 為每個變化欄位套用顏色
                for i, (col_idx, value) in enumerate(change_values):
                    # 跳過收盤價欄位
                    if col_idx == close_price_col:
                        continue

                    # 只標記正值（上升）
                    if value >= change_threshold:
                        # 檢測連續上升（往後看）
                        consecutive_count = 1
                        for j in range(i + 1, min(i + 3, len(change_values))):
                            if change_values[j][1] >= change_threshold:
                                consecutive_count += 1
                            else:
                                break

                        # 根據數值大小和連續程度選擇顏色
                        if consecutive_count >= 3:
                            # 連續3週以上：深紅色
                            row[col_idx - 1].fill = dark_red
                        elif consecutive_count == 2:
                            # 連續2週：紅色
                            row[col_idx - 1].fill = red
                        elif value >= 1.0:
                            # 單次大漲 >1%：粉紅
                            row[col_idx - 1].fill = pink
                        elif value >= 0.5:
                            # 單次中漲 0.5-1%：淡粉紅
                            row[col_idx - 1].fill = light_pink
                        else:
                            # 單次小漲 0.2-0.5%：很淡粉紅
                            row[col_idx - 1].fill = very_light_pink

        return workbook

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
