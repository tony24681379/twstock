"""可轉債更新器 - 獨立於股票爬蟲的 CB 更新邏輯

從 all.py 搬出，在 main.py 中 SignalUpdater 之後執行，
確保讀取到最新的 stock_technical_signals。
"""

import json
import logging
from datetime import date

from openpyxl import load_workbook

logger = logging.getLogger(__name__)


async def update_convertible_bonds(db_manager):
    """批次更新所有可轉債資料：清單同步 → 每日交易資料 → 訊號計算

    Args:
        db_manager: DatabaseManager 實例

    Returns:
        tuple: (signal_records, cb_score_map)
    """
    from api.services.convertible_signal_service import ConvertibleSignalService
    from twstock.convertible_fetcher import ConvertibleBondFetcher

    cb_fetcher = ConvertibleBondFetcher(db_manager=db_manager)
    print("\n" + "=" * 60)
    print("📊 可轉債套利分析")
    print("=" * 60)

    # Step 1: 取得 CB 清單
    # ⚠️ TPEX OpenAPI 只有發行時轉換價 (Conversion/ExchangePriceAtIssuance)，
    #    不是最新調整後轉換價。CB 經除權息調整後的轉換價需要 MOPS 等額外來源。
    #    目前影響：轉換價值/溢價率計算可能偏差（分母偏大）
    bonds = await cb_fetcher.get_convertible_bond_list()
    if not bonds:
        print("⚠️  無可轉債資料")
        return [], {}

    # 儲存 CB 基本資訊
    await db_manager.save_convertible_bonds(bonds)

    # Step 2: 取得標的股收盤價映射 (用於計算轉換價值)
    underlying_ids = list(
        {b["underlying_stock_id"] for b in bonds if b.get("underlying_stock_id")}
    )
    stock_daily_map = {}
    if underlying_ids:
        from sqlalchemy import text

        async with db_manager.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT stock_id, date, close
                    FROM stock_daily
                    WHERE stock_id = ANY(:ids)
                      AND date >= NOW() - INTERVAL '30 days'
                    ORDER BY stock_id, date
                """),
                {"ids": underlying_ids},
            )
            for row in result.fetchall():
                r = row._mapping
                sid = r["stock_id"]
                d = (
                    r["date"].strftime("%Y-%m-%d")
                    if hasattr(r["date"], "strftime")
                    else str(r["date"])[:10]
                )
                if sid not in stock_daily_map:
                    stock_daily_map[sid] = {}
                stock_daily_map[sid][d] = float(r["close"])

    # Step 2b: 用 TWSE/TPEX Open Data 補齊缺失的標的股收盤價
    missing_ids = [sid for sid in underlying_ids if sid not in stock_daily_map]
    if missing_ids:
        print(f"📡 {len(missing_ids)} 檔標的股不在 stock_daily，從交易所取得收盤價...")
        all_close = await cb_fetcher.fetch_all_stock_close_prices()
        supplemented = 0
        for sid in missing_ids:
            if sid in all_close:
                # 只有最新一天的資料，用 "latest" 作為 key
                stock_daily_map[sid] = {"latest": all_close[sid]}
                supplemented += 1
        if supplemented:
            print(f"✅ 補齊 {supplemented}/{len(missing_ids)} 檔標的股收盤價")
        still_missing = len(missing_ids) - supplemented
        if still_missing:
            print(f"⚠️  仍有 {still_missing} 檔標的股無收盤價")

    # Step 3: 取得每日交易資料
    print(f"📡 更新 {len(bonds)} 檔可轉債每日資料...")
    all_daily = await cb_fetcher.fetch_all_cb_daily(bonds, stock_daily_map)
    if all_daily:
        await db_manager.save_convertible_bond_daily(all_daily)
        print(f"✅ 儲存 {len(all_daily)} 筆 CB 每日資料")

    # Step 4: 取得標的股 overall_strength (用於連動訊號)
    strength_map = {}
    try:
        from sqlalchemy import text as _text

        async with db_manager.get_session() as session:
            result = await session.execute(
                _text("SELECT stock_id, normalized_score FROM stock_technical_signals")
            )
            for row in result.fetchall():
                r = row._mapping
                strength_map[r["stock_id"]] = int(r["normalized_score"])
    except Exception:
        pass

    # Step 5: 計算套利訊號
    print("🔍 計算可轉債套利訊號...")
    signal_records = []
    # 取得最新每日資料（每支 CB 最新一筆）
    latest_daily = {}
    for rec in all_daily:
        bid = rec["bond_id"]
        if bid not in latest_daily or rec["date"] > latest_daily[bid]["date"]:
            latest_daily[bid] = rec

    bond_map = {b["bond_id"]: b for b in bonds}
    for bond_id, daily in latest_daily.items():
        bond_info = bond_map.get(bond_id, {})
        underlying_id = bond_info.get("underlying_stock_id", "")
        underlying_strength = strength_map.get(underlying_id)

        result = ConvertibleSignalService.calculate_signals(
            cb_close=daily.get("close") or 0,
            conversion_price=bond_info.get("conversion_price") or 0,
            underlying_close=daily.get("underlying_close") or 0,
            premium_rate=daily.get("premium_rate"),
            conversion_value=daily.get("conversion_value"),
            maturity_date=bond_info.get("maturity_date"),
            put_date=bond_info.get("put_date"),
            put_price=bond_info.get("put_price"),
            underlying_strength=underlying_strength,
        )

        rec = ConvertibleSignalService.build_signal_record(
            bond_id=bond_id,
            underlying_stock_id=underlying_id,
            result=result,
            premium_rate=daily.get("premium_rate"),
            conversion_value=daily.get("conversion_value"),
        )
        signal_records.append(rec)

    # 儲存訊號
    if signal_records:
        await db_manager.save_convertible_bond_signals(signal_records)
        print(f"✅ 計算 {len(signal_records)} 檔 CB 訊號")

    # Step 6: 建立 stock→CB score 映射
    cb_score_map = ConvertibleSignalService.build_stock_cb_score_map(signal_records)
    print(f"📈 {len(cb_score_map)} 支標的股有 CB 套利分數")

    # 顯示前 10 名
    top_cbs = sorted(
        signal_records, key=lambda x: x["normalized_score"], reverse=True
    )[:10]
    if top_cbs:
        print("\n🏆 TOP 10 套利機會:")
        for i, rec in enumerate(top_cbs, 1):
            bond = bond_map.get(rec["bond_id"], {})
            print(
                f"  {i:2d}. {rec['bond_id']} {bond.get('name', ''):<10s} "
                f"分數:{rec['normalized_score']:3d} "
                f"溢價率:{rec.get('premium_rate', 0) or 0:6.1f}% "
                f"({rec['risk_level']})"
            )

    return signal_records, cb_score_map


def _prepare_cb_dataframe(signal_records, bond_map):
    """將可轉債訊號資料轉換為 Excel DataFrame"""
    import pandas as pd

    rows = []
    for rec in sorted(
        signal_records, key=lambda x: x["normalized_score"], reverse=True
    ):
        bond = bond_map.get(rec["bond_id"], {})
        signals_list = (
            json.loads(rec["signals_json"])
            if isinstance(rec["signals_json"], str)
            else rec["signals_json"]
        )
        main_signals = (
            ", ".join(s["name"] for s in signals_list[:3]) if signals_list else ""
        )

        rows.append(
            {
                "CB代碼": rec["bond_id"],
                "CB名稱": bond.get("name", ""),
                "標的股": rec.get("underlying_stock_id", ""),
                "CB收盤價": (
                    rec.get("conversion_value")
                    and round(float(rec.get("premium_rate", 0) or 0), 1)
                ),
                "轉換價": bond.get("conversion_price"),
                "溢價率(%)": rec.get("premium_rate"),
                "套利空間(%)": (
                    round(-float(rec.get("premium_rate", 0) or 0) - 0.6, 1)
                    if rec.get("premium_rate") is not None
                    else None
                ),
                "套利評分": rec["normalized_score"],
                "訊號數": rec["signal_count"],
                "風險等級": rec["risk_level"],
                "主要訊號": main_signals,
            }
        )

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def append_cb_sheet_to_excel(excel_path, signal_records, bond_map):
    """將可轉債套利 sheet 追加到已存在的 Excel 檔

    Args:
        excel_path: 已存在的 Excel 檔路徑
        signal_records: CB 訊號記錄
        bond_map: {bond_id: bond_info} 映射
    """
    import pandas as pd

    if not signal_records:
        return

    cb_df = _prepare_cb_dataframe(signal_records, bond_map)
    if cb_df.empty:
        return

    wb = load_workbook(excel_path)

    # 插入到第一個位置（最高優先級）
    ws = wb.create_sheet("可轉債套利", 0)

    # 寫入 header
    for col_idx, col_name in enumerate(cb_df.columns, 1):
        ws.cell(row=1, column=col_idx, value=col_name)

    # 寫入資料
    for row_idx, row in enumerate(cb_df.itertuples(index=False), 2):
        for col_idx, value in enumerate(row, 1):
            ws.cell(row=row_idx, column=col_idx, value=value)

    wb.save(excel_path)
    print(f"   ✓ 可轉債套利 sheet: {len(cb_df)} 檔")
