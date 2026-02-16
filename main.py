import argparse
import asyncio
import os
from datetime import date, datetime

from dotenv import load_dotenv

from twstock import All
from twstock.convertible_bond_updater import (
    append_cb_sheet_to_excel,
    update_convertible_bonds,
)
from twstock.database import DatabaseManager
from twstock.signal_updater import SignalUpdater

# 載入環境變數
load_dotenv()

# 顯示當前使用的資料庫
database_url = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./twstock.db")
db_type = "PostgreSQL" if "postgresql" in database_url else "SQLite"
print(f"🗄️  資料庫: {db_type}")

all = All()


async def main():
    parser = argparse.ArgumentParser(description="twstock 股票分析")
    parser.add_argument("--fast", action="store_true",
                        help="快速模式：僅計算短線指標，跳過 MA60")
    args = parser.parse_args()

    fast_mode = args.fast
    if fast_mode:
        print("⚡ 快速模式：跳過 MA60 / 四線乖離 / 季線訊號")

    # 步驟 1: 資料獲取 + 分析
    await all.get_all_stock_list()
    await all.get_all_stock_parallel(fast_mode=fast_mode)

    # 步驟 2: 更新技術訊號
    print("\n📊 更新技術訊號...")
    db_manager = DatabaseManager()
    updater = SignalUpdater(db_manager)

    try:
        result = await updater.update_signals(force_update=False)
        print(f"✅ 技術訊號: 更新 {result['updated_count']} 支，跳過 {result['skipped_count']} 支")
        if result['error_count'] > 0:
            print(f"⚠️  錯誤: {result['error_count']} 支")
    except Exception as e:
        print(f"❌ 技術訊號更新失敗: {e}")

    # 步驟 3: 生成股票 Excel（5 sheets）
    excel_path = all.generate_excel()

    # 步驟 4: 可轉債套利分析 + 追加 CB sheet
    try:
        signal_records, cb_score_map = await update_convertible_bonds(db_manager)

        if signal_records and excel_path and os.path.exists(excel_path):
            bonds = await db_manager.get_all_convertible_bonds()
            bond_map = {b["bond_id"]: b for b in bonds}
            append_cb_sheet_to_excel(excel_path, signal_records, bond_map)
    except Exception as e:
        print(f"❌ 可轉債分析失敗（不影響主流程）: {e}")

    # 步驟 5: 自動清理舊資料（僅 production）
    try:
        environment = os.getenv("ENVIRONMENT", "development")
        retention_days = int(os.getenv("DATA_RETENTION_DAYS", "250"))

        if environment == "production" and retention_days > 0:
            print("\n🗑️  開始自動清理舊資料...")
            cleanup_result = await db_manager.auto_cleanup_old_data(
                retention_days=retention_days, vacuum=True
            )
            if cleanup_result.get("status") == "completed":
                total_deleted = sum(
                    v
                    for v in cleanup_result.get("deleted_counts", {}).values()
                    if isinstance(v, int)
                )
                print(f"✅ 自動清理完成，刪除 {total_deleted:,} 行資料")
            else:
                print(f"⏭️  自動清理跳過：{cleanup_result.get('reason', 'unknown')}")
    except Exception as e:
        print(f"❌ 自動清理失敗（不影響主流程）: {e}")

    # 清理資源
    if hasattr(db_manager, 'async_engine'):
        await db_manager.async_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
