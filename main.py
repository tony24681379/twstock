import argparse
import asyncio
import os
from datetime import datetime

from dotenv import load_dotenv

from twstock import All
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

    # 步驟 1: 更新股票數據
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
    finally:
        if hasattr(db_manager, 'async_engine'):
            await db_manager.async_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
