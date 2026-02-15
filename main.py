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
    # 步驟 1: 更新股票數據
    await all.get_all_stock_list()
    await all.get_all_stock_parallel()

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
