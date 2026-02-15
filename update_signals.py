#!/usr/bin/env python3
"""
技術訊號更新腳本

用於更新 stock_technical_signals 表，計算所有股票的技術指標訊號。

使用方式：
    poetry run python update_signals.py              # 增量更新
    poetry run python update_signals.py --force      # 強制全部更新
    poetry run python update_signals.py --stocks 2330 2454  # 更新特定股票
"""

import asyncio
import argparse
import sys
from datetime import datetime

from twstock.database import DatabaseManager
from twstock.signal_updater import SignalUpdater


async def main(stock_ids=None, force=False):
    """執行訊號更新"""

    print("=" * 60)
    print("技術訊號更新腳本")
    print("=" * 60)
    print(f"開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"更新模式: {'強制全部更新' if force else '增量更新'}")
    if stock_ids:
        print(f"指定股票: {', '.join(stock_ids)}")
    print()

    # 初始化資料庫連線
    db_manager = DatabaseManager()

    try:
        # 建立更新器
        updater = SignalUpdater(db_manager)

        # 執行更新
        result = await updater.update_signals(
            stock_ids=stock_ids,
            force_update=force
        )

        # 顯示結果
        print()
        print("=" * 60)
        print("更新完成")
        print("=" * 60)
        print(f"更新數量: {result['updated_count']}")
        print(f"跳過數量: {result['skipped_count']}")
        print(f"錯誤數量: {result['error_count']}")
        print(f"執行時間: {result['elapsed_time']:.2f} 秒")
        print(f"完成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 如果有錯誤，返回非零退出碼
        if result['error_count'] > 0:
            print()
            print("⚠️  部分股票更新失敗，請查看日誌")
            return 1

        return 0

    except Exception as e:
        print()
        print(f"❌ 更新失敗: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # 關閉資料庫連線
        if hasattr(db_manager, 'close'):
            await db_manager.close()
        elif hasattr(db_manager, 'async_engine'):
            await db_manager.async_engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="更新股票技術訊號預計算表",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例：
  poetry run python update_signals.py                    # 增量更新
  poetry run python update_signals.py --force            # 強制全部更新
  poetry run python update_signals.py --stocks 2330 2454 # 更新特定股票
        """
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="強制更新所有股票（忽略增量檢查）"
    )

    parser.add_argument(
        "--stocks",
        nargs="+",
        help="指定要更新的股票代號"
    )

    args = parser.parse_args()

    # 執行更新
    exit_code = asyncio.run(main(
        stock_ids=args.stocks,
        force=args.force
    ))

    sys.exit(exit_code)
