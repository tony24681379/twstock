import asyncio
import time
from typing import Any, Awaitable, Callable


async def batch_execute(
    items: list[str],
    worker: Callable[[str], Awaitable[tuple[str, Any]]],
    max_workers: int = 10,
    label: str = "處理",
    progress_interval: int = 50,
) -> dict[str, Any]:
    """
    通用批量並發執行器

    Args:
        items: 要處理的 item ID 列表
        worker: async 函式，接受 item ID，回傳 (item_id, result)
        max_workers: 最大並發數
        label: 進度顯示的標籤
        progress_interval: 每幾個 item 顯示一次進度

    Returns:
        {item_id: result} 字典，失敗的項目不包含在結果中
    """
    if not items:
        return {}

    semaphore = asyncio.Semaphore(max_workers)
    results = {}
    success_count = 0
    failed_count = 0
    start_time = time.time()

    async def wrapped_worker(item_id: str):
        async with semaphore:
            return await worker(item_id)

    tasks = [wrapped_worker(item_id) for item_id in items]

    print(f"   並發{label}數: {max_workers}")

    for task in asyncio.as_completed(tasks):
        try:
            item_id, result = await task
            results[item_id] = result
            success_count += 1
        except Exception as e:
            failed_count += 1
            print(f"   ❌ {label}失敗: {e}")

        completed = success_count + failed_count
        if completed % progress_interval == 0 or completed <= 10:
            elapsed = time.time() - start_time
            speed = success_count / elapsed if elapsed > 0 else 0
            remaining = len(items) - completed
            eta = remaining / speed if speed > 0 else 0
            print(
                f"   {label}進度: [{completed}/{len(items)}] "
                f"成功: {success_count} 失敗: {failed_count} - "
                f"速度: {speed:.2f} 支/秒 - 預估剩餘: {int(eta/60)}分{int(eta%60)}秒"
            )

    elapsed = time.time() - start_time
    print(f"\n   ✓ {label}完成！成功: {success_count} 失敗: {failed_count}")
    print(f"   耗時: {int(elapsed/60)}分{int(elapsed%60)}秒")

    return results
