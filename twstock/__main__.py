# -*- coding: utf-8 -*-

from twstock.all import All

if __name__ == "__main__":
    import asyncio

    all_processor = All()
    asyncio.run(all_processor.get_all_stock_list())
    asyncio.run(all_processor.get_all_stock_parallel())
