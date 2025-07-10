#!/usr/bin/env python3
"""
Lighter合約實時Orderbook收集程序（升級版）
實時接收每一筆orderbook變化並顯示/錄製
"""

import json
import logging
import asyncio
import lighter
import time
from tabulate import tabulate
from datetime import datetime
from collections import deque
import pandas as pd
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LighterOrderbookRecorder:
    def __init__(self, market_ids=[1], enable_recording=False):
        self.market_ids = market_ids
        self.enable_recording = enable_recording
        self.loop = asyncio.get_event_loop()
        
        # 數據緩衝區
        self.data_buffer = deque(maxlen=10000)
        self.last_flush_time = time.time()
        self.flush_interval = 3  # 秒
        self.buffer_size = 500
        
        # 統計信息
        self.msg_count = 0
        self.start_time = time.time()
        self.last_update_time = None
        
        # 創建數據目錄
        if self.enable_recording:
            self.data_dir = Path("orderbook_data/lighter")
            self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def process_orderbook_data(self, market_id, order_book):
        """處理訂單簿數據並計算衍生指標"""
        timestamp = datetime.now()
        
        # 提取賣單和買單
        asks = order_book.get("asks", [])
        bids = order_book.get("bids", [])
        
        # 排序買賣單
        sorted_asks = sorted(asks, key=lambda x: float(x["price"]))[:10]
        sorted_bids = sorted(bids, key=lambda x: float(x["price"]), reverse=True)[:10]
        
        # 計算最佳買賣價
        best_bid = float(sorted_bids[0]["price"]) if sorted_bids else None
        best_ask = float(sorted_asks[0]["price"]) if sorted_asks else None
        best_bid_size = float(sorted_bids[0]["size"]) if sorted_bids else None
        best_ask_size = float(sorted_asks[0]["size"]) if sorted_asks else None
        
        # 計算價差和中間價
        spread = (best_ask - best_bid) if (best_bid and best_ask) else None
        spread_percent = (spread / best_ask * 100) if spread else None
        mid_price = ((best_bid + best_ask) / 2) if (best_bid and best_ask) else None
        
        # 計算總量
        total_bid_volume = sum([float(bid["size"]) for bid in sorted_bids])
        total_ask_volume = sum([float(ask["size"]) for ask in sorted_asks])
        
        return {
            "timestamp": timestamp.isoformat(),
            "exchange": "lighter",
            "market_id": market_id,
            "event_type": "orderbook_update",
            "bids": [[float(bid["price"]), float(bid["size"])] for bid in sorted_bids],
            "asks": [[float(ask["price"]), float(ask["size"])] for ask in sorted_asks],
            "best_bid": best_bid,
            "best_ask": best_ask,
            "best_bid_size": best_bid_size,
            "best_ask_size": best_ask_size,
            "spread": spread,
            "spread_percent": spread_percent,
            "mid_price": mid_price,
            "total_bid_volume": total_bid_volume,
            "total_ask_volume": total_ask_volume
        }
    
    def display_orderbook(self, data):
        """顯示訂單簿數據"""
        # 清屏（可選）
        os.system('clear' if os.name == 'posix' else 'cls')
        
        print(f"\n{'='*60}")
        print(f"🌟 Lighter合約實時訂單簿 - Market {data['market_id']}")
        print(f"📅 時間: {data['timestamp'][:19]}")
        print(f"{'='*60}")
        
        # 顯示最佳價格信息
        if data['best_bid'] and data['best_ask']:
            print(f"🟢 最佳買價: {data['best_bid']:.2f} (數量: {data['best_bid_size']:.2f})")
            print(f"🔴 最佳賣價: {data['best_ask']:.2f} (數量: {data['best_ask_size']:.2f})")
            print(f"📊 中間價: {data['mid_price']:.2f}")
            print(f"📈 價差: {data['spread']:.2f} ({data['spread_percent']:.3f}%)")
        
        print(f"💰 總買量: {data['total_bid_volume']:.2f} | 總賣量: {data['total_ask_volume']:.2f}")
        print()
        
        # 格式化賣單數據（從高到低）
        asks_display = []
        for price, qty in reversed(data['asks']):
            total_value = price * qty
            asks_display.append([f"{price:.2f}", f"{qty:.2f}", f"{total_value:.2f}"])
        
        # 顯示賣單
        if asks_display:
            print("📈 賣單 (Asks)")
            print(tabulate(asks_display, 
                          headers=["價格", "數量", "總值"],
                          tablefmt="pretty",
                          stralign="right"))
        
        print(f"\n{' '*20}--- 價差: {data['spread']:.2f} ---\n")
        
        # 格式化買單數據（從高到低）
        bids_display = []
        for price, qty in data['bids']:
            total_value = price * qty
            bids_display.append([f"{price:.2f}", f"{qty:.2f}", f"{total_value:.2f}"])
        
        # 顯示買單
        if bids_display:
            print("📉 買單 (Bids)")
            print(tabulate(bids_display,
                          headers=["價格", "數量", "總值"],
                          tablefmt="pretty",
                          stralign="right"))
    
    def display_stats(self):
        """顯示統計信息"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        rate = self.msg_count / elapsed if elapsed > 0 else 0
        
        print(f"\n📊 統計信息:")
        print(f"   消息總數: {self.msg_count}")
        print(f"   接收頻率: {rate:.2f} msg/sec")
        print(f"   緩衝區大小: {len(self.data_buffer)}")
        print(f"   運行時間: {elapsed:.0f}秒")
    
    async def check_flush_buffer(self):
        """檢查是否需要刷新緩衝區"""
        current_time = time.time()
        if (len(self.data_buffer) >= self.buffer_size or 
            current_time - self.last_flush_time > self.flush_interval):
            await self.flush_buffer()
    
    async def flush_buffer(self):
        """刷新數據緩衝區到文件"""
        if not self.data_buffer:
            return
        
        try:
            # 獲取當前小時的文件名
            now = datetime.now()
            # 使用更規範的文件名格式：lighter_orderbook_BTCUSDT_YYYY_MM_DD_HH.parquet
            filename = f"lighter_orderbook_market{self.market_ids[0]}_{now.strftime('%Y_%m_%d_%H')}.parquet"
            filepath = self.data_dir / filename
            
            # 轉換為DataFrame
            df = pd.DataFrame(list(self.data_buffer))
            
            # 保存到parquet文件（追加模式）
            if filepath.exists():
                existing_df = pd.read_parquet(filepath)
                df = pd.concat([existing_df, df], ignore_index=True)
            
            df.to_parquet(filepath, engine='pyarrow', compression='snappy')
            
            print(f"\n💾 已保存 {len(self.data_buffer)} 條記錄到 {filepath}")
            logger.info(f"Flushed {len(self.data_buffer)} records to {filepath}")
            
            # 清空緩衝區
            self.data_buffer.clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            logger.error(f"刷新緩衝區時出錯: {e}", exc_info=True)
    
    def on_order_book_update(self, market_id, order_book):
        """訂單簿更新回調函數 - 同步版本"""
        try:
            # 創建異步任務
            asyncio.run_coroutine_threadsafe(
                self._process_order_book_update(market_id, order_book),
                self.loop
            )
        except Exception as e:
            logger.error(f"創建訂單簿更新任務時出錯: {e}", exc_info=True)

    async def _process_order_book_update(self, market_id, order_book):
        """訂單簿更新的異步處理函數"""
        try:
            # 統計信息
            self.msg_count += 1
            self.last_update_time = datetime.now()
            
            # 處理數據（顯示和錄製）
            processed_data = self.process_orderbook_data(market_id, order_book)
            
            # 顯示訂單簿
            self.display_orderbook(processed_data)
            
            # 錄製數據
            if self.enable_recording:
                self.data_buffer.append(processed_data)
                await self.check_flush_buffer()
            
            # 定期顯示統計信息
            if self.msg_count % 50 == 0:
                self.display_stats()
                
        except Exception as e:
            logger.error(f"處理訂單簿更新時出錯: {e}", exc_info=True)
    
    def on_account_update(self, account_id, account):
        """賬戶更新回調函數 - 已棄用"""
        pass
    
    async def stop(self):
        """停止程序並保存剩餘數據"""
        if self.enable_recording and self.data_buffer:
            await self.flush_buffer()
        print(f"\n🛑 程序已停止")

async def main():
    """主函數"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Lighter合約實時Orderbook收集程序')
    parser.add_argument('--markets', nargs='+', type=int, default=[1], help='Market ID列表')
    parser.add_argument('--record', action='store_true', help='啟用數據錄製')
    
    args = parser.parse_args()
    
    # 創建Lighter orderbook錄製器
    recorder = LighterOrderbookRecorder(
        market_ids=args.markets,
        enable_recording=args.record
    )
    
    try:
        print(f"\033[1;32m🌟 啟動Lighter合約Orderbook收集器\033[0m")
        print(f"   Market IDs: {args.markets}")
        print(f"   錄製模式: {'啟用' if args.record else '禁用'}")
        print(f"   按 Ctrl+C 停止程序\n")
        
        # 創建Lighter WebSocket客戶端
        client = lighter.WsClient(
            order_book_ids=args.markets,
            on_order_book_update=recorder.on_order_book_update,
        )
        
        await client.run_async()
        
    except KeyboardInterrupt:
        print(f"\n\033[1;33m收到停止信號...\033[0m")
        await recorder.stop()
    except Exception as e:
        logger.error(f"程序運行時出錯: {e}", exc_info=True)
        await recorder.stop()

if __name__ == "__main__":
    asyncio.run(main())
