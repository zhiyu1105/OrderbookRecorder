#!/usr/bin/env python3
"""
幣安現貨實時Orderbook收集程序
實時接收每一筆orderbook變化並顯示/錄製
"""

import json
import logging
import asyncio
import websockets
import time
from datetime import datetime
from tabulate import tabulate
from collections import deque
import pandas as pd
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceSpotOrderbook:
    def __init__(self, symbol="BTCUSDT", depth_levels=10, enable_recording=False):
        self.symbol = symbol.upper()
        self.depth_levels = depth_levels
        self.enable_recording = enable_recording
        
        # WebSocket配置
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth"
        
        # 數據緩衝區
        self.data_buffer = deque(maxlen=10000)
        self.last_flush_time = time.time()
        self.flush_interval = 5  # 秒
        self.buffer_size = 1000
        
        # 訂單簿數據
        self.orderbook = {"bids": {}, "asks": {}}
        self.sequence_id = 0
        
        # 統計信息
        self.msg_count = 0
        self.start_time = time.time()
        self.last_update_time = None
        
        # 創建數據目錄
        if self.enable_recording:
            self.data_dir = Path("orderbook_data/binance_spot") / self.symbol
            self.data_dir.mkdir(parents=True, exist_ok=True)
    
    async def connect(self):
        """連接到WebSocket並開始接收數據"""
        reconnect_delay = 1
        max_reconnect_delay = 60
        
        while True:
            try:
                print(f"\033[1;32m[幣安現貨] 連接到: {self.ws_url}\033[0m")
                logger.info(f"Connecting to Binance Spot WebSocket: {self.ws_url}")
                
                async with websockets.connect(self.ws_url) as websocket:
                    print(f"\033[1;32m[幣安現貨] WebSocket連接成功! 訂閱 {self.symbol} 深度數據\033[0m")
                    reconnect_delay = 1  # 重置重連延遲
                    
                    async for message in websocket:
                        try:
                            await self.process_message(message)
                        except Exception as e:
                            logger.error(f"處理消息時出錯: {e}", exc_info=True)
                            
            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
                print(f"\033[1;33m[幣安現貨] 連接斷開: {e}, {reconnect_delay}秒後重連...\033[0m")
                logger.warning(f"WebSocket connection lost: {e}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                
            except Exception as e:
                print(f"\033[1;31m[幣安現貨] 連接錯誤: {e}, {reconnect_delay}秒後重連...\033[0m")
                logger.error(f"WebSocket error: {e}", exc_info=True)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    async def process_message(self, message):
        """處理接收到的WebSocket消息"""
        try:
            data = json.loads(message)
            
            # 檢查是否為深度更新事件
            if 'e' in data and data['e'] == 'depthUpdate':
                await self.handle_depth_update(data)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析錯誤: {e}")
        except Exception as e:
            logger.error(f"處理消息時出錯: {e}", exc_info=True)
    
    async def handle_depth_update(self, data):
        """處理深度更新數據"""
        try:
            # 更新本地訂單簿
            self.update_local_orderbook(data)
            
            # 統計信息
            self.msg_count += 1
            self.last_update_time = datetime.now()
            self.sequence_id = data.get('u', self.sequence_id)
            
            # 處理數據（顯示和錄製）
            processed_data = self.process_orderbook_data(data)
            
            # 顯示訂單簿
            self.display_orderbook(processed_data)
            
            # 錄製數據
            if self.enable_recording:
                self.data_buffer.append(processed_data)
                await self.check_flush_buffer()
            
            # 定期顯示統計信息
            if self.msg_count % 100 == 0:
                self.display_stats()
                
        except Exception as e:
            logger.error(f"處理深度更新時出錯: {e}", exc_info=True)
    
    def update_local_orderbook(self, data):
        """更新本地訂單簿"""
        # 更新買單
        for bid in data.get('b', []):
            price, quantity = float(bid[0]), float(bid[1])
            if quantity == 0:
                self.orderbook['bids'].pop(price, None)
            else:
                self.orderbook['bids'][price] = quantity
        
        # 更新賣單
        for ask in data.get('a', []):
            price, quantity = float(ask[0]), float(ask[1])
            if quantity == 0:
                self.orderbook['asks'].pop(price, None)
            else:
                self.orderbook['asks'][price] = quantity
    
    def process_orderbook_data(self, raw_data):
        """處理訂單簿數據並計算衍生指標"""
        timestamp = datetime.now()
        
        # 獲取排序後的買賣單
        sorted_bids = sorted(self.orderbook['bids'].items(), reverse=True)[:self.depth_levels]
        sorted_asks = sorted(self.orderbook['asks'].items())[:self.depth_levels]
        
        # 計算最佳買賣價
        best_bid = sorted_bids[0][0] if sorted_bids else None
        best_ask = sorted_asks[0][0] if sorted_asks else None
        best_bid_size = sorted_bids[0][1] if sorted_bids else None
        best_ask_size = sorted_asks[0][1] if sorted_asks else None
        
        # 計算價差和中間價
        spread = (best_ask - best_bid) if (best_bid and best_ask) else None
        spread_percent = (spread / best_ask * 100) if spread else None
        mid_price = ((best_bid + best_ask) / 2) if (best_bid and best_ask) else None
        
        # 計算總量
        total_bid_volume = sum([qty for _, qty in sorted_bids])
        total_ask_volume = sum([qty for _, qty in sorted_asks])
        
        return {
            "timestamp": timestamp.isoformat(),
            "sequence_id": self.sequence_id,
            "exchange": "binance_spot",
            "symbol": self.symbol,
            "event_type": "depthUpdate",
            "bids": [[price, qty] for price, qty in sorted_bids],
            "asks": [[price, qty] for price, qty in sorted_asks],
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
        print(f"🚀 幣安現貨實時訂單簿 - {data['symbol']}")
        print(f"📅 時間: {data['timestamp'][:19]}")
        print(f"🔢 序號: {data['sequence_id']}")
        print(f"{'='*60}")
        
        # 顯示最佳價格信息
        if data['best_bid'] and data['best_ask']:
            print(f"🟢 最佳買價: {data['best_bid']:.2f} (數量: {data['best_bid_size']:.4f})")
            print(f"🔴 最佳賣價: {data['best_ask']:.2f} (數量: {data['best_ask_size']:.4f})")
            print(f"📊 中間價: {data['mid_price']:.2f}")
            print(f"📈 價差: {data['spread']:.2f} ({data['spread_percent']:.3f}%)")
        
        print(f"💰 總買量: {data['total_bid_volume']:.4f} | 總賣量: {data['total_ask_volume']:.4f}")
        print()
        
        # 格式化賣單數據（從高到低）
        asks_display = []
        for price, qty in reversed(data['asks']):
            total_value = price * qty
            asks_display.append([f"{price:.2f}", f"{qty:.4f}", f"{total_value:.2f}"])
        
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
            bids_display.append([f"{price:.2f}", f"{qty:.4f}", f"{total_value:.2f}"])
        
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
            # 使用統一的文件名格式：binance_spot_orderbook_BTCUSDT_YYYY_MM_DD_HH.parquet
            filename = f"binance_spot_orderbook_{self.symbol}_{now.strftime('%Y_%m_%d_%H')}.parquet"
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
    
    async def stop(self):
        """停止程序並保存剩餘數據"""
        if self.enable_recording and self.data_buffer:
            await self.flush_buffer()
        print(f"\n🛑 程序已停止")

async def main():
    """主函數"""
    import argparse
    
    parser = argparse.ArgumentParser(description='幣安現貨實時Orderbook收集程序')
    parser.add_argument('--symbol', default='BTCUSDT', help='交易對符號')
    parser.add_argument('--depth', type=int, default=10, help='顯示深度級數')
    parser.add_argument('--record', action='store_true', help='啟用數據錄製')
    
    args = parser.parse_args()
    
    # 創建orderbook收集器
    collector = BinanceSpotOrderbook(
        symbol=args.symbol,
        depth_levels=args.depth,
        enable_recording=args.record
    )
    
    try:
        print(f"\033[1;32m🚀 啟動幣安現貨Orderbook收集器\033[0m")
        print(f"   交易對: {args.symbol}")
        print(f"   深度級數: {args.depth}")
        print(f"   錄製模式: {'啟用' if args.record else '禁用'}")
        print(f"   按 Ctrl+C 停止程序\n")
        
        await collector.connect()
        
    except KeyboardInterrupt:
        print(f"\n\033[1;33m收到停止信號...\033[0m")
        await collector.stop()
    except Exception as e:
        logger.error(f"程序運行時出錯: {e}", exc_info=True)
        await collector.stop()

if __name__ == "__main__":
    asyncio.run(main()) 