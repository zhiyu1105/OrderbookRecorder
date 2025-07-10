#!/usr/bin/env python3
"""
統一多交易所高頻實時Orderbook錄製管理系統
支持同時錄製幣安現貨、幣安合約、Lighter合約的orderbook數據
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
from collections import deque
import psutil
import signal
import sys

# 導入各交易所的收集器
from binance_spot_orderbook import BinanceSpotOrderbook
from binance_futures_orderbook import BinanceFuturesOrderbook
from lighter_orderbook import LighterOrderbookRecorder

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrderbookRecorder:
    """統一orderbook錄製管理器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.collectors = {}
        self.tasks = []
        self.start_time = time.time()
        self.is_running = False
        self.stats = {
            'total_messages': 0,
            'total_records_saved': 0,
            'exchanges': {}
        }
        
        # 創建主數據目錄
        self.base_data_dir = Path(config.get('base_data_dir', 'orderbook_data'))
        self.base_data_dir.mkdir(parents=True, exist_ok=True)
        
        # 設置信號處理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信號處理器"""
        logger.info(f"收到信號 {signum}，開始停止程序...")
        asyncio.create_task(self.stop())
    
    async def start_recording(self, symbols: List[str] = None, duration_hours: int = None):
        """開始錄製"""
        self.is_running = True
        
        print(f"\033[1;32m🚀 啟動統一Orderbook錄製系統\033[0m")
        print(f"📅 開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if duration_hours:
            print(f"⏰ 錄製時長: {duration_hours} 小時")
        
        print(f"🔧 配置信息:")
        for exchange, config in self.config.get('exchanges', {}).items():
            if config.get('enabled', True):
                print(f"   {exchange}: {config.get('symbols', config.get('market_ids', []))}")
        print()
        
        # 創建各交易所收集器
        await self._create_collectors(symbols)
        
        # 啟動所有收集器
        await self._start_all_collectors()
        
        # 啟動統計和監控任務
        self.tasks.append(asyncio.create_task(self._stats_monitor()))
        
        # 如果設置了持續時間，創建自動停止任務
        if duration_hours:
            self.tasks.append(asyncio.create_task(self._auto_stop(duration_hours)))
        
        try:
            # 等待所有任務完成
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"錄製過程中出錯: {e}")
        finally:
            await self.stop()
    
    async def _create_collectors(self, symbols: List[str] = None):
        """創建各交易所收集器"""
        exchanges_config = self.config.get('exchanges', {})
        
        # 幣安現貨
        if exchanges_config.get('binance_spot', {}).get('enabled', False):
            spot_config = exchanges_config['binance_spot']
            spot_symbols = symbols or spot_config.get('symbols', ['BTCUSDT'])
            
            for symbol in spot_symbols:
                collector_id = f"binance_spot_{symbol}"
                self.collectors[collector_id] = BinanceSpotOrderbook(
                    symbol=symbol,
                    depth_levels=spot_config.get('depth_levels', 20),
                    enable_recording=True
                )
                self.stats['exchanges'][collector_id] = {
                    'messages': 0,
                    'last_update': None,
                    'start_time': time.time()
                }
        
        # 幣安合約
        if exchanges_config.get('binance_futures', {}).get('enabled', False):
            futures_config = exchanges_config['binance_futures']
            futures_symbols = symbols or futures_config.get('symbols', ['BTCUSDT'])
            
            for symbol in futures_symbols:
                collector_id = f"binance_futures_{symbol}"
                self.collectors[collector_id] = BinanceFuturesOrderbook(
                    symbol=symbol,
                    depth_levels=futures_config.get('depth_levels', 20),
                    enable_recording=True
                )
                self.stats['exchanges'][collector_id] = {
                    'messages': 0,
                    'last_update': None,
                    'start_time': time.time()
                }
        
        # Lighter合約
        if exchanges_config.get('lighter', {}).get('enabled', False):
            lighter_config = exchanges_config['lighter']
            market_ids = lighter_config.get('market_ids', [1])
            
            collector_id = "lighter_markets"
            self.collectors[collector_id] = LighterOrderbookRecorder(
                market_ids=market_ids,
                account_ids=[],
                enable_recording=True
            )
            self.stats['exchanges'][collector_id] = {
                'messages': 0,
                'last_update': None,
                'start_time': time.time()
            }
        
        logger.info(f"創建了 {len(self.collectors)} 個收集器")
    
    async def _start_all_collectors(self):
        """啟動所有收集器"""
        for collector_id, collector in self.collectors.items():
            if 'binance_spot' in collector_id:
                task = asyncio.create_task(collector.connect())
                self.tasks.append(task)
                
            elif 'binance_futures' in collector_id:
                task = asyncio.create_task(collector.connect())
                self.tasks.append(task)
                
            elif 'lighter' in collector_id:
                # Lighter需要特殊處理
                import lighter
                client = lighter.WsClient(
                    order_book_ids=collector.market_ids,
                    account_ids=collector.account_ids,
                    on_order_book_update=collector.on_order_book_update,
                )
                task = asyncio.create_task(client.run_async())
                self.tasks.append(task)
        
        logger.info(f"啟動了 {len(self.tasks)} 個收集任務")
    
    async def _stats_monitor(self):
        """統計監控任務"""
        while self.is_running:
            try:
                await asyncio.sleep(30)  # 每30秒更新一次統計
                self._display_overall_stats()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"統計監控錯誤: {e}")
    
    async def _auto_stop(self, duration_hours: int):
        """自動停止任務"""
        try:
            await asyncio.sleep(duration_hours * 3600)
            logger.info(f"達到設定的錄製時長 {duration_hours} 小時，自動停止")
            await self.stop()
        except asyncio.CancelledError:
            pass
    
    def _display_overall_stats(self):
        """顯示整體統計信息"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        print(f"\n{'='*80}")
        print(f"📊 統一Orderbook錄製系統統計 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        print(f"⏱️  運行時間: {elapsed/3600:.1f} 小時 ({elapsed:.0f} 秒)")
        
        # 系統資源
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        cpu_percent = process.cpu_percent()
        
        print(f"💻 系統資源:")
        print(f"   內存使用: {memory_mb:.1f} MB")
        print(f"   CPU使用率: {cpu_percent:.1f}%")
        
        # 各交易所統計
        print(f"\n📈 各交易所統計:")
        total_rate = 0
        
        for collector_id, collector in self.collectors.items():
            if hasattr(collector, 'msg_count'):
                msg_count = collector.msg_count
                collector_elapsed = current_time - self.stats['exchanges'][collector_id]['start_time']
                rate = msg_count / collector_elapsed if collector_elapsed > 0 else 0
                total_rate += rate
                
                buffer_size = len(collector.data_buffer) if hasattr(collector, 'data_buffer') else 0
                
                print(f"   {collector_id}:")
                print(f"     消息數: {msg_count:,}")
                print(f"     頻率: {rate:.1f} msg/sec")
                print(f"     緩衝區: {buffer_size}")
        
        print(f"\n🚀 總接收頻率: {total_rate:.1f} msg/sec")
        print(f"{'='*80}\n")
    
    async def stop(self):
        """停止錄製"""
        if not self.is_running:
            return
        
        self.is_running = False
        logger.info("正在停止錄製系統...")
        
        # 停止所有收集器並保存剩餘數據
        for collector_id, collector in self.collectors.items():
            try:
                if hasattr(collector, 'stop'):
                    await collector.stop()
            except Exception as e:
                logger.error(f"停止收集器 {collector_id} 時出錯: {e}")
        
        # 取消所有任務
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # 等待任務完成
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # 顯示最終統計
        self._display_final_stats()
        
        print(f"\n\033[1;32m✅ 錄製系統已完全停止\033[0m")
    
    def _display_final_stats(self):
        """顯示最終統計"""
        elapsed = time.time() - self.start_time
        
        print(f"\n{'='*80}")
        print(f"📋 最終統計報告")
        print(f"{'='*80}")
        print(f"⏱️  總運行時間: {elapsed/3600:.2f} 小時")
        
        total_messages = 0
        for collector_id, collector in self.collectors.items():
            if hasattr(collector, 'msg_count'):
                total_messages += collector.msg_count
                rate = collector.msg_count / elapsed if elapsed > 0 else 0
                print(f"   {collector_id}: {collector.msg_count:,} 條消息 ({rate:.1f} msg/sec)")
        
        print(f"\n🎯 總消息數: {total_messages:,}")
        print(f"📊 平均頻率: {total_messages/elapsed:.1f} msg/sec")
        print(f"💾 數據存儲: {self.base_data_dir}")
        print(f"{'='*80}")
    
    def flush_all_buffers(self):
        """強制刷新所有緩衝區"""
        for collector_id, collector in self.collectors.items():
            try:
                if hasattr(collector, 'flush_buffer'):
                    asyncio.create_task(collector.flush_buffer())
            except Exception as e:
                logger.error(f"刷新緩衝區 {collector_id} 時出錯: {e}")

def load_config(config_file: str = None) -> Dict[str, Any]:
    """加載配置文件"""
    default_config = {
        "base_data_dir": "orderbook_data",
        "exchanges": {
            "binance_spot": {
                "enabled": True,
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "depth_levels": 20,
                "buffer_size": 1000,
                "flush_interval": 5
            },
            "binance_futures": {
                "enabled": True,
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "depth_levels": 20,
                "buffer_size": 1000,
                "flush_interval": 5
            },
            "lighter": {
                "enabled": False,  # 默認關閉，需要lighter包
                "market_ids": [1, 2],
                "depth_levels": 10,
                "buffer_size": 500,
                "flush_interval": 3
            }
        },
        "storage": {
            "format": "parquet",
            "compression": "snappy",
            "file_rotation": "hourly",
            "max_records_per_file": 100000
        },
        "performance": {
            "async_write": True,
            "batch_write": True,
            "memory_limit_mb": 512,
            "write_threads": 2
        }
    }
    
    if config_file and Path(config_file).exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                user_config = json.load(f)
            # 合併配置
            default_config.update(user_config)
        except Exception as e:
            logger.warning(f"讀取配置文件失敗: {e}，使用默認配置")
    
    return default_config

async def main():
    """主函數"""
    import argparse
    
    parser = argparse.ArgumentParser(description='統一多交易所Orderbook錄製系統')
    parser.add_argument('--config', help='配置文件路徑')
    parser.add_argument('--mode', choices=['realtime', 'test'], default='realtime', help='運行模式')
    parser.add_argument('--duration', type=float, help='錄製時長（小時）')
    parser.add_argument('--symbols', nargs='+', help='交易對符號列表')
    parser.add_argument('--exchanges', nargs='+', 
                       choices=['binance_spot', 'binance_futures', 'lighter'],
                       help='啟用的交易所')
    
    args = parser.parse_args()
    
    # 加載配置
    config = load_config(args.config)
    
    # 根據命令行參數調整配置
    if args.exchanges:
        for exchange in config['exchanges']:
            config['exchanges'][exchange]['enabled'] = exchange in args.exchanges
    
    if args.symbols:
        for exchange_config in config['exchanges'].values():
            if 'symbols' in exchange_config:
                exchange_config['symbols'] = args.symbols
    
    # 創建錄製器
    recorder = OrderbookRecorder(config)
    
    try:
        await recorder.start_recording(
            symbols=args.symbols,
            duration_hours=args.duration
        )
    except KeyboardInterrupt:
        print(f"\n\033[1;33m收到停止信號...\033[0m")
    except Exception as e:
        logger.error(f"程序運行時出錯: {e}", exc_info=True)
    finally:
        await recorder.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n\033[1;33m程序被中斷\033[0m")
    except Exception as e:
        logger.error(f"程序錯誤: {e}", exc_info=True) 