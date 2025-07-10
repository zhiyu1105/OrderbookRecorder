#!/usr/bin/env python3
"""
高頻Orderbook錄製系統測試腳本
用於驗證各個組件是否正常工作
"""

import asyncio
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_binance_spot():
    """測試幣安現貨orderbook收集"""
    print("\n🚀 測試幣安現貨Orderbook收集器...")
    
    try:
        from binance_spot_orderbook import BinanceSpotOrderbook
        
        collector = BinanceSpotOrderbook(
            symbol="BTCUSDT",
            depth_levels=5,
            enable_recording=False  # 測試時不錄製
        )
        
        # 創建連接任務
        task = asyncio.create_task(collector.connect())
        
        # 運行10秒
        await asyncio.sleep(10)
        
        # 檢查是否收到數據
        if collector.msg_count > 0:
            print(f"✅ 幣安現貨測試成功! 收到 {collector.msg_count} 條消息")
            rate = collector.msg_count / 10
            print(f"   接收頻率: {rate:.1f} msg/sec")
            return True
        else:
            print("❌ 幣安現貨測試失敗: 未收到數據")
            return False
            
    except ImportError as e:
        print(f"❌ 導入錯誤: {e}")
        return False
    except Exception as e:
        print(f"❌ 幣安現貨測試錯誤: {e}")
        return False
    finally:
        # 取消任務
        if 'task' in locals() and not task.done():
            task.cancel()

async def test_binance_futures():
    """測試幣安合約orderbook收集"""
    print("\n⚡ 測試幣安合約Orderbook收集器...")
    
    try:
        from binance_futures_orderbook import BinanceFuturesOrderbook
        
        collector = BinanceFuturesOrderbook(
            symbol="BTCUSDT",
            depth_levels=5,
            enable_recording=False
        )
        
        task = asyncio.create_task(collector.connect())
        await asyncio.sleep(10)
        
        if collector.msg_count > 0:
            print(f"✅ 幣安合約測試成功! 收到 {collector.msg_count} 條消息")
            rate = collector.msg_count / 10
            print(f"   接收頻率: {rate:.1f} msg/sec")
            return True
        else:
            print("❌ 幣安合約測試失敗: 未收到數據")
            return False
            
    except ImportError as e:
        print(f"❌ 導入錯誤: {e}")
        return False
    except Exception as e:
        print(f"❌ 幣安合約測試錯誤: {e}")
        return False
    finally:
        if 'task' in locals() and not task.done():
            task.cancel()

async def test_lighter():
    """測試Lighter合約orderbook收集"""
    print("\n🌟 測試Lighter合約Orderbook收集器...")
    
    try:
        from lighter_orderbook import LighterOrderbookRecorder
        import lighter
        
        recorder = LighterOrderbookRecorder(
            market_ids=[1],
            enable_recording=False
        )
        
        client = lighter.WsClient(
            order_book_ids=[1],
            on_order_book_update=recorder.on_order_book_update,
        )
        
        # 創建連接任務
        task = asyncio.create_task(client.run_async())
        
        try:
            # 運行10秒
            await asyncio.sleep(10)
            
            if recorder.msg_count > 0:
                print(f"✅ Lighter測試成功! 收到 {recorder.msg_count} 條消息")
                rate = recorder.msg_count / 10
                print(f"   接收頻率: {rate:.1f} msg/sec")
                return True
            else:
                print("❌ Lighter測試失敗: 未收到數據")
                return False
        finally:
            # 取消任務並等待其完成
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # 停止recorder
            await recorder.stop()
            
    except ImportError as e:
        print(f"⚠️ Lighter未安裝，跳過測試: {e}")
        return None
    except Exception as e:
        print(f"❌ Lighter測試錯誤: {e}")
        return False

def test_dependencies():
    """測試依賴包是否正確安裝"""
    print("\n📦 檢查依賴包...")
    
    required_packages = [
        'asyncio', 'websockets', 'pandas', 'pyarrow', 
        'tabulate', 'psutil', 'aiohttp', 'json', 'logging'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            if package == 'asyncio':
                import asyncio
            elif package == 'websockets':
                import websockets
            elif package == 'pandas':
                import pandas
            elif package == 'pyarrow':
                import pyarrow
            elif package == 'tabulate':
                import tabulate
            elif package == 'psutil':
                import psutil
            elif package == 'aiohttp':
                import aiohttp
            elif package == 'json':
                import json
            elif package == 'logging':
                import logging
            
            print(f"✅ {package}")
            
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package}")
    
    if missing_packages:
        print(f"\n⚠️  缺少依賴包: {', '.join(missing_packages)}")
        print("請運行: pip install -r requirements.txt")
        return False
    else:
        print("\n✅ 所有依賴包都已正確安裝!")
        return True

def test_file_structure():
    """測試文件結構"""
    print("\n📁 檢查文件結構...")
    
    required_files = [
        'binance_spot_orderbook.py',
        'binance_futures_orderbook.py', 
        'lighter_orderbook.py',
        'orderbook_recorder.py',
        'config.json',
        'requirements.txt',
        'README.md'
    ]
    
    missing_files = []
    
    for filename in required_files:
        file_path = Path(filename)
        if file_path.exists():
            print(f"✅ {filename}")
        else:
            missing_files.append(filename)
            print(f"❌ {filename}")
    
    if missing_files:
        print(f"\n⚠️  缺少文件: {', '.join(missing_files)}")
        return False
    else:
        print("\n✅ 所有必需文件都存在!")
        return True

async def test_data_recording():
    """測試數據錄製功能"""
    print("\n💾 測試數據錄製功能...")
    
    try:
        from binance_spot_orderbook import BinanceSpotOrderbook
        
        # 創建測試目錄
        test_dir = Path("test_data")
        test_dir.mkdir(exist_ok=True)
        
        collector = BinanceSpotOrderbook(
            symbol="BTCUSDT",
            depth_levels=5,
            enable_recording=True
        )
        
        # 修改數據目錄到測試目錄
        collector.data_dir = test_dir
        collector.buffer_size = 10  # 小緩衝區便於測試
        collector.flush_interval = 2  # 短間隔
        
        task = asyncio.create_task(collector.connect())
        await asyncio.sleep(15)  # 運行15秒確保觸發錄製
        
        # 強制刷新緩衝區
        await collector.flush_buffer()
        
        # 檢查是否生成了數據文件
        data_files = list(test_dir.glob("*.parquet"))
        
        if data_files:
            print(f"✅ 數據錄製測試成功! 生成了 {len(data_files)} 個文件")
            for file in data_files:
                size_kb = file.stat().st_size / 1024
                print(f"   {file.name}: {size_kb:.1f} KB")
            
            # 清理測試文件
            for file in data_files:
                file.unlink()
            test_dir.rmdir()
            
            return True
        else:
            print("❌ 數據錄製測試失敗: 未生成數據文件")
            return False
            
    except Exception as e:
        print(f"❌ 數據錄製測試錯誤: {e}")
        return False
    finally:
        if 'task' in locals() and not task.done():
            task.cancel()

async def main():
    """主測試函數"""
    print("🧪 高頻Orderbook錄製系統測試")
    print("=" * 50)
    
    # 記錄測試結果
    results = {}
    
    # 1. 測試依賴包
    results['dependencies'] = test_dependencies()
    
    # 2. 測試文件結構
    results['file_structure'] = test_file_structure()
    
    if not results['dependencies'] or not results['file_structure']:
        print("\n❌ 基本環境檢查失敗，請先修復上述問題")
        return
    
    # 3. 測試各組件連接
    print("\n🔗 開始連接測試...")
    
    # results['binance_spot'] = await test_binance_spot()
    # results['binance_futures'] = await test_binance_futures()
    results['lighter'] = await test_lighter()
    
    # 4. 測試數據錄製
    results['data_recording'] = await test_data_recording()
    
    # 總結測試結果
    print("\n" + "=" * 50)
    print("📋 測試結果總結:")
    print("=" * 50)
    
    for test_name, result in results.items():
        if result is True:
            status = "✅ 通過"
        elif result is False:
            status = "❌ 失敗"
        else:
            status = "⚠️ 跳過"
        
        print(f"{test_name:20}: {status}")
    
    # 統計成功率
    passed = sum(1 for r in results.values() if r is True)
    total = sum(1 for r in results.values() if r is not None)
    
    print(f"\n🎯 總體成功率: {passed}/{total} ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 所有測試通過! 系統已就緒!")
        print("\n📖 使用說明:")
        print("   1. 單獨測試: python binance_spot_orderbook.py --record")
        print("   2. 統一錄製: python orderbook_recorder.py --duration 1")
        print("   3. 查看README.md獲取詳細使用方法")
    else:
        print("\n⚠️  部分測試失敗，請檢查上述錯誤信息")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 測試被中斷")
    except Exception as e:
        print(f"\n❌ 測試過程中出錯: {e}")
        logger.error("Test error", exc_info=True) 