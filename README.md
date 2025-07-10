# 高頻實時Orderbook錄製系統

一個高性能的多交易所實時orderbook數據收集和錄製系統，支持同時監控幣安現貨、幣安合約和Lighter合約的高頻orderbook變化。

## ✨ 核心特性

- 🚀 **實時高頻收集**: 接收每一筆orderbook變化，而非定時快照
- 🌐 **多交易所支持**: 同時支持幣安現貨、幣安合約、Lighter合約
- 💾 **高效數據存儲**: Parquet格式，snappy壓縮，每小時輪轉文件
- 📊 **實時監控**: 美觀的tabulate格式顯示，實時統計信息
- 🔧 **靈活配置**: JSON配置文件，命令行參數覆蓋
- 🛡️ **可靠性保障**: 自動重連、錯誤處理、緩衝區管理
- ⚡ **高性能**: 異步處理、內存管理、批量寫入

## 📦 安裝依賴

```bash
# 安裝Python依賴
pip install -r requirements.txt

# 如果使用Lighter，需要額外安裝
# pip install lighter  # 或從源碼安裝
```

## 🚀 快速開始

### 1. 單獨運行各交易所收集器

**幣安現貨實時orderbook:**
```bash
# 基本使用
python binance_spot_orderbook.py

# 指定交易對和深度
python binance_spot_orderbook.py --symbol ETHUSDT --depth 20

# 啟用錄製模式
python binance_spot_orderbook.py --symbol BTCUSDT --record
```

**幣安合約實時orderbook:**
```bash
# 基本使用
python binance_futures_orderbook.py

# 指定參數並錄製
python binance_futures_orderbook.py --symbol ETHUSDT --depth 15 --record
```

**Lighter合約實時orderbook:**
```bash
# 基本使用
python lighter_orderbook.py

# 指定市場和錄製
python lighter_orderbook.py --markets 1 2 --record
```

### 2. 統一錄製系統

**實時高頻錄製 (推薦):**
```bash
# 使用默認配置錄製24小時
python orderbook_recorder.py --mode realtime --duration 24

# 指定交易對
python orderbook_recorder.py --symbols BTCUSDT ETHUSDT --duration 12

# 只錄製特定交易所
python orderbook_recorder.py --exchanges binance_spot binance_futures --duration 6

# 使用自定義配置文件
python orderbook_recorder.py --config my_config.json --duration 48
```

## 📊 數據格式

錄製的orderbook數據包含以下字段：

```json
{
  "timestamp": "2024-01-01T12:00:00.123456",
  "sequence_id": 123456789,
  "exchange": "binance_spot",
  "symbol": "BTCUSDT",
  "event_type": "depthUpdate",
  "bids": [["43000.00", "0.5"], ["42999.00", "1.2"]],
  "asks": [["43001.00", "0.8"], ["43002.00", "0.9"]],
  "best_bid": 43000.00,
  "best_ask": 43001.00,
  "spread": 1.00,
  "spread_percent": 0.023,
  "mid_price": 43000.50,
  "total_bid_volume": 15.6,
  "total_ask_volume": 12.8
}
```

## 📁 文件組織結構

```
orderbook_data/
├── binance_spot/
│   ├── BTCUSDT/
│   │   ├── binance_spot_orderbook_BTCUSDT_YYYY_MM_DD_HH.parquet
│   │   └── ...
│   └── ETHUSDT/
├── binance_futures/
│   ├── BTCUSDT/
│   │   ├── binance_futures_orderbook_BTCUSDT_YYYY_MM_DD_HH.parquet
│   │   └── ...
│   └── ETHUSDT/
└── lighter/
    ├── lighter_orderbook_market1_YYYY_MM_DD_HH.parquet
    └── ...
```

## ⚙️ 配置文件

創建 `config.json` 文件來自定義設置：

```json
{
  "base_data_dir": "orderbook_data",
  "exchanges": {
    "binance_spot": {
      "enabled": true,
      "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT"],
      "depth_levels": 20,
      "buffer_size": 1000,
      "flush_interval": 5
    },
    "binance_futures": {
      "enabled": true,
      "symbols": ["BTCUSDT", "ETHUSDT"],
      "depth_levels": 20,
      "buffer_size": 1000,
      "flush_interval": 5
    },
    "lighter": {
      "enabled": false,
      "market_ids": [1, 2],
      "depth_levels": 10,
      "buffer_size": 500,
      "flush_interval": 3
    }
  }
}
```

## 📈 預期數據量

根據市場活躍度，各交易所的orderbook更新頻率：

- **幣安現貨BTCUSDT**: ~100-500 updates/秒
- **幣安合約BTCUSDT**: ~200-800 updates/秒  
- **Lighter合約**: 根據市場活躍度變化

**存儲估算**:
- 每條記錄約 1-2KB (壓縮後)
- BTCUSDT 24小時約 50-200MB
- 多交易對 24小時約 500MB-2GB

## 🔧 高級功能

### 1. 性能調優

```python
# 調整緩衝區大小
BUFFER_SIZE = 2000  # 增加緩衝區

# 調整刷新間隔
FLUSH_INTERVAL = 3  # 更頻繁刷新

# 內存限制
MEMORY_LIMIT_MB = 1024  # 增加內存限制
```

### 2. 實時監控

程序運行時會顯示：
- 實時orderbook數據 (最佳買賣價、價差、深度)
- 接收頻率統計
- 內存和CPU使用情況
- 緩衝區狀態

### 3. 錯誤處理

- 自動重連機制
- 數據完整性檢查
- 異常日誌記錄
- 優雅停機處理

## 📋 使用命令參考

### orderbook_recorder.py (統一系統)

```bash
# 基本參數
--config          # 配置文件路徑
--mode           # 運行模式: realtime, test
--duration       # 錄製時長(小時)
--symbols        # 交易對列表
--exchanges      # 啟用的交易所

# 示例
python orderbook_recorder.py \
  --exchanges binance_spot binance_futures \
  --symbols BTCUSDT ETHUSDT \
  --duration 12 \
  --config my_config.json
```

### 各交易所單獨程序

```bash
# binance_spot_orderbook.py
--symbol         # 交易對符號 (默認: BTCUSDT)
--depth          # 顯示深度級數 (默認: 10)
--record         # 啟用數據錄製

# binance_futures_orderbook.py
--symbol         # 交易對符號 (默認: BTCUSDT)  
--depth          # 顯示深度級數 (默認: 10)
--record         # 啟用數據錄製

# ws_async.py (Lighter)
--markets        # Market ID列表 (默認: [1])
--accounts       # Account ID列表 (默認: [])
--record         # 啟用數據錄製
```

## ⚠️ 注意事項

1. **網絡要求**: 穩定的網絡連接，建議使用VPS
2. **存儲空間**: 高頻數據量較大，確保足夠的磁盤空間
3. **內存管理**: 長時間運行建議設置內存限制
4. **API限制**: 遵守各交易所的API使用限制
5. **數據備份**: 定期備份重要的錄製數據

## 🛠️ 故障排除

### 常見問題

**1. WebSocket連接失敗**
```bash
# 檢查網絡連接
ping stream.binance.com

# 檢查防火牆設置
# 確保443端口可訪問
```

**2. 內存使用過高**
```python
# 減少緩衝區大小
buffer_size = 500

# 增加刷新頻率
flush_interval = 2
```

**3. 數據丟失**
```python
# 檢查序列號連續性
# 啟用詳細日誌記錄
logging.basicConfig(level=logging.DEBUG)
```

## 📞 聯系支持

如有問題或建議，請：
- 提交GitHub Issue
- 查看日誌文件: `orderbook_recorder.log`
- 檢查系統資源使用情況

---

**🎯 提示**: 建議從單一交易對開始測試，確認系統穩定後再擴展到多交易對多交易所錄製。 