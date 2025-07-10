# OrderbookRecorder

[中文文檔](README.cn.md)

A high-performance orderbook data recorder for cryptocurrency exchanges, supporting multiple venues including Binance Spot, Binance Futures, and Lighter.

## ✨ Core Features

- 🚀 **Real-time Recording**: Captures every orderbook update, not just periodic snapshots
- 🌐 **Multi-Exchange Support**: Binance Spot, Binance Futures, and Lighter
- 💾 **Efficient Storage**: Parquet format with snappy compression, hourly file rotation
- 📊 **Live Monitoring**: Beautiful tabulate format display with real-time statistics
- 🔧 **Flexible Configuration**: JSON config file with command-line parameter override
- 🛡️ **Reliability**: Auto-reconnection, error handling, buffer management
- ⚡ **High Performance**: Async processing, memory management, batch writing

## 📦 Installation

1. Clone the repository:
```bash
git clone https://github.com/zhiyu1105/OrderbookRecorder.git
cd OrderbookRecorder
```

2. Install dependencies:
```bash
pip install -r requirements.txt

# If using Lighter, additional installation required
# pip install lighter  # or install from source
```

## 🚀 Quick Start

### 1. Run Individual Exchange Collectors

**Binance Spot Orderbook:**
```bash
# Basic usage
python binance_spot_orderbook.py

# Specify symbol and depth
python binance_spot_orderbook.py --symbol ETHUSDT --depth 20

# Enable recording
python binance_spot_orderbook.py --symbol BTCUSDT --record
```

**Binance Futures Orderbook:**
```bash
# Basic usage
python binance_futures_orderbook.py

# With parameters and recording
python binance_futures_orderbook.py --symbol ETHUSDT --depth 15 --record
```

**Lighter Orderbook:**
```bash
# Basic usage
python lighter_orderbook.py

# Specify markets and record
python lighter_orderbook.py --markets 1 2 --record
```

### 2. Unified Recording System

**Real-time Recording (Recommended):**
```bash
# Record for 24 hours with default config
python orderbook_recorder.py --mode realtime --duration 24

# Specify symbols
python orderbook_recorder.py --symbols BTCUSDT ETHUSDT --duration 12

# Record specific exchanges only
python orderbook_recorder.py --exchanges binance_spot binance_futures --duration 6

# Use custom config file
python orderbook_recorder.py --config my_config.json --duration 48
```

## 📊 Data Format

The recorded orderbook data contains the following fields:

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

## 📁 File Structure

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

## ⚙️ Configuration

Create a `config.json` file to customize settings:

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

## 📈 Expected Data Volume

Update frequency by exchange based on market activity:

- **Binance Spot BTCUSDT**: ~100-500 updates/sec
- **Binance Futures BTCUSDT**: ~200-800 updates/sec
- **Lighter**: Varies by market activity

**Storage Estimation**:
- ~1-2KB per record (compressed)
- 50-200MB per 24h for BTCUSDT
- 500MB-2GB per 24h for multiple symbols

## 🔧 Advanced Features

### 1. Performance Tuning

```python
# Adjust buffer size
BUFFER_SIZE = 2000  # Increase buffer

# Adjust flush interval
FLUSH_INTERVAL = 3  # More frequent flush

# Memory limit
MEMORY_LIMIT_MB = 1024  # Increase memory limit
```

### 2. Live Monitoring

The program displays:
- Real-time orderbook data (best bid/ask, spread, depth)
- Reception rate statistics
- Memory and CPU usage
- Buffer status

### 3. Error Handling

- Automatic reconnection
- Data integrity checks
- Exception logging
- Graceful shutdown

## 📋 Command Reference

### orderbook_recorder.py (Unified System)

```bash
# Basic parameters
--config          # Config file path
--mode           # Run mode: realtime, test
--duration       # Recording duration (hours)
--symbols        # Symbol list
--exchanges      # Enabled exchanges

# Example
python orderbook_recorder.py \
  --exchanges binance_spot binance_futures \
  --symbols BTCUSDT ETHUSDT \
  --duration 12 \
  --config my_config.json
```

### Individual Exchange Programs

```bash
# binance_spot_orderbook.py
--symbol         # Trading symbol (default: BTCUSDT)
--depth          # Display depth levels (default: 10)
--record         # Enable data recording

# binance_futures_orderbook.py
--symbol         # Trading symbol (default: BTCUSDT)
--depth          # Display depth levels (default: 10)
--record         # Enable data recording

# ws_async.py (Lighter)
--markets        # Market ID list (default: [1])
--accounts       # Account ID list (default: [])
--record         # Enable data recording
```

## ⚠️ Important Notes

1. **Network Requirements**: Stable connection, VPS recommended
2. **Storage Space**: High-frequency data requires sufficient disk space
3. **Memory Management**: Set memory limits for long-running sessions
4. **API Limits**: Comply with exchange API restrictions
5. **Data Backup**: Regularly backup important recorded data

## 🛠️ Troubleshooting

### Common Issues

**1. WebSocket Connection Failure**
```bash
# Check network connection
ping stream.binance.com

# Check firewall settings
```

## 📝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details. 