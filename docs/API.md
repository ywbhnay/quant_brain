# 量化中枢系统 — REST API 接口文档

> Base URL: `http://<host>:8000`
> 所有接口返回 `application/json`
> 行情数据返回格式：紧凑数组 `[timestamp, open, high, low, close, vol, amount]`

---

## 一、行情数据接口 (`/api/market/*`)

### 1.1 获取日线数据

```
GET /api/market/daily/{ts_code}
```

获取指定股票的日线 OHLCV 数据，支持日期范围和复权因子。

**路径参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `ts_code` | string | 是 | 股票代码，如 `000001.SZ` |

**查询参数**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `start_date` | string | 无 | 开始日期，格式 `YYYYMMDD` |
| `end_date` | string | 无 | 结束日期，格式 `YYYYMMDD` |
| `with_adj` | boolean | `false` | 是否返回复权因子 |

**响应示例**

```json
{
  "ts_code": "000001.SZ",
  "bars": [
    ["2024-01-15", 12.50, 12.80, 12.40, 12.70, 500000.0, 6350000.0],
    ["2024-01-16", 12.70, 13.00, 12.60, 12.90, 600000.0, 7740000.0],
    ["2024-01-17", 12.90, 13.10, 12.80, 13.00, 450000.0, 5850000.0]
  ],
  "adj_factors": [1.05, 1.05, 1.05]
}
```

**bars 数组字段说明**

| 索引 | 字段 | 类型 |
|------|------|------|
| 0 | timestamp | string (ISO 8601) |
| 1 | open | float |
| 2 | high | float |
| 3 | low | float |
| 4 | close | float |
| 5 | vol | float (成交量，手) |
| 6 | amount | float (成交额，元) |

**cURL 示例**

```bash
# 获取全部日线
curl "http://localhost:8000/api/market/daily/000001.SZ"

# 获取指定日期范围
curl "http://localhost:8000/api/market/daily/000001.SZ?start_date=20240101&end_date=20240131"

# 带复权因子
curl "http://localhost:8000/api/market/daily/000001.SZ?with_adj=true"
```

---

### 1.2 获取分钟线数据

```
GET /api/market/minute/{ts_code}
```

获取指定股票的分钟级 K 线数据。

**路径参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `ts_code` | string | 是 | 股票代码，如 `000001.SZ` |

**查询参数**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `start_time` | string | 无 | 开始时间，格式 `YYYY-MM-DD HH:MM:SS` |
| `end_time` | string | 无 | 结束时间，格式 `YYYY-MM-DD HH:MM:SS` |
| `limit` | integer | `1000` | 返回条数上限 (1-5000) |

**响应示例**

```json
{
  "ts_code": "000001.SZ",
  "bars": [
    ["2024-01-15T09:30:00", 12.50, 12.55, 12.48, 12.52, 5000.0, 62500.0],
    ["2024-01-15T09:31:00", 12.52, 12.58, 12.50, 12.56, 3000.0, 37680.0],
    ["2024-01-15T09:32:00", 12.56, 12.60, 12.55, 12.58, 4000.0, 50320.0]
  ]
}
```

**cURL 示例**

```bash
# 获取最近 1000 条分钟线
curl "http://localhost:8000/api/market/minute/000001.SZ"

# 获取指定时间范围
curl "http://localhost:8000/api/market/minute/000001.SZ?start_time=2024-01-15%2009:30:00&end_time=2024-01-15%2015:00:00"

# 限制返回 500 条
curl "http://localhost:8000/api/market/minute/000001.SZ?limit=500"
```

---

### 1.3 搜索股票列表

```
GET /api/market/stocks
```

搜索股票列表，支持代码和名称模糊搜索。

**查询参数**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `keyword` | string | `""` | 搜索关键词 (支持代码/名称模糊匹配) |
| `limit` | integer | `50` | 返回条数上限 (1-200) |

**响应示例**

```json
{
  "stocks": [
    {"ts_code": "000001.SZ", "name": "平安银行"},
    {"ts_code": "000002.SZ", "name": "万科A"},
    {"ts_code": "000004.SZ", "name": "国华网安"}
  ]
}
```

**cURL 示例**

```bash
# 获取全部股票列表
curl "http://localhost:8000/api/market/stocks"

# 搜索关键词
curl "http://localhost:8000/api/market/stocks?keyword=平安"

# 搜索股票代码
curl "http://localhost:8000/api/market/stocks?keyword=000001"
```

---

## 二、交易指令接口 (`/api/trading/*`)

### 2.1 提交交易指令

```
POST /api/trading/order
```

提交一笔新的交易指令。订单初始状态为 `PENDING`，写入 PostgreSQL 后由量化引擎消费。

**请求体 (JSON)**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `ts_code` | string | 是 | 股票代码，如 `000001.SZ` |
| `price` | float | 是 | 委托价格 |
| `volume` | integer | 是 | 委托数量 (股) |
| `direction` | string | 是 | 方向：`BUY` (买入) 或 `SELL` (卖出) |
| `order_type` | string | 否 | 订单类型：`LIMIT` (限价) 或 `MARKET` (市价)，默认 `LIMIT` |

**请求示例**

```json
{
  "ts_code": "000001.SZ",
  "price": 12.50,
  "volume": 1000,
  "direction": "BUY",
  "order_type": "LIMIT"
}
```

**响应示例**

```json
{
  "order_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "PENDING"
}
```

**cURL 示例**

```bash
curl -X POST "http://localhost:8000/api/trading/order" \
  -H "Content-Type: application/json" \
  -d '{
    "ts_code": "000001.SZ",
    "price": 12.50,
    "volume": 1000,
    "direction": "BUY",
    "order_type": "LIMIT"
  }'
```

---

### 2.2 撤单

```
POST /api/trading/cancel
```

撤销一笔活跃订单。仅当订单状态为 `PENDING`、`SENT`、`ACK`、`PARTIAL` 时可撤销。

**请求体 (JSON)**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `order_id` | string | 是 | 订单 UUID |

**请求示例**

```json
{
  "order_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

**响应示例**

```json
{
  "order_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "CANCELLED"
}
```

**cURL 示例**

```bash
curl -X POST "http://localhost:8000/api/trading/cancel" \
  -H "Content-Type: application/json" \
  -d '{"order_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}'
```

---

### 2.3 查询订单状态

```
GET /api/trading/order/{order_id}
```

查询指定订单的详细信息。

**路径参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `order_id` | string | 是 | 订单 UUID |

**响应示例**

```json
{
  "order_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "ts_code": "000001.SZ",
  "direction": "BUY",
  "price": 12.50,
  "volume": 1000,
  "status": "FILLED",
  "created_at": "2024-01-15T09:30:00+08:00"
}
```

**订单状态说明**

| 状态 | 说明 |
|------|------|
| `PENDING` | 已提交，等待处理 |
| `SENT` | 已发送至交易网关 |
| `ACK` | 交易网关已确认 |
| `FILLED` | 全部成交 |
| `PARTIAL` | 部分成交 |
| `REJECTED` | 被拒绝 (风控/系统错误) |
| `CANCELLED` | 已撤销 |

**cURL 示例**

```bash
curl "http://localhost:8000/api/trading/order/a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

---

## 三、账户接口 (`/api/account/*`)

### 3.1 查询账户总资产

```
GET /api/account
```

返回账户资金概况，包括现金、总资产和持仓市值。

**响应示例**

```json
{
  "cash": 500000.00,
  "total_assets": 1250000.00,
  "market_value": 750000.00
}
```

**字段说明**

| 字段 | 类型 | 说明 |
|------|------|------|
| `cash` | float | 可用资金 (元) |
| `total_assets` | float | 总资产 = cash + market_value |
| `market_value` | float | 持仓市值 (元) |

**cURL 示例**

```bash
curl "http://localhost:8000/api/account"
```

---

### 3.2 查询持仓列表

```
GET /api/account/positions
```

返回当前持仓的股票列表及明细。

**响应示例**

```json
{
  "positions": [
    {
      "ts_code": "000001.SZ",
      "volume": 5000,
      "available_volume": 3000,
      "cost_price": 12.00,
      "market_price": 12.50,
      "market_value": 62500.00
    },
    {
      "ts_code": "600519.SH",
      "volume": 200,
      "available_volume": 200,
      "cost_price": 1800.00,
      "market_price": 1850.00,
      "market_value": 370000.00
    }
  ]
}
```

**字段说明**

| 字段 | 类型 | 说明 |
|------|------|------|
| `ts_code` | string | 股票代码 |
| `volume` | integer | 持仓总量 (股) |
| `available_volume` | integer | 可用数量 (股，不含今日买入) |
| `cost_price` | float | 成本价 (元) |
| `market_price` | float | 当前市价 (元) |
| `market_value` | float | 持仓市值 (元) |

**cURL 示例**

```bash
curl "http://localhost:8000/api/account/positions"
```

---

## 四、错误码参考

| HTTP 状态码 | 含义 | 常见场景 |
|-------------|------|---------|
| `400` | Bad Request | 参数校验失败、direction 不合法、订单不可撤销 |
| `404` | Not Found | 订单不存在、账户信息不存在 |
| `409` | Conflict | 订单已存在 (UUID 重复) |
| `500` | Internal Server Error | 数据库连接失败、未知异常 |

**错误响应格式**

```json
{
  "detail": "具体错误信息"
}
```

**常见错误信息**

| 错误信息 | 原因 | 解决方案 |
|---------|------|---------|
| `direction 必须为 BUY 或 SELL` | direction 参数不合法 | 使用 `BUY` 或 `SELL` |
| `order_type 必须为 LIMIT 或 MARKET` | order_type 参数不合法 | 使用 `LIMIT` 或 `MARKET` |
| `订单不存在` | order_id 错误 | 检查订单 UUID |
| `订单状态 FILLED 不可撤销` | 订单已终态 | 只能撤销活跃订单 |
| `订单已存在` | UUID 冲突 | 使用新的 UUID |
| `数据库错误: ...` | PostgreSQL 异常 | 检查数据库连接 |
| `账户信息不存在` | account_summary 表为空 | 初始化账户数据 |

---

## 五、接口总览

| 方法 | 路径 | 说明 | 认证 |
|------|------|------|------|
| `GET` | `/api/market/daily/{ts_code}` | 获取日线数据 | 无 |
| `GET` | `/api/market/minute/{ts_code}` | 获取分钟线数据 | 无 |
| `GET` | `/api/market/stocks` | 搜索股票列表 | 无 |
| `POST` | `/api/trading/order` | 提交交易指令 | 无 |
| `POST` | `/api/trading/cancel` | 撤单 | 无 |
| `GET` | `/api/trading/order/{order_id}` | 查询订单状态 | 无 |
| `GET` | `/api/account` | 查询账户总资产 | 无 |
| `GET` | `/api/account/positions` | 查询持仓列表 | 无 |

> **注意**：当前版本未启用认证。生产环境部署时建议添加 API Key 或 JWT 认证。

---

## 六、前端复权计算

后端返回的日线数据中，`with_adj=true` 时额外返回 `adj_factors` 数组。前端需自行计算复权价格：

```javascript
// 前复权计算
function forwardAdjust(bars, adjFactors) {
  if (!adjFactors) return bars;
  const latestFactor = adjFactors[adjFactors.length - 1];
  return bars.map((bar, i) => {
    const ratio = adjFactors[i] / latestFactor;
    return [
      bar[0],                  // timestamp
      bar[1] * ratio,          // open
      bar[2] * ratio,          // high
      bar[3] * ratio,          // low
      bar[4] * ratio,          // close
      bar[5],                  // vol (不变)
      bar[6],                  // amount (不变)
    ];
  });
}
```

复权只影响 OHLC 价格列，成交量和成交额不变。
