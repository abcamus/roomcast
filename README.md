# RoomCast

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8-blue)](https://www.typescriptlang.org/)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-F38020)](https://workers.cloudflare.com/)

> 基于 Cloudflare Workers + Durable Objects 的轻量级 WebSocket 房间中继服务,
> 按“房间”维度在多个客户端之间转发任意 payload,适用于多端实时同步、协作白板、
> 实时通知等场景。

## ✨ 特性

- 🚀 **零运维**:全球边缘节点自动调度,免费额度即可使用
- 🏠 **房间隔离**:每房间独立 Durable Object,故障与压力天然不互串
- 💓 **心跳保活**:基于 DO Alarm(非 `setInterval`),DO 休眠也能被准时唤醒
- 🔐 **可选鉴权**:`ROOMHUB_TOKEN` 共享 token
- 📦 **资源限制**:可配置房间人数 / 消息大小 / 报文上限
- 🔁 **单播 + 广播**:`to: "*"` 房间内广播,`to: <peerId>` 单播
- 📊 **健康检查**:`GET /health` 返回 `colo` / `config` / 时间戳

## 🏗 架构

```
┌────────┐            ┌──────────────────┐
│ Client │ ── ws ──►  │ Worker           │
└────────┘            │  (路由 / 鉴权)    │
                      └────────┬─────────┘
                               │ idFromName(room)
                               ▼
                      ┌──────────────────┐
                      │ Durable Object   │
                      │ RoomHubDO        │
                      │  - peers Map     │
                      │  - alarm 清理    │
                      └──────────────────┘
```

每个 room 通过 `idFromName(roomName)` 路由到独立的 DO 实例,
单房间高并发不会污染其他房间;DO 内部用 `storage.setAlarm` 周期性
清理心跳超时的 peer,即使 DO 处于休眠态也能被 Cloudflare 准时唤醒。

## 🚀 部署

### 前置条件

- Node.js ≥ 18
- 一个 Cloudflare 账户(免费额度即可)

### 步骤

```sh
# 1. 登陆
npx wrangler login

# 2. (可选) 设置共享 token,用于 join 鉴权
npx wrangler secret put ROOMHUB_TOKEN

# 3. 部署
npm run deploy
```

### 本地开发

```sh
npm run dev
```

启动后默认监听 `http://127.0.0.1:8787`。

## ⚙️ 配置项

通过 `wrangler.toml` 的 `[vars]` 段(明文)或 `wrangler secret`(机密)配置:

| 环境变量 | 默认值 | 说明 |
|:---|:---:|:---|
| `ROOMHUB_TOKEN` | `""` | 可选,空字符串则不校验 join token |
| `MAX_RELAY_PAYLOAD_CHARS` | `2097152` (2MB) | 单条 relay 消息 payload 字符上限 |
| `MAX_ROOM_PEERS` | `8` | 单房间最大 peer 数 |
| `HEARTBEAT_TIMEOUT_MS` | `60000` | peer 多久未上报心跳判定为离线 |
| `HEARTBEAT_SWEEP_INTERVAL_MS` | `20000` | DO Alarm 触发清理的间隔 |
| `MIN_PONG_INTERVAL_MS` | `15000` | 服务端回 pong 的最小间隔,防心跳风暴 |
| `ENABLE_INFO_LOG` | `false` | 是否输出 info 级别日志 |

## 📡 协议

所有消息均为 UTF-8 JSON,单条 ≤ `MAX_RELAY_PAYLOAD_CHARS`。

### Client → Server

```ts
type ClientMsg =
  | { type: "join";   room: string; from: string; token?: string }
  | { type: "relay";  room: string; from: string; to: string; payload: string /* base64 */ }
  | { type: "leave";  room: string; from: string }
  | { type: "ping";   room: string; from: string; ts?: number };
```

### Server → Client

```ts
type ServerMsg =
  | { type: "joined";      room: string; from: string; peers: string[] }
  | { type: "peer-joined"; room: string; from: string }
  | { type: "peer-left";   room: string; from: string }
  | { type: "relay";       room: string; from: string; to: string; payload: string }
  | { type: "pong";        room: string; from: string; ts?: number }
  | { type: "error";       code: string };
```

### error code

| code | 含义 |
|:---|:---|
| `BAD_JSON` | 消息不是合法 JSON |
| `BAD_JOIN` / `BAD_RELAY` | 必填字段缺失 |
| `UNAUTHORIZED` | token 校验失败 |
| `ROOM_FULL` | 超过 `MAX_ROOM_PEERS` |
| `REPLACED` | 同 peerId 的旧连接被新连接顶替 |
| `PAYLOAD_TOO_LARGE` | payload 超过 `MAX_RELAY_PAYLOAD_CHARS` |
| `SENDER_MISMATCH` | `from` 与该 ws 注册的 peerId / room 不一致 |
| `NOT_JOINED` | 未 join 就发送 relay / ping |

### 连接流程

```
Client                              Server
  │  ──── Upgrade: websocket ────►  │
  │  ──── {type:"join"} ────────►   │
  │  ◄── {type:"joined",peers} ───  │
  │                                 │
  │  ──── {type:"relay",to:"*"} ─►  │  ── broadcast ─► 其它 peers
  │  ◄── {type:"relay",...} ──────  │
  │  ◄── {type:"peer-joined"} ────  │  (有新人加入)
  │                                 │
  │  ──── {type:"ping"} ────────►   │
  │  ◄── {type:"pong"} ──────────   │
  │                                 │
  │  ──── {type:"leave"} ───────►   │
  │  ──── ws.close() ───────────►   │
  │  ◄── {type:"peer-left"} ──────  │  (其它 peer 收到)
```

## 🩺 健康检查

```sh
curl https://<your-worker>.workers.dev/health
```

返回:

```json
{
  "status": "ok",
  "time": "2026-06-05T10:00:00.000Z",
  "config": {
    "maxRelayPayloadChars": 2097152,
    "maxRoomPeers": 8,
    "heartbeatTimeoutMs": 60000,
    "heartbeatSweepIntervalMs": 20000,
    "minPongIntervalMs": 15000,
    "enableInfoLog": false
  },
  "colo": "HKG"
}
```

## 🔌 客户端示例

Node.js(使用 `wscat`):

```sh
npm i -g wscat
wscat -c "wss://<your-worker>.workers.dev/ws?room=demo"
> {"type":"join","room":"demo","from":"alice"}
< {"type":"joined","room":"demo","from":"alice","peers":[]}
> {"type":"relay","room":"demo","from":"alice","to":"*","payload":"aGVsbG8="}
```

浏览器:

```ts
const ws = new WebSocket("wss://<your-worker>.workers.dev/ws?room=demo");
ws.onopen = () => ws.send(JSON.stringify({
  type: "join", room: "demo", from: "alice",
}));
ws.onmessage = (e) => console.log("recv:", e.data);
```

## 💰 Cloudflare 免费额度

参考: https://www.cloudflare-cn.com/plans/developer-platform-pricing/

| 指标 | 免费额度 |
|:---|:---:|
| 每日请求 | 100,000 |
| CPU 时间 | 10 ms / 每次请求 |
| WebSocket | 仅按消息计费,无连接时长费用 |

## 🤝 贡献

欢迎 PR!请确保:

1. `npx tsc --noEmit` 通过
2. 保持 TypeScript 严格模式
3. 新增配置项同步更新本文「配置项」表格
4. 新增 `error.code` 同步更新本文「error code」表格

## 📄 License

[Apache-2.0](LICENSE) © 2026 RoomHub Contributors

欢迎 PR!请确保:

1. `npx tsc --noEmit` 通过
2. 保持 TypeScript 严格模式
3. 新增配置项同步更新本文「配置项」表格
4. 新增 `error.code` 同步更新本文「error code」表格

## 📄 License

[Apache-2.0](LICENSE) © 2026 Sync Vault Contributors