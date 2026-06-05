export interface Env {
  RELAY_GATEWAY_DO: DurableObjectNamespace;
  RELAY_SHARED_TOKEN?: string;
  MAX_RELAY_PAYLOAD_CHARS?: string;
  MAX_ROOM_PEERS?: string;
  HEARTBEAT_TIMEOUT_MS?: string;
  HEARTBEAT_SWEEP_INTERVAL_MS?: string;
  MIN_PONG_INTERVAL_MS?: string;
  ENABLE_INFO_LOG?: string;
}

type JoinMsg = {
  type: "join";
  room: string;
  from: string; // peerId
  token?: string;
};

type RelayMsg = {
  type: "relay";
  room: string;
  from: string;
  to: string; // peerId or "*"
  payload: string; // base64
};

type LeaveMsg = {
  type: "leave";
  room: string;
  from: string;
};

type PingMsg = {
  type: "ping";
  room: string;
  from: string;
  ts?: number;
};

type ClientMsg = JoinMsg | RelayMsg | LeaveMsg | PingMsg;

type ServerMsg =
  | { type: "joined"; room: string; from: string; peers: string[] }
  | { type: "peer-joined"; room: string; from: string }
  | { type: "peer-left"; room: string; from: string }
  | { type: "relay"; room: string; from: string; to: string; payload: string }
  | { type: "pong"; room: string; from: string; ts?: number }
  | { type: "error"; code: string };

type PeerConn = {
  room: string;
  peerId: string;
  ws: WebSocket;
  lastSeen: number;
  lastPongAt: number;
};

function safeJsonParse<T>(raw: string): T | null {
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function getConfig(env: Env) {
  return {
    maxRelayPayloadChars: parseInt(env.MAX_RELAY_PAYLOAD_CHARS || "") || 2 * 1024 * 1024,
    maxRoomPeers: parseInt(env.MAX_ROOM_PEERS || "") || 8,
    heartbeatTimeoutMs: parseInt(env.HEARTBEAT_TIMEOUT_MS || "") || 60_000,
    heartbeatSweepIntervalMs: parseInt(env.HEARTBEAT_SWEEP_INTERVAL_MS || "") || 20_000,
    minPongIntervalMs: parseInt(env.MIN_PONG_INTERVAL_MS || "") || 15_000,
    enableInfoLog: env.ENABLE_INFO_LOG === "true",
  };
}

type Config = ReturnType<typeof getConfig>;

function sendJson(ws: WebSocket, data: ServerMsg | Record<string, unknown>): boolean {
  try {
    ws.send(JSON.stringify(data));
    return true;
  } catch (e) {
    logWarn("ws send failed", { error: String((e as any)?.message ?? e) });
    return false;
  }
}

function logWarn(msg: string, extra?: Record<string, unknown>) {
  console.warn(`[relay] ${msg}`, extra ?? {});
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      const config = getConfig(env);
      const health = {
        status: "ok",
        time: new Date().toISOString(),
        config,
        // Cloudflare Workers 特有属性，显示当前数据中心代码 (如 HKG, SJC)
        colo: (request as any).cf?.colo || "unknown",
      };
      return new Response(JSON.stringify(health, null, 2), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }

    if (url.pathname !== "/relay") {
      return new Response("Not Found", { status: 404 });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      logWarn("non-websocket request", { path: url.pathname });
      return new Response("Expected websocket", { status: 426 });
    }

    // 改进：根据 URL 中的 room 参数或特定 Header 路由到不同的 DO 实例
    // 这实现了房间级别的隔离，单个房间的压力不会影响全球其他房间
    const roomName = url.searchParams.get("room") || "default";
    const id = env.RELAY_GATEWAY_DO.idFromName(roomName);
    const stub = env.RELAY_GATEWAY_DO.get(id);
    return stub.fetch(request);
  }
};

export class RelayGatewayDO {
  private peers = new Map<WebSocket, PeerConn>();
  private peerIdToWs = new Map<string, WebSocket>();
  private config: Config;

  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env
  ) {
    this.config = getConfig(env);
  }

  private logInfo(msg: string, extra?: Record<string, unknown>) {
    if (!this.config.enableInfoLog) return;
    console.log(`[relay] ${msg}`, extra ?? {});
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      // 在 accept 之前设置好监听器是更稳妥的做法
      server.addEventListener("message", (evt) => {
        try {
          let data: string;
          if (typeof evt.data === 'string') {
            data = evt.data;
          } else if (evt.data instanceof ArrayBuffer) {
            data = new TextDecoder().decode(evt.data);
          } else {
            data = String(evt.data ?? "");
          }
          void this.onMessage(server, data);
        } catch (e) {
          logWarn("message decode error", { error: String(e) });
        }
      });

      server.addEventListener("close", (evt) => {
        this.logInfo("socket closed", { code: evt.code, reason: evt.reason });
        this.cleanup(server);
      });
      
      server.addEventListener("error", (evt) => {
        logWarn("socket error", { error: (evt as any).message });
        this.cleanup(server);
      });

      server.accept();

      return new Response(null, { status: 101, webSocket: client });
    } catch (e) {
      logWarn("DO fetch error", { error: String((e as any)?.message ?? e) });
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // 使用 Durable Object Alarms 替代 setInterval
  // 即使 DO 被休眠，Cloudflare 也会准时唤醒执行清理，更加可靠且节省资源
  async alarm() {
    this.sweepStalePeers();
    if (this.peers.size > 0) {
      await this.state.storage.setAlarm(Date.now() + this.config.heartbeatSweepIntervalMs);
    }
  }

  private async onMessage(ws: WebSocket, raw: string): Promise<void> {
    try {
      const msg = safeJsonParse<ClientMsg>(raw);

      const existing = this.peers.get(ws);
      if (existing) {
        existing.lastSeen = Date.now();
        this.peers.set(ws, existing);
      }
      if (!msg || typeof msg !== "object" || !("type" in msg)) {
        logWarn("bad json", { rawPreview: raw.slice(0, 120) });
        sendJson(ws, { type: "error", code: "BAD_JSON" });
        return;
      }

      if (msg.type === "join") {
        const { room, from, token } = msg;
        this.logInfo("join received", { room, from });
        if (!room || !from) {
          logWarn("join rejected: bad join", { room, from });
          sendJson(ws, { type: "error", code: "BAD_JOIN" });
          return;
        }

        const required = this.env.RELAY_SHARED_TOKEN || "";
        if (required && token !== required) {
          logWarn("join rejected: unauthorized", { room, from });
          sendJson(ws, { type: "error", code: "UNAUTHORIZED" });
          ws.close(1008, "unauthorized");
          return;
        }

        // 若同 peerId 已存在，踢掉旧连接
        const old = this.peerIdToWs.get(from);
        if (old && old !== ws) {
          try {
            sendJson(old, { type: "error", code: "REPLACED" });
            old.close(1000, "replaced");
          } catch { }
          this.cleanup(old);
        }

        if (!this.peerIdToWs.has(from) && this.peerIdToWs.size >= this.config.maxRoomPeers) {
          sendJson(ws, { type: "error", code: "ROOM_FULL" });
          ws.close(1008, "room full");
          return;
        }

        const existingPeers = Array.from(this.peerIdToWs.keys()).filter((peerId) => peerId !== from);

        this.ensureHeartbeatTimer();
        this.peers.set(ws, { room, peerId: from, ws, lastSeen: Date.now(), lastPongAt: 0 });
        this.peerIdToWs.set(from, ws);

        sendJson(ws, { type: "joined", room, from, peers: existingPeers });
        this.logInfo("joined", { room, from, peers: existingPeers.length });

        const staleTargets: WebSocket[] = [];
        for (const [targetWs, targetPeer] of this.peers.entries()) {
          if (targetPeer.peerId === from || targetPeer.room !== room) continue;
          if (!sendJson(targetWs, { type: "peer-joined", room, from })) {
            staleTargets.push(targetWs);
          }
        }
        for (const targetWs of staleTargets) {
          try {
            targetWs.close(1011, "send failed");
          } catch { }
          this.cleanup(targetWs);
        }
        this.logInfo("peer-joined broadcast", { room, from, notified: Math.max(this.peerIdToWs.size - 1, 0) });
        return;
      }

      if (msg.type === "leave") {
        const leaving = this.peers.get(ws);
        this.logInfo("leave received", { room: leaving?.room, from: leaving?.peerId });
        this.cleanup(ws);
        try {
          ws.close(1000, "leave");
        } catch { }
        return;
      }

      if (msg.type === "ping") {
        const sender = this.peers.get(ws);
        if (!sender) {
          sendJson(ws, { type: "error", code: "NOT_JOINED" });
          return;
        }
        if (sender.room !== msg.room || sender.peerId !== msg.from) {
          sendJson(ws, { type: "error", code: "SENDER_MISMATCH" });
          return;
        }
        const now = Date.now();
        if (now - sender.lastPongAt >= this.config.minPongIntervalMs) {
          sender.lastPongAt = now;
          this.peers.set(ws, sender);
          sendJson(ws, { type: "pong", room: sender.room, from: sender.peerId, ts: msg.ts });
        }
        return;
      }

      if (msg.type === "relay") {
        const sender = this.peers.get(ws);
        if (!sender) {
          logWarn("relay rejected: not joined");
          sendJson(ws, { type: "error", code: "NOT_JOINED" });
          return;
        }

        const { room, from, to, payload } = msg;
        if (!room || !from || !to || typeof payload !== "string") {
          sendJson(ws, { type: "error", code: "BAD_RELAY" });
          return;
        }
        if (payload.length > this.config.maxRelayPayloadChars) {
          sendJson(ws, { type: "error", code: "PAYLOAD_TOO_LARGE" });
          return;
        }

        if (sender.room !== room || sender.peerId !== from) {
          logWarn("relay rejected: sender mismatch", { senderRoom: sender.room, room, senderPeerId: sender.peerId, from });
          sendJson(ws, { type: "error", code: "SENDER_MISMATCH" });
          return;
        }

        if (to === "*") {
          let fanout = 0;
          const staleTargets: WebSocket[] = [];
          for (const [targetWs, targetPeer] of this.peers.entries()) {
            if (targetPeer.peerId === from || targetPeer.room !== room) continue;
            if (sendJson(targetWs, { type: "relay", room, from, to: targetPeer.peerId, payload })) {
              fanout++;
            } else {
              staleTargets.push(targetWs);
            }
          }
          for (const targetWs of staleTargets) {
            try {
              targetWs.close(1011, "send failed");
            } catch { }
            this.cleanup(targetWs);
          }
          this.logInfo("relay broadcast", { room, from, fanout, payloadChars: payload.length });
        } else {
          const target = this.peerIdToWs.get(to);
          if (target) {
            const targetPeer = this.peers.get(target);
            if (targetPeer && targetPeer.room === room) {
              if (!sendJson(target, { type: "relay", room, from, to, payload })) {
                try {
                  target.close(1011, "send failed");
                } catch { }
                this.cleanup(target);
              } else {
                this.logInfo("relay unicast", { room, from, to, payloadChars: payload.length });
              }
            } else {
              logWarn("relay rejected: target in different room", { room, to });
            }
          } else {
            logWarn("relay target not found", { room, from, to });
          }
        }
        return;
      }
    } catch (e) {
      logWarn("onMessage crash", { error: String((e as any)?.message ?? e) });
    }
  }

  private cleanup(ws: WebSocket): void {
    const peer = this.peers.get(ws);
    if (!peer) {
      // 如果还没 join 就断开了，也要确保从内存中移除（虽然此时可能不在 peers 里）
      this.peers.delete(ws);
      return;
    }

    this.peers.delete(ws);
    this.peerIdToWs.delete(peer.peerId);
    this.logInfo("cleanup", { room: peer.room, from: peer.peerId, remaining: this.peerIdToWs.size });

    const staleTargets: WebSocket[] = [];
    for (const [targetWs, targetPeer] of this.peers.entries()) {
      if (targetPeer.peerId === peer.peerId || targetPeer.room !== peer.room) continue;
      if (!sendJson(targetWs, { type: "peer-left", room: peer.room, from: peer.peerId })) {
        staleTargets.push(targetWs);
      }
    }
    
    // 批量处理失效的连接，通过 close 触发它们的 cleanup
    for (const targetWs of staleTargets) {
      try {
        targetWs.close(1011, "peer broadcast failed");
      } catch { }
      // 注意：这里不需要手动调用 this.cleanup(targetWs)，
      // 因为 targetWs 的 "close" 监听器会被触发并执行 cleanup。
    }
  }

  private async ensureHeartbeatTimer(): Promise<void> {
    const alarm = await this.state.storage.getAlarm();
    if (alarm === null) {
      await this.state.storage.setAlarm(Date.now() + this.config.heartbeatSweepIntervalMs);
    }
  }

  private sweepStalePeers(): void {
    const now = Date.now();
    for (const [ws, peer] of this.peers.entries()) {
      if (now - peer.lastSeen <= this.config.heartbeatTimeoutMs) continue;
      logWarn("heartbeat timeout", { room: peer.room, from: peer.peerId, lastSeen: peer.lastSeen });
      try {
        ws.close(1001, "heartbeat timeout");
      } catch { }
      this.cleanup(ws);
    }
  }
}