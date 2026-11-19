const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = Number(process.env.PORT || 3000);
const MAX_PLAYERS_PER_ROOM = 32;
const rooms = new Map();

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true, service: "DARK-ZONE multiplayer", rooms: rooms.size }));
  }
  if (req.url === "/" || req.url === "/index.html") {
    const gamePath = path.join(__dirname, "index.html");
    if (!fs.existsSync(gamePath)) {
      res.writeHead(503, { "content-type": "text/plain; charset=utf-8" });
      return res.end("DARK-ZONE game file is missing. Add index.html to the repository.");
    }
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" });
    return res.end(fs.readFileSync(gamePath));
  }
  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

function send(ws, packet) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(packet));
}
function broadcast(room, packet, except) {
  for (const player of room.values()) {
    if (player.ws !== except) send(player.ws, packet);
  }
}
function safeText(value, fallback, max = 16) {
  return typeof value === "string" ? value.trim().slice(0, max) || fallback : fallback;
}
function removePlayer(ws) {
  const roomId = ws.roomId;
  const clientId = ws.clientId;
  if (!roomId || !clientId) return;
  const room = rooms.get(roomId);
  if (room) {
    room.delete(clientId);
    broadcast(room, { type: "player_left", clientId });
    if (room.size === 0) rooms.delete(roomId);
    else broadcast(room, { type: "room_state", players: [...room.values()].map(({ ws: _ws, ...p }) => p) });
  }
  ws.roomId = null;
  ws.clientId = null;
}
function validRoomId(value) {
  return typeof value === "string" && /^[A-Za-z0-9_-]{2,40}$/.test(value);
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let packet;
    try { packet = JSON.parse(raw.toString()); }
    catch { return send(ws, { type: "error", message: "Invalid JSON packet." }); }
    if (!packet || typeof packet !== "object" || typeof packet.type !== "string") {
      return send(ws, { type: "error", message: "Invalid packet." });
    }

    if (packet.type === "join_room") {
      const roomId = packet.roomId;
      const clientId = safeText(packet.clientId, "", 64);
      if (!validRoomId(roomId) || !clientId) return send(ws, { type: "error", message: "A valid room and player ID are required." });
      removePlayer(ws);
      let room = rooms.get(roomId);
      if (!room) { room = new Map(); rooms.set(roomId, room); }
      if (room.size >= MAX_PLAYERS_PER_ROOM) return send(ws, { type: "error", message: "Room is full." });

      const player = {
        clientId,
        name: safeText(packet.name, "PLAYER"),
        status: "IN ROOM",
        mode: safeText(packet.mode, "SOLO", 12),
        map: safeText(packet.map, "DARK ISLAND", 24),
        position: null,
        ws
      };
      room.set(clientId, player);
      ws.roomId = roomId;
      ws.clientId = clientId;
      send(ws, { type: "room_state", players: [...room.values()].map(({ ws: _ws, ...p }) => p) });
      broadcast(room, { type: "player_joined", player: { clientId, name: player.name, status: player.status, position: null } }, ws);
      return;
    }

    if (packet.type === "leave_room") {
      removePlayer(ws);
      return;
    }

    if (packet.type === "player_state") {
      if (!ws.roomId || !ws.clientId) return send(ws, { type: "error", message: "Join a room before sending player state." });
      const room = rooms.get(ws.roomId);
      const player = room && room.get(ws.clientId);
      if (!player) return;
      const pos = packet.position;
      if (pos && [pos.x, pos.y, pos.z, pos.rotation].every(v => Number.isFinite(Number(v)))) {
        player.position = {
          x: Math.max(-10000, Math.min(10000, Number(pos.x))),
          y: Math.max(-1000, Math.min(1000, Number(pos.y))),
          z: Math.max(-10000, Math.min(10000, Number(pos.z))),
          rotation: Number(pos.rotation)
        };
      }
      player.name = safeText(packet.name, player.name);
      player.status = "IN MATCH";
      broadcast(room, {
        type: "player_state",
        clientId: player.clientId,
        name: player.name,
        status: player.status,
        position: player.position
      }, ws);
    }
  });

  ws.on("close", () => removePlayer(ws));
  ws.on("error", () => removePlayer(ws));
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { removePlayer(ws); ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
wss.on("close", () => clearInterval(heartbeat));

server.listen(PORT, "0.0.0.0", () => {
  console.log(`DARK-ZONE multiplayer server listening on port ${PORT}`);
});
