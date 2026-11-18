// Pure Dark - team play relay + static file server.
// Setup:  npm install   then   node server.js   (PORT env var optional, default 3000)
// Open http://localhost:3000 on every device. Behind HTTPS (Render, Railway, Fly...) the page uses wss:// automatically.
// The server is a dumb room relay: it only stores each player's "presence" object and forwards changes to the others
// in the same room. All game and lobby logic runs in the browsers.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'index.html');
const MAX_ROOM = 8;

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  fs.readFile(INDEX, (err, data) => {
    if (err) { res.writeHead(500); return res.end('index.html missing'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 8 * 1024 });
const rooms = new Map();   // room name -> Map(ws -> { id, presence })
let counter = 0;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const okKey = (k) => /^[A-Za-z_][A-Za-z0-9_]{0,31}$/.test(k) && k !== '__proto__' && k !== 'constructor' && k !== 'prototype';

function leave(ws) {
  const room = rooms.get(ws.room);
  if (!room) return;
  const me = room.get(ws);
  room.delete(ws);
  if (room.size === 0) rooms.delete(ws.room);
  else if (me) room.forEach((_, other) => send(other, { t: 'l', id: me.id }));
  ws.room = null;
}

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.room = null; ws.count = 0;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    if (++ws.count > 80) return;                       // flood guard: max 80 msgs/sec
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'join') {
      if (ws.room) return;
      const name = String(m.room || '');
      if (!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(name)) return ws.close();
      let room = rooms.get(name);
      if (!room) { room = new Map(); rooms.set(name, room); }
      if (room.size >= MAX_ROOM) { send(ws, { t: 'err', msg: 'Room is full' }); return ws.close(); }
      const me = { id: 'p' + (++counter).toString(36), presence: {} };
      room.set(ws, me); ws.room = name;
      send(ws, { t: 'snap', me: me.id, peers: [...room.values()].map((p) => ({ peer: p.id, presence: p.presence })) });
      room.forEach((_, other) => { if (other !== ws) send(other, { t: 'u', id: me.id, p: me.presence }); });
    } else if (m.t === 'pres') {
      const room = rooms.get(ws.room);
      const me = room && room.get(ws);
      if (!me || !m.p || typeof m.p !== 'object' || Array.isArray(m.p)) return;
      const next = Object.assign({}, me.presence);
      for (const k of Object.keys(m.p)) {
        if (!okKey(k)) continue;
        if (m.p[k] === null) delete next[k]; else next[k] = m.p[k];
      }
      if (JSON.stringify(next).length > 4096) return;  // presence stays small
      me.presence = next;
      room.forEach((_, other) => { if (other !== ws) send(other, { t: 'u', id: me.id, p: next }); });
    }
  });
  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

setInterval(() => wss.clients.forEach((ws) => { ws.count = 0; }), 1000);          // reset flood counter
setInterval(() => wss.clients.forEach((ws) => {                                   // drop dead connections
  if (!ws.isAlive) return ws.terminate();
  ws.isAlive = false; ws.ping();
}), 30000);

server.listen(PORT, () => console.log('Pure Dark server on http://localhost:' + PORT));
