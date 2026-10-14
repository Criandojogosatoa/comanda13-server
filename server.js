// Servidor de salas do Comanda 13 (relay WebSocket, sem dependências).
// Uso: node server.js   (porta via variável PORT, padrão 8080)
const http = require('http'), crypto = require('crypto');
const PORT = process.env.PORT || 8080, MAX = 4;
const rooms = new Map(); // code -> {code, players: Map(id -> client)}
let nextId = 1;

const srv = http.createServer((q, r) => { r.writeHead(200); r.end('Comanda13 server OK'); });

srv.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) return sock.destroy();
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  const c = { sock, id: nextId++, name: 'Jogador', room: null, buf: Buffer.alloc(0), last: Date.now() };
  sock.setNoDelay(true);
  sock.on('data', d => { c.buf = Buffer.concat([c.buf, d]); if (c.buf.length > 1 << 20) return sock.destroy(); parse(c); });
  sock.on('close', () => leave(c));
  sock.on('error', () => sock.destroy());
});

function frame(str, op) {
  const p = Buffer.from(str), n = p.length;
  const h = n < 126 ? Buffer.from([0x80 | (op || 1), n]) :
    n < 65536 ? Buffer.from([0x80 | (op || 1), 126, n >> 8, n & 255]) : null;
  return h ? Buffer.concat([h, p]) : null;
}
const send = (c, o) => { const f = frame(JSON.stringify(o)); if (f && !c.sock.destroyed) c.sock.write(f); };

function parse(c) {
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const op = b[0] & 15; let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    const masked = b[1] & 128; if (masked) off += 4;
    if (len > 16384) return c.sock.destroy();
    if (b.length < off + len) return;
    const pl = Buffer.from(b.slice(off, off + len));
    if (masked) { const m = b.slice(off - 4, off); for (let i = 0; i < pl.length; i++) pl[i] ^= m[i & 3]; }
    c.buf = b.slice(off + len);
    if (op === 8) { c.sock.end(); return; }
    if (op === 9) { c.sock.write(Buffer.concat([Buffer.from([0x8A, pl.length]), pl])); continue; }
    if (op === 1) { try { handle(c, JSON.parse(pl.toString())); } catch (e) {} }
  }
}

const mkCode = () => { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; let s; do { s = ''; for (let i = 0; i < 4; i++) s += A[Math.random() * A.length | 0]; } while (rooms.has(s)); return s; };
const list = r => [...r.players.values()].map(p => ({ id: p.id, name: p.name }));
const bcast = (r, o, except) => r.players.forEach(p => { if (p !== except) send(p, o); });

function join(c, r) {
  r.players.set(c.id, c); c.room = r;
  send(c, { t: 'room', code: r.code, id: c.id, host: r.host, players: list(r) });
  bcast(r, { t: 'join', id: c.id, name: c.name }, c);
}
function leave(c) {
  const r = c.room; if (!r) return; c.room = null; r.players.delete(c.id);
  if (!r.players.size) return rooms.delete(r.code);
  if (r.host === c.id) r.host = r.players.keys().next().value;
  bcast(r, { t: 'leave', id: c.id, host: r.host });
}
function handle(c, m) {
  if (m.t === 'create' || m.t === 'join') {
    leave(c); c.name = String(m.name || 'Jogador').slice(0, 14);
    if (m.t === 'create') { const r = { code: mkCode(), host: c.id, players: new Map() }; rooms.set(r.code, r); return join(c, r); }
    const r = rooms.get(String(m.code || '').toUpperCase().trim());
    if (!r) return send(c, { t: 'err', m: 'Sala não encontrada.' });
    if (r.players.size >= MAX) return send(c, { t: 'err', m: 'Sala cheia (máx. ' + MAX + ').' });
    return join(c, r);
  }
  if (!c.room) return;
  if (m.t === 's') return bcast(c.room, { t: 's', id: c.id, x: m.x, z: m.z, y: m.y, a: m.a, w: m.w }, c);
  if (m.t === 'ev') return bcast(c.room, { t: 'ev', id: c.id, k: m.k, d: m.d }, c); // eventos genéricos (fase 2)
  if (m.t === 'leave') leave(c);
}
// remove conexões mortas
setInterval(() => rooms.forEach(r => r.players.forEach(p => { if (p.sock.destroyed) leave(p); })), 30000);
srv.listen(PORT, () => console.log('Comanda13 server na porta ' + PORT));
