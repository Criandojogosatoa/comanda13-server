// Servidor de salas do Comanda 13 (relay WebSocket, sem dependências).
// Uso: node server.js   (porta via variável PORT, padrão 8080)
const http = require('http'), crypto = require('crypto');
const PORT = process.env.PORT || 8080, MAX = 4;
const GRACE = +process.env.GRACE_MS || 60000; // quanto tempo a vaga fica guardada depois que a conexão cai
const IDLE = 45000; // sem receber nada por esse tempo = conexão morta (só para clientes com reconexão)
const rooms = new Map(); // code -> {code, players: Map(id -> client)}
let nextId = 1;

const srv = http.createServer((q, r) => { r.writeHead(200); r.end('Comanda13 server OK'); });

srv.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) return sock.destroy();
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  const c = { sock, id: nextId++, name: 'Jogador', room: null, buf: Buffer.alloc(0), last: Date.now(),
    tok: crypto.randomBytes(8).toString('hex'), rc: false, dead: false, timer: null };
  sock.setNoDelay(true);
  sock.on('data', d => { c.last = Date.now(); c.buf = Buffer.concat([c.buf, d]); if (c.buf.length > 1 << 20) return sock.destroy(); parse(c); });
  sock.on('close', () => lost(c));
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
  bcast(r, { t: 'join', id: c.id, name: c.name }, c);
}
function leave(c) {
  const r = c.room; if (!r) return; c.room = null; clearTimeout(c.timer);
  if (r.players.get(c.id) !== c) return; // a vaga já foi retomada por uma reconexão
  r.players.delete(c.id);
  if (!r.players.size) return rooms.delete(r.code);
  if (r.host === c.id) { // o novo anfitrião é alguém que está conectado de verdade
    const vivo = [...r.players.values()].find(p => !p.sock.destroyed);
    r.host = (vivo || r.players.values().next().value).id;
  }
  bcast(r, { t: 'leave', id: c.id, host: r.host });
}
// a conexão caiu: quem sabe reconectar (rc) tem a vaga guardada por GRACE ms
function lost(c) {
  if (c.dead) return; c.dead = true;
  const r = c.room; if (!r || r.players.get(c.id) !== c) return;
  if (!c.rc) return leave(c);
  c.timer = setTimeout(() => leave(c), GRACE);
  bcast(r, { t: 'dc', id: c.id }, c);
}
// o jogador voltou com uma conexão nova: ela assume a vaga (mesmo id) da antiga
function takeover(c, r, old) {
  clearTimeout(old.timer); old.dead = true; old.room = null;
  c.id = old.id; c.tok = old.tok; c.name = old.name; c.rc = true; c.room = r;
  r.players.set(c.id, c);
  try { old.sock.destroy(); } catch (e) {}
  send(c, { t: 'room', code: r.code, id: c.id, host: r.host, players: list(r), tok: c.tok, re: 1, pub: r.pub ? 1 : 0 });
  bcast(r, { t: 'rj', id: c.id, name: c.name }, c);
}
function handle(c, m) {
  // partida rápida: entra na sala pública mais cheia (mesma versão do jogo) que ainda tenha vaga;
  // se não houver nenhuma, abre uma sala pública nova e espera os outros chegarem
  if (m.t === 'quick') {
    leave(c); c.name = String(m.name || 'Jogador').slice(0, 14); c.rc = !!m.rc;
    const v = String(m.v || '').slice(0, 12);
    let best = null, bn = 0;
    rooms.forEach(r => {
      if (!r.pub || r.v !== v || r.players.size >= MAX) return;
      const n = [...r.players.values()].filter(p => !p.sock.destroyed).length; // só conta quem está conectado
      if (n > bn) { best = r; bn = n; }
    });
    if (best) return join(c, best);
    const r = { code: mkCode(), host: c.id, players: new Map(), pub: true, v };
    rooms.set(r.code, r); return join(c, r);
  }
  if (m.t === 'create' || m.t === 'join' || m.t === 'rejoin') {
    if (m.t === 'rejoin') {
      const r0 = rooms.get(String(m.code || '').toUpperCase().trim());
      if (!r0) return send(c, { t: 'rjfail', why: 'room' });
      const old = [...r0.players.values()].find(p => p.tok === String(m.tok || ''));
      if (old && old !== c) return takeover(c, r0, old);
      m = Object.assign({}, m, { t: 'join' }); // a vaga expirou, mas a sala existe: entra como jogador novo
    }
    leave(c); c.name = String(m.name || 'Jogador').slice(0, 14); c.rc = !!m.rc;
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
// a cada 10 s: avisa que o servidor está vivo (heartbeat) e derruba conexões mudas
setInterval(() => rooms.forEach(r => r.players.forEach(p => {
  if (p.sock.destroyed) return;
  if (p.rc && Date.now() - p.last > IDLE) return p.sock.destroy();
  send(p, { t: 'p' });
})), 10000);
srv.listen(PORT, () => console.log('Comanda13 server na porta ' + PORT));
