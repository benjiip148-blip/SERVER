'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const SHOTS_PER_PLAYER = Number(process.env.SHOTS_PER_PLAYER) || 5; // tiros por jugador
const ROUND_MS = Number(process.env.ROUND_MS) || 15000;  // tiempo para elegir
const RESULT_MS = Number(process.env.RESULT_MS) || 3200; // pausa tras cada tiro
const START_MS = Number(process.env.START_MS) || 1500;   // pausa al emparejar
const GRACE_MS = Number(process.env.GRACE_MS) || 30000;  // tiempo para reconectar
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const rooms = new Map();
const other = (t) => (t === 'red' ? 'blue' : 'red');
const clone = (o) => JSON.parse(JSON.stringify(o));
const freshScores = () => ({ red: { g: 0, s: 0 }, blue: { g: 0, s: 0 } });
const send = (ws, m) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(m)); };
const bothConnected = (r) => !!(r.players.red && r.players.red.connected && r.players.blue && r.players.blue.connected);
const shooterOf = (r) => (r.round % 2 === 0 ? 'red' : 'blue');

// ---------- salas ----------
function createRoom() {
  let code;
  do {
    code = Array.from({ length: 6 }, () => ALPHABET[crypto.randomInt(ALPHABET.length)]).join('');
  } while (rooms.has(code));
  const room = {
    code, players: { red: null, blue: null }, phase: 'lobby', round: 0,
    scores: freshScores(), actions: {}, timer: null, job: null,
    grace: {}, rematch: {}, lastResult: null, final: null,
  };
  rooms.set(code, room);
  return room;
}

function addPlayer(room, team, ws) {
  const p = { token: crypto.randomBytes(16).toString('hex'), ws, connected: true };
  room.players[team] = p;
  ws.room = room;
  ws.team = team;
  return p;
}

function deleteRoom(room) {
  clearJob(room);
  Object.values(room.grace).forEach(clearTimeout);
  rooms.delete(room.code);
}

function broadcast(room, m) {
  for (const t of ['red', 'blue']) send(room.players[t] && room.players[t].ws, m);
}

// ---------- temporizador pausable (se detiene si alguien se desconecta) ----------
function schedule(room, fn, ms) {
  clearTimeout(room.timer);
  room.timer = null;
  room.job = { fn, left: ms, at: 0 };
  if (bothConnected(room)) arm(room);
}
function arm(room) {
  room.job.at = Date.now();
  room.timer = setTimeout(() => {
    room.timer = null;
    const job = room.job;
    room.job = null;
    if (job) job.fn();
  }, room.job.left);
}
function pause(room) {
  if (room.job && room.timer) {
    clearTimeout(room.timer);
    room.timer = null;
    room.job.left = Math.max(0, room.job.left - (Date.now() - room.job.at));
  }
}
function resumeJob(room) {
  if (room.job && !room.timer && bothConnected(room)) arm(room);
}
function clearJob(room) {
  clearTimeout(room.timer);
  room.timer = null;
  room.job = null;
}
function jobLeft(room) {
  if (!room.job) return ROUND_MS;
  return Math.max(0, room.job.left - (room.timer ? Date.now() - room.job.at : 0));
}

// ---------- flujo de la partida ----------
function roundMsg(room, team) {
  return {
    t: 'round', round: room.round, shooter: shooterOf(room), scores: clone(room.scores),
    total: SHOTS_PER_PLAYER * 2, deadlineIn: jobLeft(room), acted: team ? !!room.actions[team] : false,
  };
}

function snapshot(room, team) {
  if (room.phase === 'aim') return roundMsg(room, team);
  if (room.phase === 'result') return room.lastResult;
  if (room.phase === 'over') return room.final;
  return room.players.blue ? { t: 'matched' } : { t: 'waiting' };
}
function sendSnapshot(room, team) {
  const s = snapshot(room, team);
  if (s) send(room.players[team] && room.players[team].ws, s);
}
function broadcastSnapshot(room) {
  sendSnapshot(room, 'red');
  sendSnapshot(room, 'blue');
}

function startRound(room) {
  room.phase = 'aim';
  room.actions = {};
  schedule(room, () => resolve(room), ROUND_MS);
  broadcastSnapshot(room);
}

function resolve(room) {
  if (room.phase !== 'aim') return;
  clearJob(room);
  const shooter = shooterOf(room);
  const keeper = other(shooter);
  const a = room.actions;
  // Si alguien no actúa a tiempo, se elige al azar.
  const x = a[shooter] ? a[shooter].x : Math.random() * 1.8 - 0.9;
  const dive = a[keeper] ? a[keeper].dive : [-1, 0, 1][crypto.randomInt(3)];
  const zone = x < -0.34 ? -1 : x > 0.34 ? 1 : 0;
  let outcome;
  if (Math.abs(x) > 1) outcome = 'miss';
  else if (zone === dive) outcome = 'save';
  else outcome = 'goal';
  if (outcome === 'goal') room.scores[shooter].g++;
  if (outcome === 'save') room.scores[keeper].s++;

  room.phase = 'result';
  room.lastResult = { t: 'result', round: room.round, shooter, x, dive, outcome, scores: clone(room.scores) };
  broadcast(room, room.lastResult);
  room.round++;

  // Fin: tras los tiros normales y con ronda completa (ambos tiraron lo mismo) y goles distintos.
  const done = room.round >= SHOTS_PER_PLAYER * 2 && room.round % 2 === 0 &&
    room.scores.red.g !== room.scores.blue.g;
  schedule(room, () => {
    if (done) finish(room, room.scores.red.g > room.scores.blue.g ? 'red' : 'blue', 'score');
    else startRound(room);
  }, RESULT_MS);
}

function finish(room, winner, reason) {
  clearJob(room);
  room.phase = 'over';
  room.rematch = {};
  room.final = { t: 'over', winner, reason, scores: clone(room.scores) };
  broadcast(room, room.final);
}

// ---------- mensajes ----------
function onCreate(ws) {
  if (ws.room) return;
  const room = createRoom();
  const p = addPlayer(room, 'red', ws);
  send(ws, { t: 'created', code: room.code, token: p.token, team: 'red' });
}

function onJoin(ws, msg) {
  if (ws.room) return;
  const room = rooms.get(String(msg.code || '').trim().toUpperCase());
  if (!room || !room.players.red || !room.players.red.connected) {
    return send(ws, { t: 'error', code: 'NOT_FOUND', msg: 'Código incorrecto' });
  }
  if (room.players.blue || room.phase !== 'lobby') {
    return send(ws, { t: 'error', code: 'FULL', msg: 'La sala está llena' });
  }
  const p = addPlayer(room, 'blue', ws);
  send(ws, { t: 'joined', code: room.code, token: p.token, team: 'blue' });
  room.phase = 'starting';
  broadcast(room, { t: 'matched' });
  schedule(room, () => startRound(room), START_MS);
}

function onResume(ws, msg) {
  if (ws.room) return;
  const room = rooms.get(String(msg.code || '').toUpperCase());
  const team = room && ['red', 'blue'].find((t) => room.players[t] && room.players[t].token && room.players[t].token === msg.token);
  if (!team) return send(ws, { t: 'error', code: 'SESSION_EXPIRED', msg: 'La sesión expiró' });
  const p = room.players[team];
  if (p.ws && p.ws !== ws) {
    p.ws.room = null;
    try { p.ws.close(); } catch (e) { /* noop */ }
  }
  p.ws = ws;
  p.connected = true;
  ws.room = room;
  ws.team = team;
  clearTimeout(room.grace[team]);
  send(ws, { t: 'joined', code: room.code, token: p.token, team, resumed: true });
  const opp = room.players[other(team)];
  send(ws, { t: 'opp', connected: !!(opp && opp.connected) });
  if (opp && opp.connected) send(opp.ws, { t: 'opp', connected: true });
  resumeJob(room);
  if (bothConnected(room)) broadcastSnapshot(room);
  else sendSnapshot(room, team);
}

function onAct(ws, msg) {
  const room = ws.room;
  if (!room || room.phase !== 'aim' || msg.round !== room.round) return;
  const team = ws.team;
  if (room.actions[team]) return send(ws, { t: 'acted', round: room.round }); // duplicado: se ignora
  if (team === shooterOf(room)) {
    const x = Number(msg.x);
    if (!Number.isFinite(x)) return;
    room.actions[team] = { x: Math.max(-1.3, Math.min(1.3, x)) };
  } else {
    const d = Number(msg.dive);
    if (![-1, 0, 1].includes(d)) return;
    room.actions[team] = { dive: d };
  }
  send(ws, { t: 'acted', round: room.round });
  if (room.actions.red && room.actions.blue) resolve(room);
}

function onRematch(ws) {
  const room = ws.room;
  if (!room || room.phase !== 'over') return;
  room.rematch[ws.team] = true;
  if (room.rematch.red && room.rematch.blue) {
    room.scores = freshScores();
    room.round = 0;
    room.final = null;
    room.lastResult = null;
    room.rematch = {};
    startRound(room);
  } else {
    broadcast(room, { t: 'rematch', red: !!room.rematch.red, blue: !!room.rematch.blue });
  }
}

function onClose(ws, voluntary) {
  const room = ws.room;
  const team = ws.team;
  if (!room) return;
  const p = room.players[team];
  if (!p || p.ws !== ws) return;
  p.connected = false;
  ws.room = null;
  pause(room);
  const opp = room.players[other(team)];

  if (voluntary) {
    p.token = null;
    if (opp && room.phase !== 'over') finish(room, other(team), 'left');
    if (!(opp && opp.connected)) deleteRoom(room);
    return;
  }

  if (opp && opp.connected) send(opp.ws, { t: 'opp', connected: false, graceMs: GRACE_MS });
  clearTimeout(room.grace[team]);
  room.grace[team] = setTimeout(() => {
    if (p.connected) return;
    const o = room.players[other(team)];
    if (o && o.connected && room.phase !== 'over' && room.phase !== 'lobby') {
      finish(room, other(team), 'forfeit'); // el rival no volvió: gana quien sigue conectado
    }
    if (!o || !o.connected) deleteRoom(room);
  }, GRACE_MS);
}

// ---------- servidor ----------
const server = http.createServer((req, res) => { res.end('penales ok'); });
const wss = new WebSocketServer({ server, maxPayload: 2048 });

wss.on('connection', (ws) => {
  ws.alive = true;
  ws.room = null;
  ws.team = null;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    switch (m.t) {
      case 'create': onCreate(ws); break;
      case 'join': onJoin(ws, m); break;
      case 'resume': onResume(ws, m); break;
      case 'act': onAct(ws, m); break;
      case 'rematch': onRematch(ws); break;
      case 'leave': onClose(ws, true); break;
      default: break;
    }
  });
  ws.on('close', () => onClose(ws, false));
  ws.on('error', () => {});
});

// Detecta conexiones muertas (cada 10 s).
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.alive) return ws.terminate();
    ws.alive = false;
    ws.ping();
  });
}, 10000);

server.listen(PORT, () => console.log('Servidor de penales en puerto ' + PORT));
