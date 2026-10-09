import express from 'express';
import {createServer} from 'node:http';
import {randomUUID} from 'node:crypto';
import {Server} from 'socket.io';

const app = express();
const http = createServer(app);
const io = new Server(http);
app.use(express.static('public'));

const rooms = new Map();
const code = () => String(Math.floor(100000 + Math.random() * 900000));

function publicState(r) {
  return {
    code: r.code,
    phase: r.phase,
    question: r.phase === 'question' ? r.question : null,
    seconds: r.seconds,
    round: r.round,
    players: Object.values(r.players).map(p => ({
      name: p.name,
      score: p.score,
      answered: r.answers[p.id] !== undefined
    })),
    results: r.phase === 'results' ? r.results : null,
    correctAnswer: r.phase === 'results' ? r.resultsAnswer : null,
    hostConnected: !!r.hostSocket
  };
}

function broadcast(r) {
  io.to(r.code).emit('state', publicState(r));
}

function finish(r) {
  if (r.phase !== 'question') return;
  clearInterval(r.timer);
  r.phase = 'results';
  const correct = r.answer;

  r.results = Object.values(r.players).map(p => {
    let a = r.answers[p.id];
    let pts = 0;
    if (a !== undefined) {
      const diff = Math.abs(a - correct);
      const pct = correct === 0 ? (diff === 0 ? 0 : Infinity) : diff / Math.abs(correct);
      pts = diff === 0 ? 10 : pct <= 0.05 + 1e-12 ? 7 : pct <= 0.10 + 1e-12 ? 3 : 0;
      p.score += pts;
    }
    return {name: p.name, answer: a ?? null, points: pts, total: p.score};
  }).sort((a,b) => b.total - a.total);

  r.resultsAnswer = correct;
  broadcast(r);
}

io.on('connection', s => {
  s.on('create', (_, cb) => {
    let c = code();
    while (rooms.has(c)) c = code();
    const r = {
      code: c,
      hostSocket: s.id,
      hostKey: randomUUID(),
      players: {},
      answers: {},
      phase: 'lobby',
      round: 0,
      seconds: 0
    };
    rooms.set(c, r);
    s.join(c);
    s.data = {code: c, host: true};
    cb({ok: true, code: c, hostKey: r.hostKey});
    broadcast(r);
  });

  s.on('join', ({code: c, name, id}, cb) => {
    const r = rooms.get(String(c));
    name = String(name || '').trim().slice(0,24);
    if (!r || !name) return cb({error: 'الغرفة غير موجودة أو الاسم فارغ'});
    if (!id || !r.players[id]) {
      if (Object.values(r.players).some(p => p.name === name))
        return cb({error: 'الاسم مستخدم بالفعل'});
      id = randomUUID();
      r.players[id] = {id, name, score: 0};
    }
    s.join(r.code);
    s.data = {code: r.code, playerId: id};
    cb({ok: true, id});
    broadcast(r);
  });

  s.on('watch', ({code: c}, cb = () => {}) => {
    const r = rooms.get(String(c || ''));
    if (!r) return cb({error: 'الغرفة غير موجودة'});
    s.join(r.code);
    s.data = {code: r.code, tv: true};
    cb({ok: true});
    s.emit('state', publicState(r));
  });

  s.on('reconnectHost', ({code: c, key}, cb) => {
    const r = rooms.get(String(c));
    if (!r || r.hostKey !== key)
      return cb({error: 'تعذر استعادة المضيف'});
    r.hostSocket = s.id;
    s.join(r.code);
    s.data = {code: r.code, host: true};
    cb({ok: true});
    broadcast(r);
  });

  s.on('start', ({question, answer, seconds}, cb = () => {}) => {
    const r = rooms.get(s.data.code);
    if (!r || r.hostSocket !== s.id)
      return cb({error: 'للمضيف فقط'});
    question = String(question || '').trim().slice(0,250);
    answer = Number(answer);
    seconds = Math.max(10, Math.min(180, Number(seconds) || 30));
    if (!question || !Number.isFinite(answer))
      return cb({error: 'أدخل السؤال والإجابة الرقمية'});
    clearInterval(r.timer);
    r.phase = 'question';
    r.question = question;
    r.answer = answer;
    r.answers = {};
    r.results = null;
    r.round++;
    r.seconds = seconds;
    broadcast(r);
    r.timer = setInterval(() => {
      if (--r.seconds <= 0) finish(r);
      else broadcast(r);
    }, 1000);
    cb({ok: true});
  });

  s.on('answer', ({value}, cb) => {
    const r = rooms.get(s.data.code);
    const id = s.data.playerId;
    const n = Number(value);
    if (!r || !id || !r.players[id] || r.phase !== 'question' ||
        r.answers[id] !== undefined || value === '' || !Number.isFinite(n))
      return cb({error: 'لا يمكن تسجيل هذه الإجابة'});
    r.answers[id] = n;
    cb({ok: true});
    broadcast(r);
    if (Object.keys(r.answers).length === Object.keys(r.players).length &&
        Object.keys(r.players).length > 0) finish(r);
  });

  s.on('finish', () => {
    const r = rooms.get(s.data.code);
    if (r && r.hostSocket === s.id) finish(r);
  });

  s.on('disconnect', () => {
    const r = rooms.get(s.data.code);
    if (r && r.hostSocket === s.id) {
      r.hostSocket = null;
      broadcast(r);
    }
  });
});

const port = process.env.PORT || 3000;
http.listen(port, () => console.log('Game at http://localhost:' + port));
