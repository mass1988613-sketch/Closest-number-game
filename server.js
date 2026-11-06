import express from 'express';
import {createServer} from 'node:http';
import {randomUUID,timingSafeEqual} from 'node:crypto';
import {Server} from 'socket.io';
const app=express(), http=createServer(app), io=new Server(http,{maxHttpBufferSize:8e6});
app.use(express.static('public'));
const rooms=new Map();
const code=()=>String(Math.floor(100000+Math.random()*900000));
const attempts=new Map();
function hostAuthorized(socket,password){
 const expected=process.env.HOST_PASSWORD;
 if(!expected)return false;
 const ip=socket.handshake.address||'unknown',now=Date.now();
 const x=attempts.get(ip)||{count:0,until:0};
 if(now>x.until){x.count=0;x.until=now+15*60*1000;}
 if(x.count>=12)return false;
 const a=Buffer.from(String(password||'')),b=Buffer.from(expected);
 const ok=a.length===b.length&&timingSafeEqual(a,b);
 if(!ok)x.count++;else x.count=0;
 attempts.set(ip,x);return ok;
}
function normalizeText(v){return String(v??'').toLowerCase().normalize('NFKC').replace(/[أإآٱ]/g,'ا').replace(/ى/g,'ي').replace(/ة/g,'ه').replace(/ـ/g,'').replace(/[\u064b-\u065f\u0670]/g,'').replace(/[^\p{L}\p{N}]/gu,'');}
function editDistance(a,b){const prev=Array.from({length:b.length+1},(_,i)=>i);for(let i=1;i<=a.length;i++){let last=prev[0];prev[0]=i;for(let j=1;j<=b.length;j++){const t=prev[j];prev[j]=Math.min(prev[j]+1,prev[j-1]+1,last+(a[i-1]===b[j-1]?0:1));last=t;}}return prev[b.length];}
function textCorrect(answer,expected,alternates=[]){const a=normalizeText(answer);if(!a)return false;return [expected,...alternates].some(v=>{const b=normalizeText(v);if(!b)return false;if(a===b)return true;const max=b.length>=9?2:b.length>=5?1:0;return editDistance(a,b)<=max;});}
function state(r,host=false){const visible=['question','results'].includes(r.phase);return {code:r.code,phase:r.phase,round:r.round,total:r.queue.length,question:visible?r.current?.question:null,image:visible?r.current?.image:null,questionType:visible?r.current?.type:null,seconds:r.seconds,correctAnswer:r.phase==='results'?r.current?.answer:null,players:Object.values(r.players).filter(p=>!p.kicked).map(p=>({name:p.name,score:p.score,answered:r.answers[p.id]!==undefined})),results:r.phase==='results'?r.results:null,hostConnected:!!r.hostSocket,soundEnabled:r.soundEnabled!==false,soundVolume:r.soundVolume??35,...(host?{upcoming:r.queue[r.round]||null}:{} )};}
function broadcast(r){io.to(r.code).emit('state',state(r));if(r.hostSocket)io.to(r.hostSocket).emit('hostPreview',{upcoming:r.queue[r.round]||null,round:r.round+1});}
function finish(r){if(r.phase!=='question')return;clearInterval(r.timer);r.phase='results';const q=r.current,correct=q.answer;const all=Object.values(r.players).filter(p=>!p.kicked);const graded=all.map(p=>{const rec=r.answers[p.id];let pts=0,exact=false;if(rec!==undefined){if(q.type==='text'){exact=textCorrect(rec.value,correct,q.alternates);pts=exact?10:0;}else{const n=Number(rec.value),diff=Math.abs(n-correct),pct=correct===0?(diff===0?0:Infinity):diff/Math.abs(correct);exact=diff===0;pts=exact?10:pct<=.05+1e-12?7:pct<=.10+1e-12?3:0;}}return {p,rec,pts,exact,bonus:0};});
 const fastest=graded.filter(x=>x.exact).sort((a,b)=>a.rec.time-b.rec.time);if(fastest[0])fastest[0].bonus=3;if(fastest[1])fastest[1].bonus=1;
 r.results=graded.map(x=>{x.p.score+=x.pts+x.bonus;return {name:x.p.name,answer:x.rec?.value??null,points:x.pts,bonus:x.bonus,total:x.p.score};}).sort((a,b)=>b.total-a.total);broadcast(r);}
function hostRoom(s,cb){const r=rooms.get(s.data.code);if(!r||r.hostSocket!==s.id){cb?.({error:'للمضيف فقط'});return null;}return r;}
function safeImage(v){if(!v)return '';if(typeof v!=='string')return null;if(/^\/images\/[a-zA-Z0-9_-]+\.webp$/.test(v))return v;if(/^data:image\/(png|jpeg|webp|gif);base64,[a-zA-Z0-9+/=]+$/.test(v)&&v.length<2500000)return v;if(/^https:\/\/[^\s<>"']{1,2000}$/i.test(v))return v;return null;}
function normalize(items){if(!Array.isArray(items)||items.length<1||items.length>100)return null;const out=[];for(const x of items){const question=String(x?.question||'').trim().slice(0,250),raw=x?.answer,type=x?.type==='text'?'text':'number',image=safeImage(x?.image||'');if(!question||raw===''||raw===null||raw===undefined||image===null)return null;const answer=type==='text'?String(raw).trim().slice(0,120):Number(raw);if(type==='number'&&!Number.isFinite(answer))return null;const alternates=type==='text'&&Array.isArray(x.alternates)?x.alternates.slice(0,8).map(v=>String(v).trim().slice(0,120)).filter(Boolean):[];out.push({question,answer,image,type,alternates});}return out;}
io.on('connection',s=>{
 s.on('create',({password}={},cb=()=>{})=>{if(!hostAuthorized(s,password))return cb({error:'كلمة مرور المضيف غير صحيحة أو المحاولات كثيرة'});let c=code();while(rooms.has(c))c=code();const r={code:c,hostSocket:s.id,hostKey:randomUUID(),players:{},banned:new Set(),answers:{},queue:[],phase:'lobby',round:0,seconds:0,soundEnabled:true,soundVolume:35};rooms.set(c,r);s.join(c);s.data={code:c,host:true};cb({ok:true,code:c,hostKey:r.hostKey});broadcast(r);});
 s.on('reconnectHost',({code:c,key},cb)=>{const r=rooms.get(String(c));if(!r||r.hostKey!==key)return cb({error:'تعذر استعادة المضيف'});r.hostSocket=s.id;s.join(r.code);s.data={code:r.code,host:true};cb({ok:true});broadcast(r);});
 s.on('watch',({code:c},cb)=>{const r=rooms.get(String(c));if(!r)return cb({error:'الغرفة غير موجودة'});s.join(r.code);s.data={code:r.code,tv:true};cb({ok:true});s.emit('state',state(r));});
 s.on('join',({code:c,name,token},cb)=>{const r=rooms.get(String(c));name=String(name||'').trim().slice(0,24);token=String(token||'');if(!r||!name)return cb({error:'الغرفة غير موجودة أو الاسم فارغ'});if(!/^[\w-]{16,100}$/.test(token))return cb({error:'تعذر التحقق من الجهاز، حدّث الصفحة'});if(r.banned.has(token))return cb({error:'تم منع هذا الجهاز من الغرفة'});let p=r.players[token];if(p){if(p.kicked)return cb({error:'تم إخراج اللاعب'});name=p.name;}else{if(r.phase!=='lobby')return cb({error:'انتهى وقت انضمام اللاعبين'});if(Object.values(r.players).some(x=>x.name.toLowerCase()===name.toLowerCase()))return cb({error:'الاسم مستخدم بالفعل'});p={id:token,name,score:0,kicked:false};r.players[token]=p;}s.join(r.code);s.data={code:r.code,playerId:token};cb({ok:true,name:p.name});broadcast(r);});
 s.on('soundSettings',({enabled,volume}={})=>{const r=hostRoom(s);if(!r)return;if(typeof enabled==='boolean')r.soundEnabled=enabled;if(Number.isFinite(Number(volume)))r.soundVolume=Math.max(0,Math.min(100,Number(volume)));broadcast(r);});
 s.on('prepare',({questions},cb=()=>{})=>{const r=hostRoom(s,cb);if(!r)return;if(r.phase!=='lobby')return cb({error:'لا يمكن تغيير الأسئلة بعد بدء المسابقة'});const q=normalize(questions);if(!q)return cb({error:'تحقق من الأسئلة والإجابات والصور (الحد 100 سؤال)'});r.queue=q;broadcast(r);cb({ok:true,count:q.length});});
 s.on('editUpcoming',({question,answer,image,type,alternates},cb=()=>{})=>{const r=hostRoom(s,cb);if(!r)return;if(r.phase==='question'||r.round>=r.queue.length)return cb({error:'لا يمكن تعديل السؤال الآن'});const q=normalize([{question,answer,image,type,alternates}]);if(!q)return cb({error:'بيانات السؤال غير صحيحة'});r.queue[r.round]=q[0];broadcast(r);cb({ok:true});});
 s.on('next',({seconds},cb=()=>{})=>{const r=hostRoom(s,cb);if(!r)return;if(!r.queue.length)return cb({error:'جهز الأسئلة أولًا'});if(r.phase==='question')return cb({error:'أنهِ السؤال الحالي أولًا'});if(r.round>=r.queue.length){r.phase='finished';broadcast(r);return cb({ok:true,finished:true});}r.current=r.queue[r.round];r.round++;r.answers={};r.results=null;r.phase='question';r.seconds=Math.max(10,Math.min(180,Number(seconds)||30));broadcast(r);clearInterval(r.timer);r.timer=setInterval(()=>{if(--r.seconds<=0)finish(r);else broadcast(r);},1000);cb({ok:true});});
 s.on('answer',({value},cb=()=>{})=>{const r=rooms.get(s.data.code),id=s.data.playerId;if(!r||!id||!r.players[id]||r.players[id].kicked||r.phase!=='question'||r.answers[id]!==undefined||String(value??'').trim()==='')return cb({error:'لا يمكن تسجيل الإجابة'});const v=r.current.type==='text'?String(value).trim().slice(0,120):Number(String(value).replace(/,/g,''));if(r.current.type!=='text'&&!Number.isFinite(v))return cb({error:'أدخل رقمًا صحيحًا'});r.answers[id]={value:v,time:Date.now()};cb({ok:true});broadcast(r);const eligible=Object.values(r.players).filter(p=>!p.kicked);if(eligible.length&&eligible.every(p=>r.answers[p.id]!==undefined))finish(r);});
 s.on('finish',()=>{const r=rooms.get(s.data.code);if(r&&r.hostSocket===s.id)finish(r);});
 s.on('kick',({name,ban},cb=()=>{})=>{const r=hostRoom(s,cb);if(!r)return;const p=Object.values(r.players).find(p=>p.name===name&&!p.kicked);if(!p)return cb({error:'اللاعب غير موجود'});p.kicked=true;if(ban)r.banned.add(p.id);io.to(r.code).emit('playerRemoved',{token:p.id});broadcast(r);cb({ok:true});});
 s.on('disconnect',()=>{const r=rooms.get(s.data.code);if(r&&r.hostSocket===s.id){r.hostSocket=null;broadcast(r);}});
});
http.listen(process.env.PORT||3000,()=>console.log('Closest Number Game ready'));
