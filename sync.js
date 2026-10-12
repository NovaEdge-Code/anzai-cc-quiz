/* QSync — redundant state sync for the live quiz.
   Transports: public MQTT brokers (retained, QoS1) + ntfy.sh (HTTP/SSE, 12h cache).
   Every message is a FULL STATE SNAPSHOT with a timestamp; receivers keep the newest. */
(function(g){
  const BROKERS=[
    {id:'emqx',  url:'wss://broker.emqx.io:8084/mqtt'},
    {id:'hivemq',url:'wss://broker.hivemq.com:8884/mqtt'}
  ];
  const NTFY='https://ntfy.sh/';
  const rid=()=>Math.random().toString(36).slice(2,10);
  function fetchTO(url,opt,ms){const ac=new AbortController();const t=setTimeout(()=>ac.abort(),ms||6000);
    return fetch(url,Object.assign({},opt,{signal:ac.signal})).finally(()=>clearTimeout(t))}

  function create(o){
    // o: {topic, role:'host'|'player', onCtl(state,src), onPlayer(snap,src), onStatus(status)}
    const base='anzai-cc-quiz/'+o.topic;
    const st={ntfy:'connecting'}; BROKERS.forEach(b=>st[b.id]='connecting');
    const clients={}; let es=null, stopped=false, esTimer=null;
    const emit=()=>{try{o.onStatus&&o.onStatus(Object.assign({},st))}catch(e){}};
    function handle(raw,src){
      if(!raw)return; let m; try{m=typeof raw==='string'?JSON.parse(raw):raw}catch(e){return}
      if(!m||typeof m!=='object')return;
      try{ if(m.k==='ctl'&&o.onCtl)o.onCtl(m,src); else if(m.k==='p'&&o.onPlayer)o.onPlayer(m,src); }catch(e){console.error(e)}
    }
    function mqttStart(){
      if(!g.mqtt){BROKERS.forEach(b=>st[b.id]='unavailable');emit();return}
      BROKERS.forEach(b=>{
        let c;
        try{c=g.mqtt.connect(b.url,{clientId:o.role+'-'+rid(),clean:true,keepalive:30,reconnectPeriod:3000,connectTimeout:8000,resubscribe:true});}
        catch(e){st[b.id]='error';emit();return}
        clients[b.id]=c;
        c.on('connect',()=>{st[b.id]='live';emit();
          c.subscribe(o.role==='host'?base+'/p/+':base+'/ctl',{qos:1});
          if(o.onReconnect)try{o.onReconnect(b.id)}catch(e){}
        });
        c.on('reconnect',()=>{st[b.id]='connecting';emit()});
        c.on('close',()=>{if(st[b.id]==='live'){st[b.id]='connecting';emit()}});
        c.on('offline',()=>{st[b.id]='error';emit()});
        c.on('error',()=>{st[b.id]='error';emit()});
        c.on('message',(t,p)=>{const s=p&&p.toString();if(s)handle(s,b.id)});
      });
    }
    function ntfyStart(){
      if(stopped)return;
      try{es&&es.close()}catch(e){}
      const ch=o.topic+(o.role==='host'?'-p':'-ctl');
      es=new EventSource(NTFY+ch+'/sse?since=all');
      es.onopen=()=>{st.ntfy='live';emit()};
      es.onmessage=ev=>{let d;try{d=JSON.parse(ev.data)}catch(e){return}if(d.event==='message')handle(d.message,'ntfy')};
      es.onerror=()=>{st.ntfy=es.readyState===2?'error':'connecting';emit();
        if(es.readyState===2){clearTimeout(esTimer);esTimer=setTimeout(ntfyStart,3000)}};
    }
    function mqttPub(topic,obj,retain){
      const ids=Object.keys(clients).filter(id=>clients[id].connected);
      if(!ids.length)return Promise.resolve(false);
      const msg=JSON.stringify(obj);
      return new Promise(res=>{let left=ids.length,ok=false;const fin=()=>{if(--left===0)res(ok)};
        const to=setTimeout(()=>res(ok),4000);
        ids.forEach(id=>{try{clients[id].publish(topic,msg,{qos:1,retain:!!retain},err=>{if(!err){ok=true;clearTimeout(to);res(true)}fin()})}catch(e){fin()}});
      });
    }
    async function ntfyPub(ch,obj){
      for(let i=0;i<3;i++){
        try{const r=await fetchTO(NTFY+ch,{method:'POST',body:JSON.stringify(obj)},6000);if(r.ok)return true;if(r.status===429)return false}catch(e){}
        await new Promise(r=>setTimeout(r,700*(i+1)));
      }
      return false;
    }
    const api={
      start(){mqttStart();ntfyStart();emit();
        document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'){
          if(!es||es.readyState===2)ntfyStart();
          Object.values(clients).forEach(c=>{if(!c.connected)try{c.reconnect()}catch(e){}});
        }});
      },
      status(){return Object.assign({},st)},
      mqttUp(){return Object.values(clients).some(c=>c.connected)},
      // host: publish full ctl state everywhere (MQTT retained + ntfy)
      async publishCtl(state){
        state.k='ctl';
        const [a,b]=await Promise.all([mqttPub(base+'/ctl',state,true),ntfyPub(o.topic+'-ctl',state)]);
        return a||b;
      },
      // player: MQTT first; ntfy if MQTT fails or if forced (host cannot hear MQTT)
      // ntfyMode: 'fallback' (only if MQTT failed) | 'force' (always) | 'never'
      async publishPlayer(snap,ntfyMode){
        snap.k='p'; ntfyMode=ntfyMode||'fallback';
        const a=await mqttPub(base+'/p/'+snap.pid,snap,true);
        let b=false; if(ntfyMode==='force'||(ntfyMode==='fallback'&&!a))b=await ntfyPub(o.topic+'-p',snap);
        return a||b;
      },
      // host-side helper for rehearsal bots
      publishAsPlayer(snap){snap.k='p';return mqttPub(base+'/p/'+snap.pid,snap,true)},
      stop(){stopped=true;try{es&&es.close()}catch(e){}Object.values(clients).forEach(c=>{try{c.end(true)}catch(e){}})}
    };
    return api;
  }
  g.QSync={create,BROKERS};
})(window);
