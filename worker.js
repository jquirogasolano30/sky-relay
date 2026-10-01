// Sky Warfare relay: passes game messages between players through Cloudflare (works on school networks).
import { DurableObject } from 'cloudflare:workers';

export class Room extends DurableObject {
  async fetch(request) {
    const role = new URL(request.url).searchParams.get('role');
    const [client, server] = Object.values(new WebSocketPair());
    const all = this.ctx.getWebSockets();
    const host = all.find(w => (w.deserializeAttachment() || {}).id === 'h');
    this.ctx.acceptWebSocket(server);
    if (role === 'host') {
      if (host) { server.send(JSON.stringify({ t: '_err', e: 'unavailable-id' })); server.close(1000, 'taken'); }
      else { server.serializeAttachment({ id: 'h' }); server.send(JSON.stringify({ t: '_open' })); }
    } else if (!host) {
      server.send(JSON.stringify({ t: '_err', e: 'peer-unavailable' })); server.close(1000, 'no room');
    } else {
      const used = new Set(all.map(w => (w.deserializeAttachment() || {}).id));
      let n = 1; while (used.has('p' + n)) n++;
      const id = 'p' + n;
      server.serializeAttachment({ id });
      server.send(JSON.stringify({ t: '_open' }));
      host.send(JSON.stringify({ t: '_join', id }));
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    const me = ws.deserializeAttachment(); if (!me || m.t === '_ping') return;
    const socks = this.ctx.getWebSockets();
    const find = id => socks.find(s => (s.deserializeAttachment() || {}).id === id);
    if (me.id === 'h') {
      const w = find(m.to);
      if (!w) return;
      if (m.t === '_to') w.send(JSON.stringify({ t: '_msg', d: m.d }));
      else if (m.t === '_kick') w.close(1000, 'kicked');
    } else if (m.t === '_up') {
      const h = find('h'); if (h) h.send(JSON.stringify({ t: '_msg', from: me.id, d: m.d }));
    }
  }

  async webSocketClose(ws) {
    const me = ws.deserializeAttachment();
    if (me) {
      for (const s of this.ctx.getWebSockets()) {
        const o = s.deserializeAttachment(); if (s === ws || !o) continue;
        if (me.id === 'h') { s.send(JSON.stringify({ t: '_hostgone' })); s.close(1000, 'host left'); }
        else if (o.id === 'h') s.send(JSON.stringify({ t: '_leave', id: me.id }));
      }
    }
    try { ws.close(); } catch (e) {}
  }

  async webSocketError(ws) { await this.webSocketClose(ws); }
}

export default {
  async fetch(request, env) {
    const m = new URL(request.url).pathname.match(/^\/ws\/([A-Za-z0-9]{3,12})$/);
    if (!m) return new Response('Sky Warfare relay is running.');
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('Expected a WebSocket', { status: 426 });
    return env.ROOM.get(env.ROOM.idFromName(m[1].toUpperCase())).fetch(request);
  }
};
