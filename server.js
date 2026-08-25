const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const queues = {
  beginner: [],
  intermediate: [],
  advanced: []
};

const rooms = new Map();

wss.on('connection', (ws) => {
  ws.id = Math.random().toString(36).substring(2, 9);
  ws.currentRoomId = null;
  ws.currentLevel = null;

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      switch (data.type) {
        case 'ACTION_JOIN_QUEUE':
          handleJoinQueue(ws, data.level || 'intermediate', data.isVip || false);
          break;

        case 'ACTION_LEAVE_QUEUE':
          removeFromQueue(ws);
          break;

        case 'SIGNAL_OFFER':
        case 'SIGNAL_ANSWER':
        case 'SIGNAL_ICE_CANDIDATE':
          relaySignal(ws, data);
          break;

        case 'ACTION_END_CALL':
          handleEndCall(ws);
          break;
      }
    } catch (err) {
      console.error('JSON parse error:', err);
    }
  });

  ws.on('close', () => {
    removeFromQueue(ws);
    handleEndCall(ws);
  });
});

function handleJoinQueue(ws, level, isVip) {
  removeFromQueue(ws);
  ws.currentLevel = level;

  const queue = queues[level] || queues['intermediate'];

  if (queue.length > 0) {
    const peer = queue.shift();
    const roomId = `room_${Date.now()}_${Math.random().toString(36).substring(2, 6)}`;

    ws.currentRoomId = roomId;
    peer.currentRoomId = roomId;

    rooms.set(roomId, { peerA: peer, peerB: ws });

    peer.send(JSON.stringify({
      type: 'EVENT_MATCH_FOUND',
      roomId: roomId,
      isInitiator: true,
      peerLevel: ws.currentLevel
    }));

    ws.send(JSON.stringify({
      type: 'EVENT_MATCH_FOUND',
      roomId: roomId,
      isInitiator: false,
      peerLevel: peer.currentLevel
    }));
  } else {
    if (isVip) {
      queue.unshift(ws);
    } else {
      queue.push(ws);
    }
    ws.send(JSON.stringify({ type: 'EVENT_WAITING_FOR_MATCH' }));
  }
}

function removeFromQueue(ws) {
  if (ws.currentLevel && queues[ws.currentLevel]) {
    queues[ws.currentLevel] = queues[ws.currentLevel].filter((client) => client.id !== ws.id);
  }
}

function relaySignal(ws, data) {
  if (!ws.currentRoomId || !rooms.has(ws.currentRoomId)) return;

  const room = rooms.get(ws.currentRoomId);
  const target = room.peerA.id === ws.id ? room.peerB : room.peerA;

  if (target && target.readyState === target.OPEN) {
    target.send(JSON.stringify(data));
  }
}

function handleEndCall(ws) {
  if (!ws.currentRoomId || !rooms.has(ws.currentRoomId)) return;

  const room = rooms.get(ws.currentRoomId);
  const target = room.peerA.id === ws.id ? room.peerB : room.peerA;

  if (target && target.readyState === target.OPEN) {
    target.send(JSON.stringify({ type: 'EVENT_CALL_ENDED' }));
    target.currentRoomId = null;
  }

  rooms.delete(ws.currentRoomId);
  ws.currentRoomId = null;
}

console.log(`Matchmaking Server listening on port ${PORT}`);

