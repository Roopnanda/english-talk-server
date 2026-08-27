const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('English Talk Signaling Server is Running Live!\n');
});

const wss = new WebSocket.Server({ server });

// Active waiting queue and live rooms
let waitingQueue = [];
const activeRooms = new Map(); // roomId -> { user1: ws, user2: ws }

function cleanupClient(ws) {
    // Remove from waiting queue
    waitingQueue = waitingQueue.filter(item => item.ws !== ws);

    // If inside an active room, notify the other peer
    for (const [roomId, room] of activeRooms.entries()) {
        if (room.user1 === ws || room.user2 === ws) {
            const peer = room.user1 === ws ? room.user2 : room.user1;
            if (peer && peer.readyState === WebSocket.OPEN) {
                peer.send(JSON.stringify({ type: 'call_ended', roomId }));
            }
            activeRooms.delete(roomId);
            console.log(`[Room Cleaned] ${roomId}`);
            break;
        }
    }
}

wss.on('connection', (ws) => {
    console.log('[Connection] New client connected');
    ws.isAlive = true;

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const { type, roomId, sdp, candidate, level, gender, talkToFemaleOnly, isVip } = data;

            switch (type) {
                case 'join_queue': {
                    // Remove if already in queue
                    waitingQueue = waitingQueue.filter(item => item.ws !== ws);

                    const userLevel = level || 'Intermediate';
                    const userGender = gender || 'Male';
                    const targetFemale = !!talkToFemaleOnly;
                    const vipStatus = !!isVip;

                    console.log(`[Queue] User joined: level=${userLevel}, gender=${userGender}, vip=${vipStatus}`);

                    // Attempt exact level match first
                    let matchIndex = waitingQueue.findIndex(peer => {
                        if (targetFemale && peer.gender !== 'Female') return false;
                        if (peer.targetFemale && userGender !== 'Female') return false;
                        return peer.level === userLevel;
                    });

                    // If no exact match and not strictly VIP locked, pair with any available peer
                    if (matchIndex === -1 && !vipStatus && waitingQueue.length > 0) {
                        matchIndex = waitingQueue.findIndex(peer => {
                            if (targetFemale && peer.gender !== 'Female') return false;
                            if (peer.targetFemale && userGender !== 'Female') return false;
                            return true;
                        });
                    }

                    if (matchIndex !== -1) {
                        const matchedPeer = waitingQueue.splice(matchIndex, 1)[0];
                        const newRoomId = `room_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

                        activeRooms.set(newRoomId, { user1: ws, user2: matchedPeer.ws });

                        // Notify Initiator (User 1)
                        ws.send(JSON.stringify({
                            type: 'match_found',
                            roomId: newRoomId,
                            isInitiator: true,
                            peerLevel: matchedPeer.level
                        }));

                        // Notify Receiver (User 2)
                        matchedPeer.ws.send(JSON.stringify({
                            type: 'match_found',
                            roomId: newRoomId,
                            isInitiator: false,
                            peerLevel: userLevel
                        }));

                        console.log(`[Matched] ${newRoomId} between two users`);
                    } else {
                        // Place into queue
                        waitingQueue.push({
                            ws,
                            level: userLevel,
                            gender: userGender,
                            targetFemale,
                            isVip: vipStatus
                        });
                    }
                    break;
                }

                case 'leave_queue': {
                    waitingQueue = waitingQueue.filter(item => item.ws !== ws);
                    console.log('[Queue] User left queue');
                    break;
                }

                case 'offer': {
                    if (roomId && activeRooms.has(roomId)) {
                        const room = activeRooms.get(roomId);
                        const target = room.user1 === ws ? room.user2 : room.user1;
                        if (target && target.readyState === WebSocket.OPEN) {
                            target.send(JSON.stringify({ type: 'offer', sdp }));
                        }
                    }
                    break;
                }

                case 'answer': {
                    if (roomId && activeRooms.has(roomId)) {
                        const room = activeRooms.get(roomId);
                        const target = room.user1 === ws ? room.user2 : room.user1;
                        if (target && target.readyState === WebSocket.OPEN) {
                            target.send(JSON.stringify({ type: 'answer', sdp }));
                        }
                    }
                    break;
                }

                case 'ice_candidate': {
                    if (roomId && activeRooms.has(roomId)) {
                        const room = activeRooms.get(roomId);
                        const target = room.user1 === ws ? room.user2 : room.user1;
                        if (target && target.readyState === WebSocket.OPEN) {
                            target.send(JSON.stringify({ type: 'ice_candidate', candidate }));
                        }
                    }
                    break;
                }

                case 'end_call': {
                    if (roomId && activeRooms.has(roomId)) {
                        const room = activeRooms.get(roomId);
                        const target = room.user1 === ws ? room.user2 : room.user1;
                        if (target && target.readyState === WebSocket.OPEN) {
                            target.send(JSON.stringify({ type: 'call_ended', roomId }));
                        }
                        activeRooms.delete(roomId);
                        console.log(`[End Call] ${roomId}`);
                    }
                    break;
                }
            }
        } catch (err) {
            console.error('[Error handling message]', err);
        }
    });

    ws.on('close', () => {
        cleanupClient(ws);
    });

    ws.on('error', (err) => {
        console.error('[Client Error]', err);
        cleanupClient(ws);
    });
});

// Periodic ping to keep cloud hosting connections alive
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 25000);

server.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});
