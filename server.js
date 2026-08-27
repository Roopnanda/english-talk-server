const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('English Talk Signaling Server is Running Live!\n');
});

const wss = new WebSocket.Server({ server });

let waitingQueue = [];
const activeRooms = new Map();

function cleanupClient(ws) {
    waitingQueue = waitingQueue.filter(item => item.ws !== ws);

    for (const [roomId, room] of activeRooms.entries()) {
        if (room.user1 === ws || room.user2 === ws) {
            const peer = room.user1 === ws ? room.user2 : room.user1;
            if (peer && peer.readyState === WebSocket.OPEN) {
                try {
                    peer.send(JSON.stringify({ type: 'call_ended', roomId }));
                } catch (e) {
                    console.error('[Send call_ended error]', e);
                }
            }
            activeRooms.delete(roomId);
            console.log(`[Room Cleaned] ${roomId}`);
            break;
        }
    }
}

wss.on('connection', (ws) => {
    console.log('[Connection] New client connected');

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const { type, roomId, sdp, candidate, level, gender, talkToFemaleOnly, isVip } = data;

            switch (type) {
                case 'join_queue': {
                    waitingQueue = waitingQueue.filter(item => item.ws !== ws && item.ws.readyState === WebSocket.OPEN);

                    const userLevel = level || 'Intermediate';
                    const userGender = gender || 'Male';
                    const targetFemale = !!talkToFemaleOnly;
                    const vipStatus = !!isVip;

                    console.log(`[Queue] User joined: level=${userLevel}, gender=${userGender}`);

                    // 1. Try finding a partner
                    let matchIndex = -1;

                    // Match with same level first
                    matchIndex = waitingQueue.findIndex(peer => {
                        if (peer.ws === ws || peer.ws.readyState !== WebSocket.OPEN) return false;
                        if (targetFemale && peer.gender !== 'Female') return false;
                        if (peer.targetFemale && userGender !== 'Female') return false;
                        return peer.level === userLevel;
                    });

                    // If no same-level match, pair with ANY available waiting user
                    if (matchIndex === -1 && waitingQueue.length > 0) {
                        matchIndex = waitingQueue.findIndex(peer => {
                            if (peer.ws === ws || peer.ws.readyState !== WebSocket.OPEN) return false;
                            if (targetFemale && peer.gender !== 'Female') return false;
                            if (peer.targetFemale && userGender !== 'Female') return false;
                            return true;
                        });
                    }

                    if (matchIndex !== -1) {
                        const matchedPeer = waitingQueue.splice(matchIndex, 1)[0];
                        const newRoomId = `room_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

                        activeRooms.set(newRoomId, { user1: ws, user2: matchedPeer.ws });

                        // Notify Initiator
                        ws.send(JSON.stringify({
                            type: 'match_found',
                            roomId: newRoomId,
                            isInitiator: true,
                            peerLevel: matchedPeer.level
                        }));

                        // Notify Receiver
                        matchedPeer.ws.send(JSON.stringify({
                            type: 'match_found',
                            roomId: newRoomId,
                            isInitiator: false,
                            peerLevel: userLevel
                        }));

                        console.log(`[Matched] ${newRoomId}`);
                    } else {
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
                    cleanupClient(ws);
                    break;
                }
            }
        } catch (err) {
            console.error('[Message error]', err);
        }
    });

    ws.on('close', () => cleanupClient(ws));
    ws.on('error', () => cleanupClient(ws));
});

server.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});
