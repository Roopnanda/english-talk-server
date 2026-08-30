const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('English Talk Signaling Server Running');
});

const wss = new WebSocket.Server({ server });
const PORT = process.env.PORT || 3000;

const waitingQueues = {
    Beginner: [],
    Advanced: []
};

// Map to track active reconnect requests: socketId -> targetSocketId
const reconnectRequests = new Map();
const clients = new Map(); // socketId -> ws
const rooms = new Map();   // roomId -> Set of socketIds

let nextId = 1;

wss.on('connection', (ws) => {
    const socketId = "user_" + (nextId++);
    ws.socketId = socketId;
    clients.set(socketId, ws);
    console.log(`Connected: ${socketId}`);

    ws.on('message', (message) => {
        try {
            const parsed = JSON.parse(message);
            const event = parsed.event;
            const data = parsed.data || {};

            if (event === 'join_queue') {
                const { level, userGender, talkToFemaleOnly, isVip } = data;
                reconnectRequests.delete(socketId);
                removeFromAllQueues(socketId);

                // --- 1. SAME-LEVEL MATCH ATTEMPT ---
                let partner = null;
                const preferredQueue = waitingQueues[level] || waitingQueues['Beginner'];

                if (preferredQueue.length > 0) {
                    partner = preferredQueue.shift();
                } else {
                    // --- 2. CROSS-LEVEL INSTANT FALLBACK ---
                    const fallbackLevel = (level === 'Advanced') ? 'Beginner' : 'Advanced';
                    if (waitingQueues[fallbackLevel] && waitingQueues[fallbackLevel].length > 0) {
                        partner = waitingQueues[fallbackLevel].shift();
                    }
                }

                if (partner) {
                    const roomId = `room_${socketId}_${partner.socketId}_${Date.now()}`;

                    ws.roomId = roomId;
                    partner.ws.roomId = roomId;

                    rooms.set(roomId, new Set([socketId, partner.socketId]));

                    console.log(`Match established: ${socketId} (${level}) <--> ${partner.socketId} (${partner.level}) in ${roomId}`);

                    send(ws, 'match_found', {
                        roomId: roomId,
                        isInitiator: true,
                        peerLevel: partner.level,
                        peerId: partner.socketId,
                        isReconnect: false
                    });

                    send(partner.ws, 'match_found', {
                        roomId: roomId,
                        isInitiator: false,
                        peerLevel: level,
                        peerId: socketId,
                        isReconnect: false
                    });
                } else {
                    preferredQueue.push({ ws, socketId, level, userGender, talkToFemaleOnly, isVip });
                    console.log(`User ${socketId} added to queue: ${level}`);
                }
            } else if (event === 'leave_queue') {
                removeFromAllQueues(socketId);
                reconnectRequests.delete(socketId);
            } else if (event === 'request_reconnect') {
                const { targetPeerId, myLevel } = data;
                removeFromAllQueues(socketId);

                const partnerTarget = reconnectRequests.get(targetPeerId);

                if (partnerTarget === socketId) {
                    // Mutual match verified
                    reconnectRequests.delete(socketId);
                    reconnectRequests.delete(targetPeerId);

                    const partnerWs = clients.get(targetPeerId);
                    if (partnerWs && partnerWs.readyState === WebSocket.OPEN) {
                        const roomId = `reconnect_${socketId}_${targetPeerId}_${Date.now()}`;
                        ws.roomId = roomId;
                        partnerWs.roomId = roomId;

                        rooms.set(roomId, new Set([socketId, targetPeerId]));

                        send(ws, 'match_found', {
                            roomId: roomId,
                            isInitiator: true,
                            peerLevel: partnerWs.dataLevel || "Beginner",
                            peerId: targetPeerId,
                            isReconnect: true
                        });

                        send(partnerWs, 'match_found', {
                            roomId: roomId,
                            isInitiator: false,
                            peerLevel: myLevel || "Beginner",
                            peerId: socketId,
                            isReconnect: true
                        });
                    } else {
                        send(ws, 'reconnect_failed', { reason: "Partner is offline." });
                    }
                } else {
                    ws.dataLevel = myLevel;
                    reconnectRequests.set(socketId, targetPeerId);
                    send(ws, 'reconnect_waiting', {});
                }
            } else if (event === 'cancel_reconnect') {
                reconnectRequests.delete(socketId);
            } else if (event === 'offer' || event === 'answer' || event === 'ice_candidate') {
                relayToRoom(ws, event, data);
            } else if (event === 'end_call') {
                relayToRoom(ws, 'call_ended', {});
                cleanUpRoom(ws.roomId);
                ws.roomId = null;
            }
        } catch (e) {
            console.error("Msg error:", e);
        }
    });

    ws.on('close', () => {
        removeFromAllQueues(socketId);
        reconnectRequests.delete(socketId);
        if (ws.roomId) {
            relayToRoom(ws, 'call_ended', {});
            cleanUpRoom(ws.roomId);
        }
        clients.delete(socketId);
        console.log(`Disconnected: ${socketId}`);
    });
});

function send(ws, event, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event, data }));
    }
}

function relayToRoom(senderWs, event, data) {
    const roomId = data.roomId || senderWs.roomId;
    if (!roomId) return;

    const roomMembers = rooms.get(roomId);
    if (roomMembers) {
        for (const peerId of roomMembers) {
            if (peerId !== senderWs.socketId) {
                const peerWs = clients.get(peerId);
                if (peerWs && peerWs.readyState === WebSocket.OPEN) {
                    send(peerWs, event, data);
                }
            }
        }
    }
}

function cleanUpRoom(roomId) {
    if (roomId && rooms.has(roomId)) {
        rooms.delete(roomId);
    }
}

function removeFromAllQueues(socketId) {
    for (const lvl in waitingQueues) {
        waitingQueues[lvl] = waitingQueues[lvl].filter(u => u.socketId !== socketId);
    }
}

server.listen(PORT, () => {
    console.log(`WebSocket server listening on port ${PORT}`);
});
