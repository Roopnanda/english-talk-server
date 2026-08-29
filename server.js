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
    Intermediate: [],
    Advanced: []
};

// Map to track active reconnect requests: socketId -> targetSocketId
const reconnectRequests = new Map();
const clients = new Map(); // socketId -> ws

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

                const targetQueue = waitingQueues[level] || waitingQueues['Intermediate'];
                const existingIdx = targetQueue.findIndex(u => u.socketId === socketId);
                if (existingIdx !== -1) targetQueue.splice(existingIdx, 1);

                if (targetQueue.length > 0) {
                    const partner = targetQueue.shift();
                    const roomId = `room_${socketId}_${partner.socketId}_${Date.now()}`;

                    ws.roomId = roomId;
                    partner.ws.roomId = roomId;

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
                    targetQueue.push({ ws, socketId, level, userGender, talkToFemaleOnly, isVip });
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

                        send(ws, 'match_found', {
                            roomId: roomId,
                            isInitiator: true,
                            peerLevel: partnerWs.dataLevel || "Intermediate",
                            peerId: targetPeerId,
                            isReconnect: true
                        });

                        send(partnerWs, 'match_found', {
                            roomId: roomId,
                            isInitiator: false,
                            peerLevel: myLevel || "Intermediate",
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
                broadcastToRoom(ws, event, data);
            } else if (event === 'end_call') {
                broadcastToRoom(ws, 'call_ended', {});
                ws.roomId = null;
            }
        } catch (e) {
            console.error("Msg error:", e);
        }
    });

    ws.on('close', () => {
        removeFromAllQueues(socketId);
        reconnectRequests.delete(socketId);
        clients.delete(socketId);
        console.log(`Disconnected: ${socketId}`);
    });
});

function send(ws, event, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event, data }));
    }
}

function broadcastToRoom(senderWs, event, data) {
    if (!senderWs.roomId) return;
    for (const client of clients.values()) {
        if (client !== senderWs && client.roomId === senderWs.roomId && client.readyState === WebSocket.OPEN) {
            send(client, event, data);
        }
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
