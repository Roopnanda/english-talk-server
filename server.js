const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('English Talk Matchmaking Server Live\n');
});

const wss = new WebSocket.Server({ server });
const waitingQueue = [];
const activeRooms = new Map();

wss.on('connection', (ws) => {
    ws.id = Math.random().toString(36).substring(2, 9);
    console.log(`[+] User connected: ${ws.id}`);

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.action) {
                case 'join_queue':
                    handleJoinQueue(ws, data);
                    break;
                case 'leave_queue':
                    removeFromQueue(ws);
                    break;
                case 'send_offer':
                case 'send_answer':
                case 'send_ice_candidate':
                    forwardSignalingData(ws, data);
                    break;
                case 'end_call':
                    handleEndCall(ws);
                    break;
            }
        } catch (err) {
            console.error('Error handling message:', err);
        }
    });

    ws.on('close', () => {
        console.log(`[-] User disconnected: ${ws.id}`);
        removeFromQueue(ws);
        handleEndCall(ws);
    });
});

function handleJoinQueue(ws, data) {
    removeFromQueue(ws);

    const userProfile = {
        ws,
        level: data.level || 'Intermediate',
        userGender: data.userGender || 'Male',
        preferredGender: data.preferredGender || 'Any',
        isVip: data.isVip || false
    };

    // Find a compatible partner in queue
    const matchIndex = waitingQueue.findIndex((peer) => {
        // 1. VIP Specific Gender Preference Check
        if (userProfile.preferredGender !== 'Any' && userProfile.preferredGender !== peer.userGender) {
            return false;
        }
        if (peer.preferredGender !== 'Any' && peer.preferredGender !== userProfile.userGender) {
            return false;
        }
        return true;
    });

    if (matchIndex !== -1) {
        const peer = waitingQueue.splice(matchIndex, 1)[0];
        const roomId = `room_${userProfile.ws.id}_${peer.ws.id}`;

        activeRooms.set(userProfile.ws.id, { peerWs: peer.ws, roomId });
        activeRooms.set(peer.ws.id, { peerWs: userProfile.ws, roomId });

        // Notify Initiator
        userProfile.ws.send(JSON.stringify({
            type: 'match_found',
            roomId: roomId,
            isInitiator: true,
            peerLevel: peer.level
        }));

        // Notify Receiver
        peer.ws.send(JSON.stringify({
            type: 'match_found',
            roomId: roomId,
            isInitiator: false,
            peerLevel: userProfile.level
        }));

        console.log(`[MATCH] Room created: ${roomId} (${userProfile.userGender} <-> ${peer.userGender})`);
    } else {
        waitingQueue.push(userProfile);
        console.log(`[QUEUE] User ${ws.id} waiting. Current queue size: ${waitingQueue.length}`);
    }
}

function removeFromQueue(ws) {
    const idx = waitingQueue.findIndex((u) => u.ws.id === ws.id);
    if (idx !== -1) {
        waitingQueue.splice(idx, 1);
        console.log(`[QUEUE] Removed user ${ws.id}`);
    }
}

function forwardSignalingData(ws, data) {
    const session = activeRooms.get(ws.id);
    if (session && session.peerWs && session.peerWs.readyState === WebSocket.OPEN) {
        if (data.action === 'send_offer') {
            session.peerWs.send(JSON.stringify({ type: 'offer', sdp: data.sdp }));
        } else if (data.action === 'send_answer') {
            session.peerWs.send(JSON.stringify({ type: 'answer', sdp: data.sdp }));
        } else if (data.action === 'send_ice_candidate') {
            session.peerWs.send(JSON.stringify({ type: 'ice_candidate', candidate: data.candidate }));
        }
    }
}

function handleEndCall(ws) {
    const session = activeRooms.get(ws.id);
    if (session) {
        if (session.peerWs && session.peerWs.readyState === WebSocket.OPEN) {
            session.peerWs.send(JSON.stringify({ type: 'call_ended' }));
        }
        activeRooms.delete(session.peerWs?.id);
        activeRooms.delete(ws.id);
    }
}

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`Matchmaking Server listening on port ${PORT}`);
});
