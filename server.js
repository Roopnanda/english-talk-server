const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT }, () => {
    console.log(`English Talk Signaling Server running on port ${PORT}`);
});

// Map of connected clients: socketId -> { ws, userGender, isVip, talkToFemaleOnly, level, language, inCall, roomId, lastPeerId }
const clients = new Map();
// Active search queue: array of socketId strings
const waitingQueue = [];
// Pending direct reconnect requests: targetSocketId -> requesterSocketId
const pendingReconnects = new Map();

let socketIdCounter = 1;

wss.on('connection', (ws) => {
    const socketId = `user_${socketIdCounter++}_${Date.now()}`;
    
    clients.set(socketId, {
        ws,
        socketId,
        userGender: 'Unknown',
        isVip: false,
        talkToFemaleOnly: false,
        level: 'Beginner',
        language: 'ENGLISH',
        inCall: false,
        roomId: null,
        lastPeerId: null
    });

    console.log(`[Connect] Client connected: ${socketId} (Total: ${clients.size})`);

    ws.on('message', (messageText) => {
        try {
            const data = JSON.parse(messageText);
            const sender = clients.get(socketId);
            if (!sender) return;

            switch (data.event) {
                case 'join_queue': {
                    sender.level = data.level || 'Beginner';
                    sender.language = (data.language || 'ENGLISH').toUpperCase();
                    sender.userGender = data.userGender || 'Unknown';
                    sender.isVip = data.isVip || false;
                    sender.talkToFemaleOnly = data.talkToFemaleOnly || false;

                    // Remove from queue if already waiting to prevent duplicates
                    const existingIdx = waitingQueue.indexOf(socketId);
                    if (existingIdx !== -1) waitingQueue.splice(existingIdx, 1);

                    console.log(`[Queue] ${socketId} searching for Level: ${sender.level}, Language: ${sender.language}`);
                    findMatchForUser(socketId);
                    break;
                }

                case 'leave_queue': {
                    const idx = waitingQueue.indexOf(socketId);
                    if (idx !== -1) {
                        waitingQueue.splice(idx, 1);
                        console.log(`[Queue] ${socketId} left the search queue`);
                    }
                    break;
                }

                case 'request_reconnect': {
                    const targetId = data.targetPeerId;
                    const targetUser = clients.get(targetId);

                    if (targetUser && !targetUser.inCall && waitingQueue.indexOf(targetId) === -1) {
                        // Check if mutual handshake already exists
                        if (pendingReconnects.get(socketId) === targetId) {
                            pendingReconnects.delete(socketId);
                            createMatch(sender, targetUser, true);
                        } else {
                            pendingReconnects.set(targetId, socketId);
                            safeSend(ws, { event: 'reconnect_waiting' });
                        }
                    } else {
                        safeSend(ws, { event: 'reconnect_failed', reason: 'Peer is unavailable' });
                    }
                    break;
                }

                case 'cancel_reconnect': {
                    for (const [target, requester] of pendingReconnects.entries()) {
                        if (requester === socketId || target === socketId) {
                            pendingReconnects.delete(target);
                        }
                    }
                    break;
                }

                case 'offer': {
                    relayToPeer(socketId, { event: 'offer', sdp: data.sdp });
                    break;
                }

                case 'answer': {
                    relayToPeer(socketId, { event: 'answer', sdp: data.sdp });
                    break;
                }

                case 'ice_candidate': {
                    relayToPeer(socketId, { event: 'ice_candidate', candidate: data.candidate });
                    break;
                }

                case 'end_call': {
                    handleCallTermination(socketId);
                    break;
                }
            }
        } catch (err) {
            console.error(`[Error] Parsing message from ${socketId}:`, err.message);
        }
    });

    ws.on('close', () => {
        console.log(`[Disconnect] Client disconnected: ${socketId}`);
        const idx = waitingQueue.indexOf(socketId);
        if (idx !== -1) waitingQueue.splice(idx, 1);

        for (const [target, requester] of pendingReconnects.entries()) {
            if (requester === socketId || target === socketId) {
                pendingReconnects.delete(target);
            }
        }

        handleCallTermination(socketId);
        clients.delete(socketId);
    });
});

function findMatchForUser(userId) {
    const user = clients.get(userId);
    if (!user || user.inCall) return;

    let matchedUserId = null;

    if (user.language !== 'ENGLISH') {
        // STRICT PARTITION: Non-English regional languages match ONLY identical language keys (Zero cross-connection)
        for (let i = 0; i < waitingQueue.length; i++) {
            const candidateId = waitingQueue[i];
            const candidate = clients.get(candidateId);
            if (!candidate || candidateId === userId || candidate.inCall) continue;

            if (candidate.language === user.language) {
                matchedUserId = candidateId;
                waitingQueue.splice(i, 1);
                break;
            }
        }
    } else {
        // ENGLISH TIERS: Exact level match first, followed by cross-level fallback
        // 1. Exact level match
        for (let i = 0; i < waitingQueue.length; i++) {
            const candidateId = waitingQueue[i];
            const candidate = clients.get(candidateId);
            if (!candidate || candidateId === userId || candidate.inCall) continue;

            if (candidate.language === 'ENGLISH' && candidate.level === user.level) {
                matchedUserId = candidateId;
                waitingQueue.splice(i, 1);
                break;
            }
        }

        // 2. Cross-level fallback across English tiers only
        if (!matchedUserId) {
            for (let i = 0; i < waitingQueue.length; i++) {
                const candidateId = waitingQueue[i];
                const candidate = clients.get(candidateId);
                if (!candidate || candidateId === userId || candidate.inCall) continue;

                if (candidate.language === 'ENGLISH') {
                    matchedUserId = candidateId;
                    waitingQueue.splice(i, 1);
                    break;
                }
            }
        }
    }

    if (matchedUserId) {
        const partner = clients.get(matchedUserId);
        createMatch(user, partner, false);
    } else {
        // No match found immediately; enqueue for matching
        if (waitingQueue.indexOf(userId) === -1) {
            waitingQueue.push(userId);
        }
    }
}

function createMatch(userA, userB, isReconnect) {
    const roomId = `room_${userA.socketId}_${userB.socketId}_${Date.now()}`;

    userA.inCall = true;
    userA.roomId = roomId;
    userA.lastPeerId = userB.socketId;

    userB.inCall = true;
    userB.roomId = roomId;
    userB.lastPeerId = userA.socketId;

    console.log(`[Match] Pair created: ${userA.socketId} <-> ${userB.socketId} in ${roomId} [Lang: ${userA.language}]`);

    safeSend(userA.ws, {
        event: 'match_found',
        roomId: roomId,
        isInitiator: true,
        peerLevel: userB.level,
        peerLanguage: userB.language,
        peerId: userB.socketId,
        isReconnect: isReconnect
    });

    safeSend(userB.ws, {
        event: 'match_found',
        roomId: roomId,
        isInitiator: false,
        peerLevel: userA.level,
        peerLanguage: userA.language,
        peerId: userA.socketId,
        isReconnect: isReconnect
    });
}

function relayToPeer(senderId, payload) {
    const sender = clients.get(senderId);
    if (!sender || !sender.lastPeerId) return;

    const partner = clients.get(sender.lastPeerId);
    if (partner && partner.ws && partner.inCall && partner.roomId === sender.roomId) {
        safeSend(partner.ws, payload);
    }
}

function handleCallTermination(userId) {
    const user = clients.get(userId);
    if (!user) return;

    if (user.inCall && user.lastPeerId) {
        const partner = clients.get(user.lastPeerId);
        if (partner && partner.inCall && partner.roomId === user.roomId) {
            partner.inCall = false;
            partner.roomId = null;
            safeSend(partner.ws, { event: 'call_ended' });
        }
    }

    user.inCall = false;
    user.roomId = null;
}

function safeSend(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}
