const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('English Talk Signaling Server Active\n');
});

const wss = new WebSocket.Server({ server });

// Queue storage & tracking structures
const queues = {
    // English tiers
    ENGLISH_BEGINNER: [],
    ENGLISH_ADVANCED: [],
    // 12 Isolated Regional pools
    HINDI: [],
    PUNJABI: [],
    MARATHI: [],
    BENGALI: [],
    BHOJPURI: [],
    GUJARATI: [],
    KANNADA: [],
    MALAYALAM: [],
    TAMIL: [],
    TELUGU: [],
    URDU: [],
    ARABIC: []
};

// Map of active peer sockets: socketId -> WebSocket instance
const activeSockets = new Map();

// Map of active calls: socketId -> { partnerId, roomId, inCall: true }
const activeCalls = new Map();

// Recent partners anti-repeat map: socketId -> Set of recently spoken partnerIds
const recentPartners = new Map();

// Pending reconnect requests: targetPeerId -> { requesterId, level, timestamp }
const reconnectRequests = new Map();

function generateRoomId() {
    return 'room_' + Math.random().toString(36).substring(2, 10);
}

// ----------------------------------------------------
// QUEUE MANAGEMENT & PURGING
// ----------------------------------------------------

function removeFromAllQueues(socketId) {
    for (const key of Object.keys(queues)) {
        queues[key] = queues[key].filter(entry => entry.socketId !== socketId);
    }
}

// Heartbeat: Pings sockets strictly in queues to clear ghosts (does NOT terminate live calls)
const heartbeatInterval = setInterval(() => {
    for (const key of Object.keys(queues)) {
        queues[key] = queues[key].filter(entry => {
            const client = activeSockets.get(entry.socketId);
            if (!client || client.readyState !== WebSocket.OPEN) {
                console.log(`[Queue-Purge] Removed stale ghost socket: ${entry.socketId} from ${key}`);
                return false;
            }
            return true;
        });
    }
}, 15000);

// ----------------------------------------------------
// MATCHMAKING ENGINE
// ----------------------------------------------------

function matchUsers() {
    // 1. Process 12 Isolated Regional Pools (Strictly within pool, No cross-fallback)
    const regionalPools = [
        'HINDI', 'PUNJABI', 'MARATHI', 'BENGALI', 'BHOJPURI',
        'GUJARATI', 'KANNADA', 'MALAYALAM', 'TAMIL', 'TELUGU',
        'URDU', 'ARABIC'
    ];

    for (const lang of regionalPools) {
        const pool = queues[lang];
        if (pool.length >= 2) {
            executePairing(pool, pool, false);
        }
    }

    // 2. Process English Pools with Priority & Soft Fallback
    processEnglishMatchmaking();
}

function processEnglishMatchmaking() {
    const beginnerPool = queues.ENGLISH_BEGINNER;
    const advancedPool = queues.ENGLISH_ADVANCED;
    const now = Date.now();

    // Priority 1: Strict Tier-level Pairing (Beginner <-> Beginner, Advanced <-> Advanced)
    executeEnglishPairing(beginnerPool);
    executeEnglishPairing(advancedPool);

    // Priority 2: Cross-Tier Fallback for users waiting > 5 seconds
    if (beginnerPool.length > 0 && advancedPool.length > 0) {
        const waitingBeginnerIdx = beginnerPool.findIndex(u => (now - u.joinedAt) >= 5000);
        const waitingAdvancedIdx = advancedPool.findIndex(u => (now - u.joinedAt) >= 5000);

        if (waitingBeginnerIdx !== -1 && waitingAdvancedIdx !== -1) {
            const userA = beginnerPool.splice(waitingBeginnerIdx, 1)[0];
            const userB = advancedPool.splice(waitingAdvancedIdx, 1)[0];

            createCallPair(userA, userB);
        }
    }
}

function executeEnglishPairing(pool) {
    const now = Date.now();

    for (let i = 0; i < pool.length; i++) {
        const userA = pool[i];
        if (!userA) continue;

        for (let j = i + 1; j < pool.length; j++) {
            const userB = pool[j];
            if (!userB) continue;

            // VIP Female Filter matching logic (Exclusively in English)
            if (userA.talkToFemaleOnly && userB.userGender !== 'Female') continue;
            if (userB.talkToFemaleOnly && userA.userGender !== 'Female') continue;

            // Soft Anti-Repeat check: avoid recent partner unless waiting > 7 seconds
            const hasRecentA = recentPartners.get(userA.socketId)?.has(userB.socketId);
            const isWaitingLong = (now - userA.joinedAt) > 7000 || (now - userB.joinedAt) > 7000;

            if (hasRecentA && !isWaitingLong && pool.length > 2) {
                continue; // Skip pairing for now if other candidates might be available
            }

            // Pop both atomically
            pool.splice(j, 1);
            pool.splice(i, 1);
            createCallPair(userA, userB);
            return executeEnglishPairing(pool); // Re-run for remaining pool
        }
    }
}

function executePairing(pool) {
    const now = Date.now();

    for (let i = 0; i < pool.length; i++) {
        const userA = pool[i];
        if (!userA) continue;

        for (let j = i + 1; j < pool.length; j++) {
            const userB = pool[j];
            if (!userB) continue;

            const hasRecent = recentPartners.get(userA.socketId)?.has(userB.socketId);
            const isWaitingLong = (now - userA.joinedAt) > 7000 || (now - userB.joinedAt) > 7000;

            if (hasRecent && !isWaitingLong && pool.length > 2) {
                continue;
            }

            pool.splice(j, 1);
            pool.splice(i, 1);
            createCallPair(userA, userB);
            return executePairing(pool);
        }
    }
}

function createCallPair(userA, userB) {
    const sockA = activeSockets.get(userA.socketId);
    const sockB = activeSockets.get(userB.socketId);

    if (!sockA || sockA.readyState !== WebSocket.OPEN || !sockB || sockB.readyState !== WebSocket.OPEN) {
        if (sockA && sockA.readyState === WebSocket.OPEN) queues[userA.queueKey].unshift(userA);
        if (sockB && sockB.readyState === WebSocket.OPEN) queues[userB.queueKey].unshift(userB);
        return;
    }

    const roomId = generateRoomId();

    // Mark active call states
    activeCalls.set(userA.socketId, { partnerId: userB.socketId, roomId });
    activeCalls.set(userB.socketId, { partnerId: userA.socketId, roomId });

    // Track in anti-repeat set (retained for 5 minutes)
    recordRecentPartner(userA.socketId, userB.socketId);

    console.log(`[Match-Found] ${userA.socketId} <-> ${userB.socketId} in Room: ${roomId}`);

    // Send match confirmation (User A is Offer Initiator)
    sockA.send(JSON.stringify({
        type: 'match_found',
        roomId: roomId,
        isInitiator: true,
        peerLevel: userB.level || 'Peer',
        peerId: userB.socketId,
        isReconnect: false
    }));

    sockB.send(JSON.stringify({
        type: 'match_found',
        roomId: roomId,
        isInitiator: false,
        peerLevel: userA.level || 'Peer',
        peerId: userA.socketId,
        isReconnect: false
    }));
}

function recordRecentPartner(idA, idB) {
    if (!recentPartners.has(idA)) recentPartners.set(idA, new Set());
    if (!recentPartners.has(idB)) recentPartners.set(idB, new Set());

    recentPartners.get(idA).add(idB);
    recentPartners.get(idB).add(idA);

    // Clear from recent list after 5 minutes
    setTimeout(() => {
        recentPartners.get(idA)?.delete(idB);
        recentPartners.get(idB)?.delete(idA);
    }, 5 * 60 * 1000);
}

// ----------------------------------------------------
// WEBSOCKET SIGNALING CONNECTION ROUTING
// ----------------------------------------------------

wss.on('connection', (ws) => {
    const socketId = 'user_' + Math.random().toString(36).substring(2, 10);
    activeSockets.set(socketId, ws);
    console.log(`[Client-Connected] Socket ID: ${socketId}`);

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.type) {
                case 'join_queue': {
                    removeFromAllQueues(socketId);
                    const lang = data.language || 'ENGLISH';
                    let queueKey = lang;

                    if (lang === 'ENGLISH') {
                        queueKey = (data.level === 'Advanced') ? 'ENGLISH_ADVANCED' : 'ENGLISH_BEGINNER';
                    }

                    if (queues[queueKey]) {
                        queues[queueKey].push({
                            socketId: socketId,
                            level: data.level,
                            language: lang,
                            queueKey: queueKey,
                            userGender: data.userGender || 'Unknown',
                            talkToFemaleOnly: data.talkToFemaleOnly === true,
                            isVip: data.isVip === true,
                            joinedAt: Date.now()
                        });
                        console.log(`[Queue-Joined] ${socketId} joined ${queueKey}`);
                        matchUsers();
                    }
                    break;
                }

                case 'leave_queue': {
                    removeFromAllQueues(socketId);
                    console.log(`[Queue-Left] ${socketId} left queue`);
                    break;
                }

                case 'request_reconnect': {
                    const targetPeerId = data.targetPeerId;
                    const targetSock = activeSockets.get(targetPeerId);

                    if (!targetSock || targetSock.readyState !== WebSocket.OPEN || activeCalls.has(targetPeerId)) {
                        ws.send(JSON.stringify({ type: 'reconnect_failed', reason: 'User unavailable' }));
                        return;
                    }

                    // Check if target already requested reconnect back (Mutual Handshake)
                    const pending = reconnectRequests.get(socketId);
                    if (pending && pending.requesterId === targetPeerId) {
                        reconnectRequests.delete(socketId);
                        const roomId = generateRoomId();

                        activeCalls.set(socketId, { partnerId: targetPeerId, roomId });
                        activeCalls.set(targetPeerId, { partnerId: socketId, roomId });

                        ws.send(JSON.stringify({
                            type: 'match_found',
                            roomId: roomId,
                            isInitiator: true,
                            peerLevel: 'Peer',
                            peerId: targetPeerId,
                            isReconnect: true
                        }));

                        targetSock.send(JSON.stringify({
                            type: 'match_found',
                            roomId: roomId,
                            isInitiator: false,
                            peerLevel: 'Peer',
                            peerId: socketId,
                            isReconnect: true
                        }));
                    } else {
                        reconnectRequests.set(targetPeerId, { requesterId: socketId, timestamp: Date.now() });
                        ws.send(JSON.stringify({ type: 'reconnect_waiting' }));
                    }
                    break;
                }

                case 'cancel_reconnect': {
                    for (const [key, val] of reconnectRequests.entries()) {
                        if (val.requesterId === socketId || key === socketId) {
                            reconnectRequests.delete(key);
                        }
                    }
                    break;
                }

                case 'offer':
                case 'answer':
                case 'ice_candidate': {
                    const callInfo = activeCalls.get(socketId);
                    if (callInfo && callInfo.partnerId) {
                        const partnerSock = activeSockets.get(callInfo.partnerId);
                        if (partnerSock && partnerSock.readyState === WebSocket.OPEN) {
                            partnerSock.send(JSON.stringify(data));
                        }
                    }
                    break;
                }

                case 'end_call': {
                    const callInfo = activeCalls.get(socketId);
                    if (callInfo && callInfo.partnerId) {
                        const partnerSock = activeSockets.get(callInfo.partnerId);
                        if (partnerSock && partnerSock.readyState === WebSocket.OPEN) {
                            partnerSock.send(JSON.stringify({ type: 'call_ended' }));
                        }
                        activeCalls.delete(callInfo.partnerId);
                    }
                    activeCalls.delete(socketId);
                    break;
                }
            }
        } catch (err) {
            console.error('[Signaling-Error]', err.message);
        }
    });

    ws.on('close', () => {
        removeFromAllQueues(socketId);

        // Notify active call partner only if socket disconnects permanently
        const callInfo = activeCalls.get(socketId);
        if (callInfo && callInfo.partnerId) {
            const partnerSock = activeSockets.get(callInfo.partnerId);
            if (partnerSock && partnerSock.readyState === WebSocket.OPEN) {
                partnerSock.send(JSON.stringify({ type: 'call_ended' }));
            }
            activeCalls.delete(callInfo.partnerId);
        }
        activeCalls.delete(socketId);
        activeSockets.delete(socketId);
        console.log(`[Client-Disconnected] ${socketId}`);
    });
});

server.listen(PORT, () => {
    console.log(`English Talk Signaling Server running on port ${PORT}`);
});
