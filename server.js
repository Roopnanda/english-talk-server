const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('English Talk Signaling Server Active\n');
});

const wss = new WebSocket.Server({ server });

// Queue storage
const queues = {
    ENGLISH_BEGINNER: [],
    ENGLISH_ADVANCED: [],
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

const activeSockets = new Map();
const activeCalls = new Map();
const recentPartners = new Map();
const reconnectRequests = new Map();

function generateRoomId() {
    return 'room_' + Math.random().toString(36).substring(2, 10);
}

function removeFromAllQueues(socketId) {
    for (const key of Object.keys(queues)) {
        const prevLen = queues[key].length;
        queues[key] = queues[key].filter(entry => entry.socketId !== socketId);
        if (queues[key].length !== prevLen) {
            console.log(`[Queue-Purge] Removed ${socketId} from ${key}`);
        }
    }
}

// Gentle keep-alive ping
setInterval(() => {
    for (const [id, sock] of activeSockets.entries()) {
        if (sock.readyState === WebSocket.OPEN) {
            try {
                sock.ping(() => {});
            } catch (e) {}
        } else if (sock.readyState === WebSocket.CLOSED || sock.readyState === WebSocket.CLOSING) {
            activeSockets.delete(id);
            removeFromAllQueues(id);
        }
    }
}, 25000);

// Continuous Match Loop (Runs every 1 second)
setInterval(() => {
    matchUsers();
}, 1000);

// ----------------------------------------------------
// MATCHMAKING ENGINE
// ----------------------------------------------------

function matchUsers() {
    try {
        // 1. 12 Isolated Regional Pools
        const regionalPools = [
            'HINDI', 'PUNJABI', 'MARATHI', 'BENGALI', 'BHOJPURI',
            'GUJARATI', 'KANNADA', 'MALAYALAM', 'TAMIL', 'TELUGU',
            'URDU', 'ARABIC'
        ];

        for (const lang of regionalPools) {
            const pool = queues[lang];
            if (pool && pool.length >= 2) {
                processQueuePairing(pool, false);
            }
        }

        // 2. English Pools
        processEnglishMatchmaking();
    } catch (err) {
        console.error('[Match-Error]', err.message);
    }
}

function processEnglishMatchmaking() {
    const beginnerPool = queues.ENGLISH_BEGINNER;
    const advancedPool = queues.ENGLISH_ADVANCED;
    const now = Date.now();

    // Priority 1: Same-Tier Pairing
    processQueuePairing(beginnerPool, true);
    processQueuePairing(advancedPool, true);

    // Priority 2: Cross-Tier Fallback after 5s
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

function processQueuePairing(pool, isEnglish = false) {
    if (pool.length < 2) return;
    const now = Date.now();

    for (let i = 0; i < pool.length; i++) {
        const userA = pool[i];
        if (!userA) continue;

        for (let j = i + 1; j < pool.length; j++) {
            const userB = pool[j];
            if (!userB) continue;

            if (isEnglish) {
                if (userA.talkToFemaleOnly && userB.userGender !== 'Female') continue;
                if (userB.talkToFemaleOnly && userA.userGender !== 'Female') continue;
            }

            const hasRecent = recentPartners.get(userA.socketId)?.has(userB.socketId);
            const isWaitingLong = (now - userA.joinedAt) > 7000 || (now - userB.joinedAt) > 7000;

            if (hasRecent && !isWaitingLong && pool.length > 2) {
                continue;
            }

            pool.splice(j, 1);
            pool.splice(i, 1);
            createCallPair(userA, userB);
            return processQueuePairing(pool, isEnglish);
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

    activeCalls.set(userA.socketId, { partnerId: userB.socketId, roomId });
    activeCalls.set(userB.socketId, { partnerId: userA.socketId, roomId });

    recordRecentPartner(userA.socketId, userB.socketId);

    console.log(`[MATCH SUCCESS] ${userA.socketId} paired with ${userB.socketId} in Room: ${roomId}`);

    const payloadA = JSON.stringify({
        type: 'match_found',
        roomId: roomId,
        isInitiator: true,
        peerLevel: userB.level || 'Peer',
        peerId: userB.socketId,
        isReconnect: false
    });

    const payloadB = JSON.stringify({
        type: 'match_found',
        roomId: roomId,
        isInitiator: false,
        peerLevel: userA.level || 'Peer',
        peerId: userA.socketId,
        isReconnect: false
    });

    try {
        sockA.send(payloadA);
        sockB.send(payloadB);
    } catch (e) {
        console.error('[Dispatch-ERR]', e.message);
    }
}

function recordRecentPartner(idA, idB) {
    if (!recentPartners.has(idA)) recentPartners.set(idA, new Set());
    if (!recentPartners.has(idB)) recentPartners.set(idB, new Set());

    recentPartners.get(idA).add(idB);
    recentPartners.get(idB).add(idA);

    setTimeout(() => {
        recentPartners.get(idA)?.delete(idB);
        recentPartners.get(idB)?.delete(idA);
    }, 5 * 60 * 1000);
}

// ----------------------------------------------------
// SIGNALING MESSAGE ROUTING
// ----------------------------------------------------

wss.on('connection', (ws) => {
    const socketId = 'user_' + Math.random().toString(36).substring(2, 10);
    ws.socketId = socketId;
    activeSockets.set(socketId, ws);
    console.log(`[Client-Connected] ${socketId}`);

    ws.on('message', (message) => {
        try {
            const messageStr = typeof message === 'string' ? message : message.toString('utf8');
            const data = JSON.parse(messageStr);
            const msgType = (data.type || data.event || data.action || '').toLowerCase();

            switch (msgType) {
                case 'join_queue':
                case 'search':
                case 'find_match':
                case 'join': {
                    removeFromAllQueues(socketId);
                    const lang = (data.language || 'ENGLISH').toUpperCase();
                    let queueKey = lang;

                    if (lang === 'ENGLISH') {
                        queueKey = (data.level === 'Advanced') ? 'ENGLISH_ADVANCED' : 'ENGLISH_BEGINNER';
                    }

                    if (queues[queueKey]) {
                        queues[queueKey].push({
                            socketId: socketId,
                            level: data.level || 'Beginner',
                            language: lang,
                            queueKey: queueKey,
                            userGender: data.userGender || data.gender || 'Unknown',
                            talkToFemaleOnly: data.talkToFemaleOnly === true || data.femaleOnly === true,
                            isVip: data.isVip === true,
                            joinedAt: Date.now()
                        });
                        console.log(`[Queue-Joined] ${socketId} joined ${queueKey} (Total in pool: ${queues[queueKey].length})`);
                        matchUsers();
                    }
                    break;
                }

                case 'leave_queue':
                case 'cancel_search':
                case 'cancel': {
                    removeFromAllQueues(socketId);
                    console.log(`[Queue-Left] ${socketId}`);
                    break;
                }

                case 'request_reconnect':
                case 'reconnect': {
                    const targetPeerId = data.targetPeerId || data.targetPeer;
                    const targetSock = activeSockets.get(targetPeerId);

                    if (!targetSock || targetSock.readyState !== WebSocket.OPEN || activeCalls.has(targetPeerId)) {
                        ws.send(JSON.stringify({ type: 'reconnect_failed', reason: 'User unavailable' }));
                        return;
                    }

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
                case 'ice_candidate':
                case 'candidate': {
                    const callInfo = activeCalls.get(socketId);
                    if (callInfo && callInfo.partnerId) {
                        const partnerSock = activeSockets.get(callInfo.partnerId);
                        if (partnerSock && partnerSock.readyState === WebSocket.OPEN) {
                            partnerSock.send(messageStr);
                        }
                    }
                    break;
                }

                case 'end_call':
                case 'hangup': {
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
            console.error('[Message-Parse-ERR]', err.message);
        }
    });

    ws.on('close', () => {
        removeFromAllQueues(socketId);

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

    ws.on('error', (err) => {
        console.error(`[Socket-Error] ${socketId}:`, err.message);
    });
});

server.listen(PORT, () => {
    console.log(`English Talk Signaling Server running on port ${PORT}`);
});
