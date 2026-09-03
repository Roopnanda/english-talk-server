const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

// HTTP Health Check Server required by Render
const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('English Talk Signaling Server is healthy and running\n');
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocket.Server({ server });

// Queue Pools
const queues = {
    english: {
        Beginner: [],
        Advanced: []
    },
    vipFemale: [],
    regional: {}
};

// State Maps
const activeRooms = new Map();         // roomId -> { user1, user2 }
const recentPartners = new Map();      // userId -> { partnerId, timestamp }
const reportRecords = new Map();       // userId -> [ { reason, timestamp } ]
const femaleConsecutiveVip = new Map();// userId -> count
const reconnectRequests = new Map();   // targetUserId -> { requesterWs, level }

function log(tag, msg) {
    console.log(`[${new Date().toISOString().substring(11, 19)}][${tag}] ${msg}`);
}

function getSafeUserId(ws) {
    return ws.userId || (ws._socket && ws._socket.remoteAddress ? ws._socket.remoteAddress + ":" + ws._socket.remotePort : "unknown");
}

// Rule 22: Smart Anti-Repeat with 7-Second Fallback
function isEligiblePair(itemA, itemB) {
    const now = Date.now();
    const waitA = now - (itemA.joinedAt || now);
    const waitB = now - (itemB.joinedAt || now);

    // If either user has been waiting for more than 7 seconds, relax anti-repeat
    if (waitA >= 7000 || waitB >= 7000) {
        return true;
    }

    const idA = getSafeUserId(itemA.ws);
    const idB = getSafeUserId(itemB.ws);
    const prevA = recentPartners.get(idA);

    if (prevA && prevA.partnerId === idB && (now - prevA.timestamp) < 300000) {
        return false;
    }
    return true;
}

function createMatch(userA, userB, level, language, isReconnect = false) {
    const roomId = "room_" + Math.random().toString(36).substring(2, 9);
    const idA = getSafeUserId(userA);
    const idB = getSafeUserId(userB);

    userA.roomId = roomId;
    userB.roomId = roomId;
    userA.partnerWs = userB;
    userB.partnerWs = userA;
    userA.inCall = true;
    userB.inCall = true;

    activeRooms.set(roomId, { user1: userA, user2: userB, startedAt: Date.now() });

    recentPartners.set(idA, { partnerId: idB, timestamp: Date.now() });
    recentPartners.set(idB, { partnerId: idA, timestamp: Date.now() });

    userA.send(JSON.stringify({
        type: 'match_found',
        roomId: roomId,
        isInitiator: true,
        peerLevel: level,
        peerId: idB,
        isReconnect: isReconnect
    }));

    userB.send(JSON.stringify({
        type: 'match_found',
        roomId: roomId,
        isInitiator: false,
        peerLevel: level,
        peerId: idA,
        isReconnect: isReconnect
    }));

    log("MATCH", `Paired ${idA} and ${idB} in Room: ${roomId} [${language} - ${level}] (Reconnect: ${isReconnect})`);
}

function removeFromAllQueues(ws) {
    for (const level in queues.english) {
        queues.english[level] = queues.english[level].filter(item => item.ws !== ws);
    }
    queues.vipFemale = queues.vipFemale.filter(item => item.ws !== ws);
    for (const lang in queues.regional) {
        queues.regional[lang] = queues.regional[lang].filter(item => item.ws !== ws);
    }
    const myId = getSafeUserId(ws);
    if (reconnectRequests.has(myId)) {
        reconnectRequests.delete(myId);
    }
}

function tryEnglishMatch(level) {
    const queue = queues.english[level];
    if (!queue || queue.length < 2) return;

    for (let i = 0; i < queue.length; i++) {
        for (let j = i + 1; j < queue.length; j++) {
            const itemA = queue[i];
            const itemB = queue[j];

            if (itemA.ws.readyState === WebSocket.OPEN && itemB.ws.readyState === WebSocket.OPEN) {
                if (isEligiblePair(itemA, itemB)) {
                    queue.splice(j, 1);
                    queue.splice(i, 1);
                    createMatch(itemA.ws, itemB.ws, level, "ENGLISH");
                    return;
                }
            }
        }
    }
}

function handleFemaleMatchmaking(femaleWs, level) {
    const femaleId = getSafeUserId(femaleWs);
    const consecutiveVip = femaleConsecutiveVip.get(femaleId) || 0;

    if (consecutiveVip >= 2) {
        log("BREATHER", `Female ${femaleId} hit 2 consecutive VIP calls. Routing to general.`);
        femaleConsecutiveVip.set(femaleId, 0);
        queues.english[level].push({ ws: femaleWs, joinedAt: Date.now() });
        tryEnglishMatch(level);
        return;
    }

    if (queues.vipFemale.length > 0) {
        const vipItem = queues.vipFemale.shift();
        const vipWs = vipItem.ws;

        if (vipWs.readyState === WebSocket.OPEN && isEligiblePair({ ws: femaleWs, joinedAt: Date.now() }, vipItem)) {
            femaleConsecutiveVip.set(femaleId, consecutiveVip + 1);
            createMatch(vipWs, femaleWs, level, "ENGLISH");
            return;
        }
    }

    queues.english[level].push({ ws: femaleWs, joinedAt: Date.now() });
    tryEnglishMatch(level);
}

function tryRegionalMatch(lang) {
    const queue = queues.regional[lang];
    if (!queue || queue.length < 2) return;

    for (let i = 0; i < queue.length; i++) {
        for (let j = i + 1; j < queue.length; j++) {
            const itemA = queue[i];
            const itemB = queue[j];

            if (itemA.ws.readyState === WebSocket.OPEN && itemB.ws.readyState === WebSocket.OPEN) {
                if (isEligiblePair(itemA, itemB)) {
                    queue.splice(j, 1);
                    queue.splice(i, 1);
                    createMatch(itemA.ws, itemB.ws, "Native", lang);
                    return;
                }
            }
        }
    }
}

// Rule 10: 5-Second Cross-Level Fallback Checker
setInterval(() => {
    const now = Date.now();
    const beg = queues.english.Beginner;
    const adv = queues.english.Advanced;

    for (let i = 0; i < beg.length; i++) {
        if (now - beg[i].joinedAt >= 5000) {
            for (let j = 0; j < adv.length; j++) {
                if (now - adv[j].joinedAt >= 5000) {
                    const itemA = beg[i];
                    const itemB = adv[j];
                    if (isEligiblePair(itemA, itemB)) {
                        beg.splice(i, 1);
                        adv.splice(j, 1);
                        createMatch(itemA.ws, itemB.ws, "General", "ENGLISH");
                        return;
                    }
                }
            }
        }
    }
}, 2000);

// Rule 23: Ghost socket purger (every 25 seconds)
setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws.isAlive) {
            removeFromAllQueues(ws);
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 25000);

wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.userId = "user_" + Math.random().toString(36).substring(2, 9);
    ws.on('pong', () => { ws.isAlive = true; });

    log("WS", `Client connected: ${ws.userId}`);

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.action) {
                case 'join_queue': {
                    removeFromAllQueues(ws);
                    ws.level = data.level || "Beginner";
                    ws.language = data.language || "ENGLISH";
                    ws.gender = data.gender || "MALE";
                    ws.femaleOnly = !!data.femaleOnly;
                    ws.isVip = !!data.isVip;

                    if (ws.language === "ENGLISH") {
                        if (ws.femaleOnly && (ws.isVip || data.hasFemalePass)) {
                            queues.vipFemale.push({ ws: ws, joinedAt: Date.now() });
                            log("QUEUE", `VIP User ${ws.userId} joined pool_english_vip_female`);
                        } else if (ws.gender === "FEMALE") {
                            handleFemaleMatchmaking(ws, ws.level);
                        } else {
                            if (!queues.english[ws.level]) queues.english[ws.level] = [];
                            queues.english[ws.level].push({ ws: ws, joinedAt: Date.now() });
                            log("QUEUE", `User ${ws.userId} joined general ${ws.level}`);
                            tryEnglishMatch(ws.level);
                        }
                    } else {
                        if (!queues.regional[ws.language]) queues.regional[ws.language] = [];
                        queues.regional[ws.language].push({ ws: ws, joinedAt: Date.now() });
                        log("QUEUE", `User ${ws.userId} joined regional [${ws.language}]`);
                        tryRegionalMatch(ws.language);
                    }
                    break;
                }

                case 'leave_queue': {
                    removeFromAllQueues(ws);
                    log("QUEUE", `User ${ws.userId} left queue`);
                    break;
                }

                // Rule 12: Direct Reconnect Handshake
                case 'request_reconnect': {
                    removeFromAllQueues(ws);
                    const targetId = data.targetPeerId;
                    const level = data.level || "Beginner";
                    const requesterId = getSafeUserId(ws);

                    log("RECONNECT", `${requesterId} requesting reconnect to ${targetId}`);

                    // Check if target is online
                    let targetWs = null;
                    wss.clients.forEach(client => {
                        if (getSafeUserId(client) === targetId && client.readyState === WebSocket.OPEN) {
                            targetWs = client;
                        }
                    });

                    if (!targetWs || targetWs.inCall) {
                        ws.send(JSON.stringify({ type: 'reconnect_failed', reason: 'offline_or_busy' }));
                        return;
                    }

                    // Check if target already requested reconnect back to this user (Mutual Handshake)
                    const pending = reconnectRequests.get(requesterId);
                    if (pending && getSafeUserId(pending.requesterWs) === targetId) {
                        reconnectRequests.delete(requesterId);
                        createMatch(ws, targetWs, level, "ENGLISH", true);
                    } else {
                        // Store pending request and notify requester to wait
                        reconnectRequests.set(targetId, { requesterWs: ws, level: level });
                        ws.send(JSON.stringify({ type: 'reconnect_waiting' }));
                    }
                    break;
                }

                case 'cancel_reconnect': {
                    const myId = getSafeUserId(ws);
                    for (const [target, record] of reconnectRequests.entries()) {
                        if (getSafeUserId(record.requesterWs) === myId) {
                            reconnectRequests.delete(target);
                        }
                    }
                    break;
                }

                case 'send_offer': {
                    if (ws.partnerWs && ws.partnerWs.readyState === WebSocket.OPEN) {
                        ws.partnerWs.send(JSON.stringify({ type: 'offer', sdp: data.sdp }));
                    }
                    break;
                }

                case 'send_answer': {
                    if (ws.partnerWs && ws.partnerWs.readyState === WebSocket.OPEN) {
                        ws.partnerWs.send(JSON.stringify({ type: 'answer', sdp: data.sdp }));
                    }
                    break;
                }

                case 'send_ice': {
                    if (ws.partnerWs && ws.partnerWs.readyState === WebSocket.OPEN) {
                        ws.partnerWs.send(JSON.stringify({
                            type: 'ice_candidate',
                            sdpMid: data.sdpMid,
                            sdpMLineIndex: data.sdpMLineIndex,
                            candidate: data.candidate
                        }));
                    }
                    break;
                }

                case 'end_call': {
                    if (ws.partnerWs && ws.partnerWs.readyState === WebSocket.OPEN) {
                        ws.partnerWs.send(JSON.stringify({ type: 'call_ended' }));
                    }
                    if (ws.roomId && activeRooms.has(ws.roomId)) {
                        activeRooms.delete(ws.roomId);
                    }
                    ws.inCall = false;
                    if (ws.partnerWs) ws.partnerWs.inCall = false;
                    ws.partnerWs = null;
                    break;
                }

                case 'report_user': {
                    const reportedId = data.reportedPeerId;
                    const reason = data.reason || "harassment";
                    const now = Date.now();

                    if (!reportRecords.has(reportedId)) {
                        reportRecords.set(reportedId, []);
                    }
                    const list = reportRecords.get(reportedId);
                    list.push({ reason: reason, timestamp: now });

                    if (reason === "not_female") {
                        const genderMismatchCount = list.filter(r => r.reason === "not_female").length;
                        log("REPORT", `Target ${reportedId} has ${genderMismatchCount} gender mismatch flags.`);
                        if (genderMismatchCount >= 3) {
                            wss.clients.forEach(client => {
                                if (getSafeUserId(client) === reportedId) {
                                    client.gender = "MALE";
                                    log("MODERATION", `Account ${reportedId} female status revoked by community reports.`);
                                }
                            });
                        }
                    }

                    const hourReports = list.filter(r => r.reason === "harassment" && (now - r.timestamp) < 3600000);
                    if (hourReports.length >= 5) {
                        wss.clients.forEach(client => {
                            if (getSafeUserId(client) === reportedId && client.readyState === WebSocket.OPEN) {
                                client.send(JSON.stringify({ type: 'server_cooldown', remainingSeconds: 180 }));
                            }
                        });
                        log("MODERATION", `User ${reportedId} locked for 3 minutes (5 reports/hour).`);
                    }
                    break;
                }
            }
        } catch (err) {
            log("ERR", `Error handling message: ${err.message}`);
        }
    });

    ws.on('close', () => {
        removeFromAllQueues(ws);
        if (ws.partnerWs && ws.partnerWs.readyState === WebSocket.OPEN) {
            ws.partnerWs.send(JSON.stringify({ type: 'call_ended' }));
            ws.partnerWs.partnerWs = null;
        }
        if (ws.roomId && activeRooms.has(ws.roomId)) {
            activeRooms.delete(ws.roomId);
        }
        log("WS", `Client disconnected: ${ws.userId}`);
    });
});

server.listen(PORT, '0.0.0.0', () => {
    log("SERVER", `Signaling server running on port ${PORT} (0.0.0.0)`);
});
