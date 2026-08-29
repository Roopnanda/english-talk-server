const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

const PORT = process.env.PORT || 3000;

// Regular waiting queues grouped by level
const waitingQueues = {
    Beginner: [],
    Intermediate: [],
    Advanced: []
};

// Map to track active reconnect intents: socketId -> targetSocketId
const reconnectRequests = new Map();

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    // --- NORMAL MATCHMAKING QUEUE ---
    socket.on('join_queue', (data) => {
        const { level, userGender, talkToFemaleOnly, isVip } = data;
        
        // Remove any stale reconnect intent when entering normal search
        reconnectRequests.delete(socket.id);

        const targetQueue = waitingQueues[level] || waitingQueues['Intermediate'];
        
        const existingIdx = targetQueue.findIndex(u => u.socket.id === socket.id);
        if (existingIdx !== -1) targetQueue.splice(existingIdx, 1);

        if (targetQueue.length > 0) {
            const partner = targetQueue.shift();
            const roomId = `room_${socket.id}_${partner.socket.id}_${Date.now()}`;

            socket.join(roomId);
            partner.socket.join(roomId);

            socket.emit('match_found', {
                roomId: roomId,
                isInitiator: true,
                peerLevel: partner.level,
                peerId: partner.socket.id,
                isReconnect: false
            });

            partner.socket.emit('match_found', {
                roomId: roomId,
                isInitiator: false,
                peerLevel: level,
                peerId: socket.id,
                isReconnect: false
            });
        } else {
            targetQueue.push({ socket, level, userGender, talkToFemaleOnly, isVip });
        }
    });

    socket.on('leave_queue', () => {
        removeFromAllQueues(socket.id);
        reconnectRequests.delete(socket.id);
    });

    // --- MUTUAL RECONNECT TO LAST CALLER ---
    socket.on('request_reconnect', (data) => {
        const { targetPeerId, myLevel } = data;
        
        removeFromAllQueues(socket.id);

        // Check if the target user already requested to reconnect with this socket
        const partnerTarget = reconnectRequests.get(targetPeerId);

        if (partnerTarget === socket.id) {
            // Mutual match verified
            reconnectRequests.delete(socket.id);
            reconnectRequests.delete(targetPeerId);

            const partnerSocket = io.sockets.sockets.get(targetPeerId);
            if (partnerSocket) {
                const roomId = `reconnect_${socket.id}_${targetPeerId}_${Date.now()}`;
                
                socket.join(roomId);
                partnerSocket.join(roomId);

                socket.emit('match_found', {
                    roomId: roomId,
                    isInitiator: true,
                    peerLevel: partnerSocket.dataLevel || "Intermediate",
                    peerId: targetPeerId,
                    isReconnect: true
                });

                partnerSocket.emit('match_found', {
                    roomId: roomId,
                    isInitiator: false,
                    peerLevel: myLevel || "Intermediate",
                    peerId: socket.id,
                    isReconnect: true
                });
            } else {
                socket.emit('reconnect_failed', { reason: "Partner is offline." });
            }
        } else {
            // Save intent and await partner's mutual button press
            socket.dataLevel = myLevel;
            reconnectRequests.set(socket.id, targetPeerId);
            socket.emit('reconnect_waiting');
        }
    });

    socket.on('cancel_reconnect', () => {
        reconnectRequests.delete(socket.id);
    });

    // --- WEBRTC SIGNALING PASS-THROUGH ---
    socket.on('offer', (data) => {
        socket.to(data.roomId).emit('offer', data.sdp);
    });

    socket.on('answer', (data) => {
        socket.to(data.roomId).emit('answer', data.sdp);
    });

    socket.on('ice_candidate', (data) => {
        socket.to(data.roomId).emit('ice_candidate', data.candidate);
    });

    socket.on('end_call', (data) => {
        if (data && data.roomId) {
            socket.to(data.roomId).emit('call_ended');
            socket.leave(data.roomId);
        }
    });

    socket.on('disconnect', () => {
        removeFromAllQueues(socket.id);
        reconnectRequests.delete(socket.id);
    });
});

function removeFromAllQueues(socketId) {
    for (const lvl in waitingQueues) {
        waitingQueues[lvl] = waitingQueues[lvl].filter(u => u.socket.id !== socketId);
    }
}

server.listen(PORT, () => {
    console.log(`Signaling server running on port ${PORT}`);
});
