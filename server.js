const express = require("express");
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
    server,
    path: "/players"
});

// serverAddress -> Set<WebSocket>
const serverGroups = new Map();

// Simple health page
app.get("/", (req, res) => {
    res.send("Player V1 WebSocket backend is running.");
});

app.get("/status", (req, res) => {
    let connectedPlayers = 0;

    for (const group of serverGroups.values()) {
        connectedPlayers += group.size;
    }

    res.json({
        online: true,
        connectedPlayers,
        serverGroups: serverGroups.size
    });
});

function generateSessionId() {
    return crypto.randomBytes(32).toString("hex");
}

function normalizeServerAddress(address) {
    if (!address || typeof address !== "string") {
        return null;
    }

    let result = address.trim().toLowerCase();

    if (!result.includes(":")) {
        result += ":25565";
    }

    return result;
}

function send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function broadcastToGroup(serverAddress, sender, data) {
    const group = serverGroups.get(serverAddress);

    if (!group) {
        return;
    }

    for (const client of group) {
        if (
            client !== sender &&
            client.readyState === WebSocket.OPEN
        ) {
            client.send(JSON.stringify(data));
        }
    }
}

function validNumber(value) {
    return typeof value === "number" && Number.isFinite(value);
}

function validatePositionUpdate(data) {
    if (!validNumber(data.x)) return false;
    if (!validNumber(data.y)) return false;
    if (!validNumber(data.z)) return false;
    if (!validNumber(data.yaw)) return false;
    if (!validNumber(data.pitch)) return false;

    if (typeof data.dimension !== "string") return false;

    return true;
}

wss.on("connection", (ws, req) => {
    console.log("WebSocket client connected.");

    ws.authenticated = false;
    ws.sessionId = null;
    ws.playerId = null;
    ws.playerName = null;
    ws.serverAddress = null;
    ws.lastPlayerData = null;

    // Basic rate limiting
    ws.messageCount = 0;
    ws.rateLimitStarted = Date.now();

    ws.on("message", (rawMessage) => {
        try {
            /*
             * Basic rate limit:
             * max 100 messages per 10 seconds
             */
            const now = Date.now();

            if (now - ws.rateLimitStarted >= 10000) {
                ws.messageCount = 0;
                ws.rateLimitStarted = now;
            }

            ws.messageCount++;

            if (ws.messageCount > 100) {
                send(ws, {
                    type: "error",
                    error: "Rate limit exceeded"
                });

                return;
            }

            if (rawMessage.length > 65536) {
                send(ws, {
                    type: "error",
                    error: "Message too large"
                });

                return;
            }

            const data = JSON.parse(rawMessage.toString());

            if (!data || typeof data !== "object") {
                return;
            }

            /*
             * ========================
             * AUTHENTICATION
             * ========================
             */
            if (data.type === "auth") {

                if (ws.authenticated) {
                    send(ws, {
                        type: "auth_error",
                        error: "Already authenticated"
                    });

                    return;
                }

                if (
                    typeof data.player_id !== "string" ||
                    typeof data.player_name !== "string" ||
                    typeof data.server_address !== "string"
                ) {
                    send(ws, {
                        type: "auth_error",
                        error: "Invalid authentication data"
                    });

                    return;
                }

                const serverAddress =
                    normalizeServerAddress(data.server_address);

                if (!serverAddress) {
                    send(ws, {
                        type: "auth_error",
                        error: "Invalid server address"
                    });

                    return;
                }

                ws.authenticated = true;
                ws.sessionId = generateSessionId();
                ws.playerId = data.player_id;
                ws.playerName = data.player_name;
                ws.serverAddress = serverAddress;

                if (!serverGroups.has(serverAddress)) {
                    serverGroups.set(
                        serverAddress,
                        new Set()
                    );
                }

                const group =
                    serverGroups.get(serverAddress);

                /*
                 * Tell the newly-connected player
                 * about players already connected.
                 */
                for (const client of group) {

                    if (
                        client.authenticated &&
                        client.lastPlayerData &&
                        client.readyState === WebSocket.OPEN
                    ) {
                        send(ws, {
                            type: "player_join",
                            player_data:
                                client.lastPlayerData
                        });
                    }
                }

                group.add(ws);

                send(ws, {
                    type: "auth_success",
                    session_id: ws.sessionId
                });

                console.log(
                    `${ws.playerName} authenticated on ${serverAddress}`
                );

                return;
            }

            /*
             * Everything below requires authentication
             */
            if (!ws.authenticated) {
                send(ws, {
                    type: "auth_error",
                    error: "Not authenticated"
                });

                return;
            }

            /*
             * ========================
             * POSITION UPDATE
             * ========================
             */
            if (data.type === "position_update") {

                if (data.session_id !== ws.sessionId) {
                    send(ws, {
                        type: "error",
                        error: "Invalid session"
                    });

                    return;
                }

                if (data.player_id !== ws.playerId) {
                    send(ws, {
                        type: "error",
                        error: "Player ID mismatch"
                    });

                    return;
                }

                if (!validatePositionUpdate(data)) {
                    send(ws, {
                        type: "error",
                        error: "Invalid position update"
                    });

                    return;
                }

                /*
                 * Never trust the supplied username/session.
                 * Replace them with authenticated values.
                 */
                const playerData = {
                    type: "position_update",

                    session_id: ws.sessionId,

                    player_id: ws.playerId,
                    player_name: ws.playerName,

                    x: data.x,
                    y: data.y,
                    z: data.z,

                    yaw: data.yaw,
                    pitch: data.pitch,

                    dimension:
                        data.dimension,

                    on_ground:
                        Boolean(data.on_ground),

                    sneaking:
                        Boolean(data.sneaking),

                    sprinting:
                        Boolean(data.sprinting),

                    swimming:
                        Boolean(data.swimming),

                    using_elytra:
                        Boolean(data.using_elytra),

                    helmet:
                        data.helmet || {
                            item: "",
                            count: 0
                        },

                    chestplate:
                        data.chestplate || {
                            item: "",
                            count: 0
                        },

                    leggings:
                        data.leggings || {
                            item: "",
                            count: 0
                        },

                    boots:
                        data.boots || {
                            item: "",
                            count: 0
                        },

                    main_hand:
                        data.main_hand || {
                            item: "",
                            count: 0
                        },

                    off_hand:
                        data.off_hand || {
                            item: "",
                            count: 0
                        },

                    timestamp:
                        Date.now()
                };

                /*
                 * First position update means we now
                 * have enough information to announce
                 * this player.
                 */
                const firstUpdate =
                    ws.lastPlayerData === null;

                ws.lastPlayerData = playerData;

                if (firstUpdate) {

                    broadcastToGroup(
                        ws.serverAddress,
                        ws,
                        {
                            type: "player_join",
                            player_data:
                                playerData
                        }
                    );

                } else {

                    broadcastToGroup(
                        ws.serverAddress,
                        ws,
                        {
                            type: "player_update",
                            player_data:
                                playerData
                        }
                    );
                }

                return;
            }

            /*
             * ========================
             * PING
             * ========================
             */
            if (data.type === "ping") {

                send(ws, {
                    type: "pong",
                    timestamp: Date.now()
                });

                return;
            }

        } catch (error) {

            console.error(
                "Invalid message:",
                error.message
            );

            send(ws, {
                type: "error",
                error: "Invalid message"
            });
        }
    });

    ws.on("close", () => {

        console.log(
            `Client disconnected: ${
                ws.playerName || "unknown"
            }`
        );

        if (!ws.serverAddress) {
            return;
        }

        const group =
            serverGroups.get(ws.serverAddress);

        if (!group) {
            return;
        }

        group.delete(ws);

        if (ws.playerId) {

            broadcastToGroup(
                ws.serverAddress,
                ws,
                {
                    type: "player_leave",
                    player_id:
                        ws.playerId
                }
            );
        }

        if (group.size === 0) {
            serverGroups.delete(
                ws.serverAddress
            );
        }
    });

    ws.on("error", (error) => {
        console.error(
            "WebSocket error:",
            error.message
        );
    });
});

const PORT =
    process.env.PORT || 3000;

server.listen(
    PORT,
    "0.0.0.0",
    () => {
        console.log(
            `Player V1 backend running on port ${PORT}`
        );

        console.log(
            `WebSocket endpoint: /players`
        );
    }
);
