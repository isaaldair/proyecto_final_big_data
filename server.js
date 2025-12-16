const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { Kafka } = require("kafkajs");

// ======================================
// CONFIG
// ======================================

const KAFKA_BROKER = "100.68.89.127:9092";
const TOPICS = ["uber_trips_clean", "uber_trips_prod"];
const GROUP_ID = "web-realtime-monitor";

const PORT = 3000;

// ======================================
// EXPRESS + SOCKET
// ======================================

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

io.on("connection", (socket) => {
    console.log("🟢 Cliente web conectado");
});

// ======================================
// KAFKA CONSUMER
// ======================================

const kafka = new Kafka({
    clientId: "kafka-web-monitor",
    brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

async function startKafka() {
    await consumer.connect();

    for (const topic of TOPICS) {
        await consumer.subscribe({ topic, fromBeginning: false });
    }

    console.log("✅ Conectado a Kafka");
    console.log("📡 Escuchando tópicos:", TOPICS);

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            try {
                const value = JSON.parse(message.value.toString());

                const payload = {
                    topic,
                    timestamp: new Date().toISOString(),
                    data: value,
                };

                // 🔥 ENVÍO EN TIEMPO REAL AL NAVEGADOR
                io.emit("kafka_event", payload);

                console.log(
                    `📩 ${topic} | Trip ${value.index_trip ?? "N/A"}`
                );
            } catch (err) {
                console.error("❌ Error procesando mensaje", err);
            }
        },
    });
}

// ======================================
// START
// ======================================

server.listen(PORT, () => {
    console.log(`🌐 Web monitor activo: http://localhost:${PORT}`);
    startKafka().catch(console.error);
});
