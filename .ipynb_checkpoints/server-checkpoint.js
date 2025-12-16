/**
 * Consumer: Node.js Realtime Monitor
 * Rol:
 *  - Consumir eventos Kafka en tiempo real
 *  - NO persistir datos
 *  - Emitir eventos enriquecidos al dashboard web
 *  - Permitir análisis de:
 *      - Volumen por producer
 *      - Latencia
 *      - Calidad de datos
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { Kafka } = require("kafkajs");

// =====================================================
// CONFIGURACIÓN GENERAL
// =====================================================

const KAFKA_BROKER = "100.68.89.127:9092";
const TOPICS = ["uber_trips_clean", "uber_trips_prod"];
const GROUP_ID = "nodejs-realtime-monitor";

const PORT = 3000;
const CONSUMER_NAME = "Node.js Realtime Consumer";

// =====================================================
// EXPRESS + SOCKET.IO
// =====================================================

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

io.on("connection", (socket) => {
    console.log("Cliente web conectado al dashboard");
    socket.emit("consumer_info", {
        consumer: CONSUMER_NAME,
        topics: TOPICS,
    });
});

// =====================================================
// KAFKA CONSUMER
// =====================================================

const kafka = new Kafka({
    clientId: "uber-realtime-dashboard",
    brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

async function startKafkaConsumer() {
    await consumer.connect();

    for (const topic of TOPICS) {
        await consumer.subscribe({
            topic,
            fromBeginning: false,
        });
    }

    console.log("Kafka consumer conectado");
    console.log("Topics suscritos:", TOPICS);

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const rawValue = message.value.toString();
                const data = JSON.parse(rawValue);

                /**
                 * Enriquecimiento del evento
                 * - _producer : topic Kafka (simula producer)
                 * - _consumer : consumer actual
                 * - _received_at : timestamp recepción
                 */
                const event = {
                    ...data,
                    _producer: topic,
                    _consumer: CONSUMER_NAME,
                    _received_at: new Date().toISOString(),
                };

                io.emit("kafka_event", event);

                console.log(
                    `[${topic}] Trip ${data.index_trip ?? "N/A"} recibido`
                );

            } catch (err) {
                console.error("Error procesando mensaje Kafka:", err);
            }
        },
    });
}

// =====================================================
// START SERVER
// =====================================================

server.listen(PORT, () => {
    console.log(`Dashboard activo en http://localhost:${PORT}`);
    startKafkaConsumer().catch((err) => {
        console.error("Error iniciando Kafka consumer:", err);
    });
});
