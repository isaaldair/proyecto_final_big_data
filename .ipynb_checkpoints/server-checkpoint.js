const express = require("express");
const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");

const app = express();
const PORT = 3000;

const AGG_DIR = "output_realtime_analysis/aggregated_data";
const CLEAN_DIR = "output_realtime_analysis/clean_data";

app.use(express.static("public"));

function readCSVFolder(folder) {
    return new Promise(resolve => {
        if (!fs.existsSync(folder)) return resolve([]);
        const files = fs.readdirSync(folder).filter(f => f.endsWith(".csv"));
        if (files.length === 0) return resolve([]);

        const rows = [];
        let pending = files.length;

        files.forEach(file => {
            fs.createReadStream(path.join(folder, file))
                .pipe(csv())
                .on("data", r => rows.push(r))
                .on("end", () => {
                    pending--;
                    if (pending === 0) resolve(rows);
                });
        });
    });
}

app.get("/api/dashboard", async (req, res) => {
    const agg = await readCSVFolder(AGG_DIR);
    const clean = await readCSVFolder(CLEAN_DIR);

    // TOTAL SALES
    const totalSales = agg.reduce(
        (s, r) => s + Number(r.total_sales_zone || 0), 0
    );

    // TOP ZONES
    const byZone = {};
    agg.forEach(r => {
        byZone[r.pickup_zone] =
            (byZone[r.pickup_zone] || 0) + Number(r.total_sales_zone || 0);
    });

    const topZones = Object.entries(byZone)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([zone, sales]) => ({ zone, sales }));

    // SALES BY HOUR
    const byHour = {};
    agg.forEach(r => {
        byHour[r.hour] =
            (byHour[r.hour] || 0) + Number(r.total_sales_zone || 0);
    });

    // LATENCY STATS
    const latencies = clean
        .filter(r => r.sent_at)
        .map(r => {
            const sent = new Date(r.sent_at).getTime();
            const recv = new Date(r.pickup_datetime).getTime();
            return recv - sent;
        })
        .filter(v => v >= 0);

    latencies.sort((a, b) => a - b);

    const p = q => latencies.length
        ? latencies[Math.floor(q * latencies.length)]
        : 0;

    res.json({
        totalSales,
        topZones,
        salesByHour: byHour,
        latency: {
            avg: latencies.length
                ? Math.round(latencies.reduce((a,b)=>a+b,0)/latencies.length)
                : 0,
            p50: p(0.50),
            p95: p(0.95),
            p99: p(0.99)
        }
    });
});

app.listen(PORT, () => {
    console.log(`Dashboard disponible en http://localhost:${PORT}`);
});
