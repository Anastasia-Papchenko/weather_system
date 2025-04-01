const amqp = require("amqplib");
const fs = require("fs");
const crypto = require("crypto");
const csv = require("csv-parser");
const { execSync } = require("child_process");

const N = process.argv[2] ? parseInt(process.argv[2], 10) : 3;

const loadedFiles = new Set();             
const storageHealth = new Array(N).fill(true);  
const replicas = new Map(); 
const storageNodesKey = new Map(); 
const isRecovering = Array(N).fill(false);
const pendingResponses = new Map();


// function normalizeDate(dateStr) {
//     if (typeof dateStr !== "string") {
//         dateStr = String(dateStr);
//     }
//     // YYYYMMDD -> YYYY-MM-DD
//     if (/^\d{8}$/.test(dateStr)) {
//         const year = dateStr.slice(0, 4);
//         const month = dateStr.slice(4, 6);
//         const day = dateStr.slice(6, 8);
//         return `${year}-${month}-${day}`;
//     }
//     // DD-MM-YYYY -> YYYY-MM-DD
//     if (/^\d{2}-\d{2}-\d{4}$/.test(dateStr)) {
//         const [d, m, y] = dateStr.split("-");
//         return `${y}-${m}-${d}`;
//     }
//     // Если уже YYYY-MM-DD – возвращаем
//     if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
//         return dateStr;
//     }
//     return dateStr;
// }

function formatDate(dateString) {
    if (typeof dateString !== "string") {
        dateString = String(dateString);
    }

    if (/^\d{2}-\d{2}-\d{4}$/.test(dateString)) {
        return dateString;
    }

    if (/^\d{4}-\d{2}-\d{2}$/.test(dateString)) {
        const [year, month, day] = dateString.split("-");
        return `${day}-${month}-${year}`;
    }

    if (/^\d{8}$/.test(dateString)) {
        const year = dateString.slice(0, 4);
        const month = dateString.slice(4, 6);
        const day = dateString.slice(6, 8);
        return `${day}-${month}-${year}`;
    }

    return dateString;
}

function getStorageNode(date) {
    const hash = crypto.createHash("md5").update(date).digest();
    let hashInt = hash.readUInt32BE(0);

    let attempts = 0;
    while (!storageHealth[hashInt % N] && attempts < N) {
        hashInt++;
        attempts++;
    }

    const selectedNode = hashInt % N;
    if (!storageHealth[selectedNode]) {
        // Если так и не нашли, пытаемся fallback
        return findNextHealthyStorage(selectedNode);
    }
    return selectedNode;
}

function findNextHealthyStorage(fromId) {
    for (let offset = 0; offset < N; offset++) {
        const candidate = (fromId + offset) % N;
        if (storageHealth[candidate]) {
            return candidate;
        }
    }
    return null;  // все упали
}

function findNextClockwiseHealthy(fromId, exclude = []) {
    for (let offset = 1; offset < N; offset++) {
        const candidate = (fromId + offset) % N;
        if (storageHealth[candidate] && !exclude.includes(candidate)) {
            return candidate;
        }
    }
    return null;
}

function findClosestAliveNode(failedId) {
    for (let offset = 1; offset < N; offset++) {
        const candidate = (failedId + offset) % N;
        if (storageHealth[candidate]) return candidate;
    }
    return null;
}

async function monitorStorages(channel) {
    const pendingHealthChecks = new Map();

    setInterval(() => {
        for (let i = 0; i < N; i++) {
            if (!storageHealth[i]) continue; 
            const correlationId = `healthCheck_${i}_${Date.now()}`;

            channel.sendToQueue(`storage_${i}_query`, Buffer.from(JSON.stringify({
                healthCheck: true,
                correlationId
            })));

            const timeout = setTimeout(() => {
                const info = pendingHealthChecks.get(correlationId);
                if (!info) return;

                const nodeId = info.nodeId;
                if (!storageHealth[nodeId] && !isRecovering[nodeId]) {
                    isRecovering[nodeId] = true; 
                    console.log(`[monitorStorages] Хранитель ${nodeId} не ответил. Считается упавшим. Начинаем восстановление...`);
                    
                    recoverStorageData(nodeId, channel).finally(() => {
                        isRecovering[nodeId] = false;
                    });
                }
                

                pendingHealthChecks.delete(correlationId);
            }, 2000);

            pendingHealthChecks.set(correlationId, { nodeId: i, timeout });
        }
    }, 5000);


    for (let i = 0; i < N; i++) {
        await channel.assertQueue(`storage_${i}_response`, { durable: false });

        channel.consume(`storage_${i}_response`, (msg) => {
            const response = JSON.parse(msg.content.toString());
            const correlationId = response.correlationId;
            if (!correlationId) return;

            const info = pendingHealthChecks.get(correlationId);
            if (!info) return;

            const nodeId = info.nodeId;
            clearTimeout(info.timeout);
            pendingHealthChecks.delete(correlationId);

            if (!storageHealth[nodeId]) {
                storageHealth[nodeId] = true;
                console.log(`[monitorStorages] Хранитель ${nodeId} снова отвечает.`);
            }
        }, { noAck: true });
    }
}

function findClosestAliveNode(failedId) {
    const alive = [];
    for (let i = 0; i < N; i++) {
        if (storageHealth[i]) alive.push(i);
    }
    if (alive.length === 0) return null;

    return alive.reduce((a, b) =>
        Math.abs(b - failedId) < Math.abs(a - failedId) ? b : a
    );
}

async function recoverStorageData(failedId, channel) {
    let somethingTransferred = false;

    console.log(`[recoverStorageData] Начинаем восстановление данных узла ${failedId}...`);
    const affectedDates = [];

    // console.log(`[DEBUG] Перед восстановлением: replicas содержит`, [...replicas.entries()].slice(0, 5));
    // console.log(`[DEBUG] Ищем восстановление по дате 31-12-2015 →`, replicas.get("31-12-2015"));
    

    for (const [date, [primary, replica]] of replicas.entries()) {
        if (!storageHealth[primary] && !storageHealth[replica]) {
            console.log(`  Оба узла (${primary}, ${replica}) мертвы для даты ${date} - удаляем ключ`);
            replicas.delete(date);
            continue;
        }
        if (primary !== failedId && replica !== failedId) continue;

        affectedDates.push({ date, primary, replica });
    }

    if (affectedDates.length === 0) {
        console.log(`  У узла ${failedId} не было данных для восстановления.`);
        return;
    }

    for (const { date, primary, replica } of affectedDates) {
        const sourceId = (failedId === primary) ? replica : primary;
        if (!storageHealth[sourceId]) {
            console.log(`  Данные за ${date} утеряны: оба узла ${primary}, ${replica} мертвы`);
            replicas.delete(date);
            continue;
        }

        // const newPrimary = findNextClockwiseHealthy(failedId, []);

        const newPrimary = findClosestAliveNode(failedId);
        if (newPrimary === null) {
            console.log(`  Нет здоровых узлов, чтобы хранить дату ${date}`);
            continue;
        }

        const newReplica = findNextClockwiseHealthy(newPrimary, [newPrimary]);

        const correlationId = `recover_${date}_${Date.now()}`;
        const responsePromise = new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                console.log(`  [recoverStorageData] Время ожидания ответа от узла ${sourceId} истекло для даты ${date}`);
                resolve(null);
            }, 3000);

            pendingResponses.set(correlationId, (response) => {
                clearTimeout(timeout);
                resolve(response);
            });
        });

        await channel.sendToQueue(`storage_${sourceId}_query`, Buffer.from(JSON.stringify({
            getAllData: true,
            correlationId
        })), {
            correlationId,
            replyTo: 'manager_response'
        });

        const response = await responsePromise;
        if (!response || !response.allData) {
            continue;
        }

        const storageData = response.allData.storageData || {};
        const formattedDate = formatDate(date);
        const records = storageData[formattedDate] || [];


        if (records.length === 0) {
            console.log(`  Нет записей по дате ${date} на узле source_${sourceId}`);
            continue;
        }

        for (const record of records) {
            await channel.sendToQueue(`storage_${newPrimary}`, Buffer.from(JSON.stringify(record)));
            if (newReplica !== null && storageHealth[newReplica]) {
                await channel.sendToQueue(`storage_${newReplica}`, Buffer.from(JSON.stringify(record)));
            }
        }

        
        replicas.set(formattedDate, [newPrimary, newReplica]);
        // console.log(`[DEBUG][RECOVERY] Устанавливаем: replicas['${formattedDate}'] = [${newPrimary}, ${newReplica}]`);


        somethingTransferred = true;
        if (!storageNodesKey.has(newPrimary)) {
            storageNodesKey.set(newPrimary, []);
        }
        storageNodesKey.get(newPrimary).push(failedId);
        
    }

    if (somethingTransferred) {
        console.log(`[recoverStorageData] Данные перенесены на новые узлы (из упавшего узла ${failedId}).`);
    } else {
        console.log(`[recoverStorageData] Нечего было переносить (узел ${failedId}).`);
    }
}


async function shutdownStorage(storageId, channel) {
    console.log(`Отключаем хранитель ${storageId}...`);
    try {
        const processes = execSync(`wmic process where "name='node.exe'" get ProcessId,CommandLine /FORMAT:LIST`)
            .toString();

        let pid = null;
        const lines = processes.trim().split("\n");
        for (let i = 0; i < lines.length; i++) {
            if (lines[i].includes(`storage.js ${storageId}`)) {
                pid = lines[i + 1]?.match(/\d+/)?.[0];
                break;
            }
        }

        if (!pid || isNaN(parseInt(pid, 10))) {
            throw new Error(`Не удалось найти PID для storage_${storageId}`);
        }

        execSync(`taskkill /PID ${pid} /F`);
        console.log(`Хранитель ${storageId} завершился.`);

        storageHealth[storageId] = false;
  
        if (!isRecovering[storageId]) {
            isRecovering[storageId] = true;
            await recoverStorageData(storageId, channel);
            isRecovering[storageId] = false;
        }
        
    } catch (error) {
        console.log(`Ошибка при завершении хранителя ${storageId}:`, error.message);
    }
}

function cleanValue(value) {
    return value === "" || value === "N/A" ? null : value;
}


function extractData(row, format) {
    let extractedData;

    switch (format) {
        case "seattle-weather":
            extractedData = {
                date: formatDate(row.date),
                precipitation: cleanValue(row.precipitation),
                temp_max: cleanValue(row.temp_max),
                temp_min: cleanValue(row.temp_min),
                wind: cleanValue(row.wind),
                weather: cleanValue(row.weather)
            };
            break;
        case "testset":
            const parsedDate = parseTestsetDate(row.datetime_utc);

            extractedData = {
                date: parsedDate.date,
                utc: parsedDate.utc,
                data: row
            };
            break;
        case "weather_prediction_dataset":
            extractedData = {
                date: formatDate(row.DATE),
                month: row.MONTH,
                base_cloud_cover: cleanValue(row.BASEL_cloud_cover),
                base_humidity: cleanValue(row.BASEL_humidity),
                base_pressure: cleanValue(row.BASEL_pressure),
                base_global_radiation: cleanValue(row.BASEL_global_radiation),
                base_precipitation: cleanValue(row.BASEL_precipitation),
                base_temp_mean: cleanValue(row.BASEL_temp_mean),
                base_temp_min: cleanValue(row.BASEL_temp_min),
                base_temp_max: cleanValue(row.BASEL_temp_max)
            };
            break;
        case "weather":
            extractedData = {
                date: formatDate(row["Date.Full"]),
                precipitation: cleanValue(row["Data.Precipitation"]),
                month: row["Date.Month"],
                city: cleanValue(row["Station.City"]),
                station_code: cleanValue(row["Station.Code"]),
                avg_temp: cleanValue(row["Data.Temperature.Avg Temp"]),
                wind_speed: cleanValue(row["Data.Wind.Speed"])
            };
            break;
        default:
            return null;
    }

    if (!extractedData.date || Object.values(extractedData).some(value => value === undefined || value === null)) {
        console.log("Ошибка обработки строки:", JSON.stringify(row));
        return null;
    }

    return extractedData;
}

async function startManager() {
    const connection = await amqp.connect("amqp://localhost:5672");
    const channel = await connection.createChannel();

    await channel.assertQueue("manager", { durable: false });
    await channel.assertQueue("manager_response", { durable: false });


    monitorStorages(channel);
    console.log("Менеджер запущен, ожидает команды...");


    channel.consume("manager", async (msg) => {
        const data = JSON.parse(msg.content.toString());

        if (data.command === "LOAD") {
            const fileName = data.file;
            const fileFormat = data.format;

            if (loadedFiles.has(fileName)) {
                console.log(`Файл ${fileName} уже записан`);
                return;
            }

            console.log(`Загружаем файл: ${fileName} (Формат: ${fileFormat})`);
            loadedFiles.add(fileName);

            fs.createReadStream(fileName)
                .pipe(csv())
                .on("data", async (row) => {
                    const processedData = extractData(row, fileFormat);
                    if (!processedData) return;

                    const formattedDate = formatDate(processedData.date);
                    processedData.date = formattedDate;

                    const primaryId = getStorageNode(formattedDate, N);
                    const replicaId = (primaryId + 1) % N;

                    replicas.set(formattedDate, [primaryId, replicaId]);

                    await channel.assertQueue(`storage_${primaryId}`, { durable: false });
                    await channel.assertQueue(`storage_${replicaId}`, { durable: false });


                    await channel.sendToQueue(`storage_${primaryId}`, Buffer.from(JSON.stringify(processedData)));
                    await channel.sendToQueue(`storage_${replicaId}`, Buffer.from(JSON.stringify(processedData)));


                    console.log(`Отправлено в storage_${primaryId}: ${formattedDate}`);
                });

        } else if (data.command === "GET") {
            const originalDate = data.date;
            const formattedDate = formatDate(originalDate);
        
            console.log(`[DEBUG][GET] Запрос: ${originalDate} → formatted: ${formattedDate}`);
        
            const replicaStorageIds = replicas.get(formattedDate);
        
            if (replicaStorageIds && replicaStorageIds.length > 0) {
                let found = false;
        
                for (const storageId of replicaStorageIds) {
                    if (storageHealth[storageId]) {
                        console.log(`Запрос GET ${formattedDate} -> storage_${storageId}`);
                        await channel.sendToQueue(`storage_${storageId}_query`, Buffer.from(JSON.stringify({ date: formattedDate })));
                        found = true;
                        break;
                    }
                }
        
                if (!found) {
                    console.log(`[DEBUG][GET] Все реплики мертвы для даты ${formattedDate}`);
                    await channel.sendToQueue("client", Buffer.from(JSON.stringify({
                        error: `Нет доступных реплик для даты ${formattedDate}`
                    })));
                }
        
            } else {
                for (const [newId, oldIds] of storageNodesKey.entries()) {
                    const [primary, replica] = replicas.get(formattedDate) || [];
        
                    if (oldIds.includes(primary) || oldIds.includes(replica)) {
                        if (storageHealth[newId]) {
                            console.log(`Данные за ${formattedDate} перенесены ранее. Используем восстановленный узел ${newId}`);
                            await channel.sendToQueue(`storage_${newId}_query`, Buffer.from(JSON.stringify({ date: formattedDate })));
                            return;
                        }
                    }
                }
        
                const fallbackId = getStorageNode(formattedDate, N);
                if (storageHealth[fallbackId]) {
                    console.log(`[DEBUG][GET] fallback -> storage_${fallbackId}`);
                    await channel.sendToQueue(`storage_${fallbackId}_query`, Buffer.from(JSON.stringify({ date: formattedDate })));
                } else {
                    const altId = findNextHealthyStorage(fallbackId);
                    if (altId !== null) {
                        console.log(`[DEBUG][GET] fallback alt -> storage_${altId}`);
                        await channel.sendToQueue(`storage_${altId}_query`, Buffer.from(JSON.stringify({ date: formattedDate })));
                    } else {
                        console.log(`[DEBUG][GET] Нет живых узлов для даты ${formattedDate}`);
                        await channel.sendToQueue("client", Buffer.from(JSON.stringify({
                            error: `Нет доступных хранилищ для даты ${formattedDate}`
                        })));
                    }
                }
            }
        }
        
        else if (data.command === "SHUTDOWN") {
            console.log(`Получена команда SHUTDOWN для хранителя ${data.storageId}`);
            await shutdownStorage(data.storageId, channel);
        }
    }, { noAck: true });

    channel.consume("manager_response", (msg) => {
        const correlationId = msg.properties.correlationId;
        const response = JSON.parse(msg.content.toString());
    
        if (pendingResponses.has(correlationId)) {
            const handler = pendingResponses.get(correlationId);
            handler(response);
            pendingResponses.delete(correlationId);
            return;
        } else if (correlationId) {
            console.log(`Неизвестный correlationId: ${correlationId}`);
        }
    
        if (response.error && !response.error.includes("не найдены")) {
            console.log(`Ошибка: ${response.error}`);
        } else if (response.data && response.data.length > 0) {
        } else {
            console.log("Данных в хранилище нет, но это не ошибка.");
        }
    
        channel.sendToQueue("client", Buffer.from(JSON.stringify(response)));
    }, { noAck: true });
    

}

startManager();