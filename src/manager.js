const amqp = require("amqplib");
const fs = require("fs");
const crypto = require("crypto");
// модуль для работы с криптографическими функциями
const csv = require("csv-parser");

const N = 3;

function getStorageNode(date) {
    return parseInt(crypto.createHash("md5").update(date).digest("hex"), 16) % N;
}
// MD5 — это алгоритм хэширования, который преобразует данные в 128-битный хэш
// Создается MD5-хеш от даты, который затем преобразуется 
// в целое число с основанием 16. После этого результат 
// делится на N с остатком, что позволяет определить номер 
// узла хранения (от 0 до N-1)


function parseTestsetDate(datetime_utc) {
    if (!datetime_utc || !datetime_utc.includes("-")) {
        return null;
    }

    const [datePart, utc] = datetime_utc.split("-");
    if (!/^\d{8}$/.test(datePart)) {
        return null;
    }

    const year = datePart.substring(0, 4);
    const month = datePart.substring(4, 6);
    const day = datePart.substring(6, 8);

    return {
        date: `${day}-${month}-${year}`, 
        utc
    };
}

function formatDate(dateString) {
    if (typeof dateString !== "string") {
        dateString = String(dateString);
    }

    if (/^\d{8}$/.test(dateString)) {
        const year = dateString.slice(0, 4);
        const month = dateString.slice(4, 6);
        const day = dateString.slice(6, 8);
        return `${day}-${month}-${year}`;
    }

    const [year, month, day] = dateString.split("-");
    return `${day}-${month}-${year}`;
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
                // conds: cleanValue(row._conds),
                // dewptm: cleanValue(row._dewptm),
                // fog: cleanValue(row._fog),
                // hail: cleanValue(row._hail),
                // heatindexm: cleanValue(row._heatindexm),
                // humidity: cleanValue(row._hum),
                // pressure: cleanValue(row._pressurem),
                // rain: cleanValue(row._rain),
                // snow: cleanValue(row._snow),
                // tempm: cleanValue(row._tempm),
                // vism: cleanValue(row._vism),
                // wdird: cleanValue(row._wdird),
                // wdire: cleanValue(row._wdire),
                // wspdm: cleanValue(row._wspdm)
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

    console.log("Менеджер запущен, ожидает команды...");

    channel.consume("manager", async (msg) => {
        const data = JSON.parse(msg.content.toString());

        if (data.command === "LOAD") {
            const fileName = data.file;
            const fileFormat = data.format;
            console.log(`Загружаем файл: ${fileName} (Формат: ${fileFormat})`);

            fs.createReadStream(fileName)
                .pipe(csv())
                .on("data", async (row) => {
                    const processedData = extractData(row, fileFormat);
                    if (!processedData) return;

                    const storageId = getStorageNode(processedData.date);
                    await channel.sendToQueue(`storage_${storageId}`, Buffer.from(JSON.stringify(processedData)));
                    console.log(`Отправлено в storage_${storageId}: ${processedData.date}`);
                });
        } else if (data.command === "GET") {
            const date = data.date;
            const storageId = getStorageNode(date);
            console.log(`Запрос GET ${date} -> storage_${storageId}`);
            await channel.sendToQueue(`storage_${storageId}_query`, Buffer.from(JSON.stringify({ date })));
        }
    }, { noAck: true });
}

startManager();


