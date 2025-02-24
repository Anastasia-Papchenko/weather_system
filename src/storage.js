const amqp = require('amqplib');

const STORAGE_ID = process.argv[2] || 0;
// node storage.js [0,1,2]
let storageData = {};

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

async function startStorage() {
    const connection = await amqp.connect('amqp://localhost:5672');
    const channel = await connection.createChannel();

    await channel.assertQueue(`storage_${STORAGE_ID}`, { durable: false });
    await channel.assertQueue(`storage_${STORAGE_ID}_query`, { durable: false });

    console.log(`Хранитель ${STORAGE_ID} запущен...`);

    channel.consume(`storage_${STORAGE_ID}`, (msg) => {
        const data = JSON.parse(msg.content.toString());
        if (!storageData[data.date]) {
            storageData[data.date] = [];
        }
        storageData[data.date].push(data);
    }, { noAck: true });

    channel.consume(`storage_${STORAGE_ID}_query`, async (msg) => {
        const query = JSON.parse(msg.content.toString());
        const responseQueue = 'client';

        if (storageData[query.date] && storageData[query.date].length > 0) {
            const formattedDate = formatDate(query.date);
            const response = JSON.stringify({ 
                date: formattedDate, 
                data: storageData[query.date] 
            });
            await channel.sendToQueue(responseQueue, Buffer.from(response));
        } else {
            const formattedDate = formatDate(query.date);
            const response = JSON.stringify({ 
                error: `Данные за ${formattedDate} не найдены` 
            });
            await channel.sendToQueue(responseQueue, Buffer.from(response));
            console.log(`[Хранитель ${STORAGE_ID}] Данные не найдены: ${formattedDate}`);
        }
    }, { noAck: true });
}

startStorage();
