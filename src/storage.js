const amqp = require('amqplib');

const STORAGE_ID = process.argv[2] || 0;
let storageData = {};


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

async function startStorage() {
    const connection = await amqp.connect('amqp://localhost:5672');
    const channel = await connection.createChannel();

    await channel.assertQueue(`storage_${STORAGE_ID}`, { durable: false });
    await channel.assertQueue(`storage_${STORAGE_ID}_query`, { durable: false });
    await channel.assertQueue(`storage_${STORAGE_ID}_response`, { durable: false });

    console.log(`Хранитель ${STORAGE_ID} запущен...`);

    channel.consume(`storage_${STORAGE_ID}`, (msg) => {
        const data = JSON.parse(msg.content.toString());
        if (!data.date) return;

        const formattedDate = formatDate(data.date);

        if (!storageData[formattedDate]) {
            storageData[formattedDate] = [];
        }

        const alreadyExists = storageData[formattedDate].some(r =>
            JSON.stringify(r) === JSON.stringify(data)
        );
        if (!alreadyExists) {
            storageData[formattedDate].push(data);
            // console.log(`[storage_${STORAGE_ID}] Получены данные за ${formattedDate}`);
        }
    }, { noAck: true });

    channel.consume(`storage_${STORAGE_ID}_query`, async (msg) => {
        const query = JSON.parse(msg.content.toString());
        const responseQueue = "manager_response";

        if (query.getAllData && query.correlationId) {
            const replyQueue = msg.properties.replyTo || `storage_${STORAGE_ID}_response`;
            // console.log(`[DEBUG] Ответ на getAllData:`, Object.keys(storageData));

            await channel.sendToQueue(replyQueue, Buffer.from(JSON.stringify({
                correlationId: query.correlationId,
                storageId: STORAGE_ID,
                allData: { storageData }
            })), {
                correlationId: query.correlationId
            });

            return;
        }

        if (query.healthCheck && query.correlationId) {
            await channel.sendToQueue(`storage_${STORAGE_ID}_response`,
                Buffer.from(JSON.stringify({ status: "alive", correlationId: query.correlationId }))
            );
            return;
        }

        if (query.recoverData && query.target !== undefined) {
            console.log(`Отправка данных из storage_${STORAGE_ID} в storage_${query.target}`);
            const targetQueue = `storage_${query.target}`;
            let dataTransferred = false;

            try {
                for (const date in storageData) {
                    for (const record of storageData[date]) {
                        await channel.sendToQueue(targetQueue, Buffer.from(JSON.stringify(record)));
                        dataTransferred = true;
                    }
                }

                if (dataTransferred) {
                    console.log(`Данные из storage_${STORAGE_ID} успешно отправлены в storage_${query.target}`);
                } else {
                    console.log(`Нет данных для передачи из storage_${STORAGE_ID}`);
                }

                await channel.sendToQueue(`storage_${STORAGE_ID}_response`,
                    Buffer.from(JSON.stringify({ status: "recovery_complete" }))
                );
            } catch (error) {
                console.log(`Ошибка при пересылке данных: ${error.message}`);
            }

            return;
        }

        if (query.clearReplica && query.date) {
            const formattedDate = formatDate(query.date);
            if (storageData[formattedDate]) {
                delete storageData[formattedDate];
                console.log(`Удалены записи для даты ${formattedDate} в storage_${STORAGE_ID}`);
            }
            return;
        }

        if (query.date) {
            const formattedDate = formatDate(query.date);
            const data = storageData[formattedDate] || [];

            const response = data.length > 0
                ? { date: formattedDate, data }
                : { error: `Данные за ${formattedDate} не найдены` };

            await channel.sendToQueue(responseQueue, Buffer.from(JSON.stringify(response)));
        }

    }, { noAck: true });
}

startStorage();
