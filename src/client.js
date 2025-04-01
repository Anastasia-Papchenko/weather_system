const amqp = require('amqplib');
const readline = require('readline');
const path = require('path');

async function startClient() {
    const connection = await amqp.connect('amqp://localhost:5672');
    const channel = await connection.createChannel();

    await channel.assertQueue('client', { durable: false });
    console.log("Клиент запущен. Введите команду: LOAD [путь_к_файлу], GET [дата] или SHUTDOWN [номер хранителя]");

    function determineFileFormat(filePath) {
        console.log("Проверяем файл:", filePath); 
        const filename = path.basename(filePath);

        if (filename.includes("seattle-weather")) {
            return "seattle-weather";
        } else if (filename.includes("testset")) {
            return "testset";
        } else if (filename.includes("weather_prediction_dataset")) {
            return "weather_prediction_dataset";
        } else if (filename.includes("weather")) {
            return "weather";
        } else {
            return "unknown";
        }
    }

    // function parseTestsetDate(datetime_utc) {
    //     if (!datetime_utc || !datetime_utc.includes("-")) {
    //         return null;
    //     }
    
    //     const [datePart, utc] = datetime_utc.split("-");
    //     if (!/^\d{8}$/.test(datePart)) {
    //         return null;
    //     }
    
    //     const year = datePart.substring(0, 4);
    //     const month = datePart.substring(4, 6);
    //     const day = datePart.substring(6, 8);
    
    //     return {
    //         date: `${day}-${month}-${year}`, 
    //         utc
    //     };
    // }

    channel.consume('client', (msg) => {
        const data = JSON.parse(msg.content.toString());
        if (data.error) {
            console.log(`Ошибка: ${data.error}`);
        } else {
            console.log(`Данные:`, data.data);
        }
    }, { noAck: true });




    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    rl.on('line', async (input) => {
        const args = input.trim().split(" ");

        if (args.length < 2) {
            console.log("Ошибка: Неверный формат команды. Используйте LOAD [файл], GET [дата] или SHUTDOWN [номер хранителя]");
            return;
        }

        if (args[0] === "LOAD") {
            const filePath = args[1];
            const fileFormat = determineFileFormat(filePath);

            if (fileFormat === "unknown") {
                console.log(`Ошибка: неизвестный формат файла: ${filePath}`);
                return;
            }

            console.log(`Отправка файла в менеджер: ${filePath} (Формат: ${fileFormat})`);
            await channel.sendToQueue('manager', Buffer.from(JSON.stringify({
                command: "LOAD",
                file: filePath,
                format: fileFormat
            })));

            // console.log(`Файл ${filePath} отправлен на загрузку`);
        } else if (args[0] === "GET") {
            await channel.sendToQueue('manager', Buffer.from(JSON.stringify({ 
                command: "GET", 
                date: args[1] 
            })));
            console.log(`Запрос отправлен: ${args[1]}`);
        } else if (args[0] === "SHUTDOWN") {
            const storageId = parseInt(args[1], 10);
            if (isNaN(storageId)) {
                console.log("Ошибка: Укажите номер хранителя (например, SHUTDOWN 0)");
                return;
            }
            console.log(`Отправка команды SHUTDOWN для хранителя ${storageId}`);
            await channel.sendToQueue("manager", Buffer.from(JSON.stringify({ command: "SHUTDOWN", storageId })));
        } else {
            console.log("Ошибка: Неизвестная команда. Используйте LOAD [файл], GET [дата] или SHUTDOWN [номер хранителя]");
        }
    });
}

startClient();


