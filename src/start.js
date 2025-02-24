const { spawn } = require('child_process');

const processes = [];

function startProcess(name, command, args = []) {
    const proc = spawn(command, args, { stdio: 'inherit' });
    console.log(`${name} запущен (PID: ${proc.pid})`);

    processes.push(proc);

    proc.on('exit', (code) => {
        console.log(`${name} завершился с кодом ${code}`);
    });
}

startProcess('Менеджер', 'node', ['src/manager.js']);

for (let i = 0; i < 3; i++) {
    startProcess(`Хранитель ${i}`, 'node', ['src/storage.js', i]);
}

setTimeout(() => {
    startProcess('Клиент', 'node', ['src/client.js']);
}, 2000); 

process.on('SIGINT', () => {
    console.log("\n Завершаем все процессы...");
    processes.forEach(proc => proc.kill());
    process.exit();
});
