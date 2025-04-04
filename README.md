Код представляет распределённую систему обработки и хранения погодных данных с использованием RabbitMQ. Система состоит из трёх частей:


1. Клиент (client.js) – отправляет команды ```LOAD``` [файл] (загрузка данных) и ```GET``` [дата] (запрос данных).
2. Менеджер (manager.js) – принимает команды от клиента, обрабатывает данные и отправляет их в соответствующие хранилища. Отслеживает состояние узлов и выполняет автоматическое восстановление данных с реплик на новые узлы при отказе.
3. Хранилище (storage.js) – сохраняет данные и обрабатывает запросы на их получение.


RabbitMQ — программный брокер сообщений на основе стандарта AMQP

После установки и запуска RabbitMQ, нужно осуществить установку зависимостей через ```npm i```. Запуск системы осуществляется через команду ```npm start N```, где ```N``` - это количество хранителей. После запуска можно отправить данные о погоде через команду ```LOAD``` и запросить данные по дате через ```GET``` (в формате DD-MM-YYYY).

Доступные данные:
- ```LOAD data/seattle-weather.csv```
- ```LOAD data/testset.csv```
- ```LOAD data/weather_prediction_dataset.csv```
- ```LOAD data/weather.csv```

Команда поиска: 
- ```GET``` DD-MM-YYYY

Команда отключение узла-хранилища: 
- ```SHUTDOWN``` [номер_узла] (для тестирования отказоустойчивости).
