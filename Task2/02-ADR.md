# ADR-02: Проектирование Event-Driven архитектуры.

**Статус:** ⚠️ На рассмотрении
**Участники:** Иван Бабиков  
**Дата:** 2026-10-31  

---

## 1. Контекст

Чтобы обрабатывать более 500 тысяч конкурентных поездок и реализовать динамическое ценообразование, требуется распределённая архитектура с возможностью обрабатывать события в реальном времени. Нужно спроектировать событийную платформу, в которой: 

- Используется модель доменных событий, таких как BookingCreated, DriverLocationUpdated и другие.
- Определена схема топиков Kafka с партиционированием по регионам.
- Для реальной обработки событий задействованы потребители (например, KStreams или Flink).
- Выбран вид Saga и проработаны соответствующие события.
- Реализованы механизмы обеспечения надёжной доставки.
- Выбраны подходы к мониторингу и составлен список метрик
- Инструменты для мониторинга определены и интегрированы в архитектуру To-Be.

---

## 2. Требования

См. `Task1\01-ADR.md`

---

## 3. Решение

### 3.1 Модели доменных событий ###

| Код события | Название | Описание | Данные |
|---|---|---|---|
| **** |  |  |  |


### 3.2 Схема топиков в Kafka ###

| Код топика | Описание данных| Producer | Consumer |
|---|---|---|---|
| **booking-saga.get_driver** | Запрос водителя на заказ | booking_service | driver_service |
| **booking-saga.get_drivers_resp** | Данные водителя водителя для заказа/нет доступных | driver_service  | booking_service|
| **booking-saga.get_price** | Запрос цены | booking_service | pricing_service |
| **booking-saga.get_price_resp** | Цена заказа/ошибка расчёты цены | pricing_service | booking_service |
| **booking-saga.start_payment** | Запрос оплаты поездки | booking_service | payments_service |
| **booking-saga.start_payment_resp** | Оплата поездки/Ошибка оплаты | payments_service | booking_service |
| **booking-saga.request_route** | Запрос маршрута | booking_service | geo_service |
| **booking-saga.order_completed** | Исполнение заказа | driver_service | payouts_service |
| **booking-saga.check_fraud** | Автоматически проверяет на мошенничество крупные заказы | booking_service | fraud_service |
| **booking-service.notifications** | Уведомления | booking_service | notification_service |
| **booking-service.notifications** | Уведомления | driver_service | notification_service |
| **booking-service.notifications** | Уведомления | payments_service | notification_service |
| **driver-payment-saga.get_trips** | Получить поездки | driver_service | booking_service |
| **driver-payment-saga.pricing_service** | Получить цены поездок | driver_service | pricing_service |
| **analitics** | Метрики геопоиска | analitics_service | analitics_service |

### 3.3 Выбор типа SAGA и реестр событий SAGA ###

Для реализации двух обнаруженных бизнес-процессов: "заказ такси" и "выплаты водителю" предлагается использовать **Parallel Saga**. Parallel Saga позволяет управлять сложными бизнес-процессами, отностильно легко изменять количество участвующих в них микросервисов и даёт высокую производительность.

### 3.4 Выбор механизмов обеспечения надёжной доставки данных между сервисами ###

Для доставки данных между сервисами выбран брокер сообщений **Kafka** в котором реализованны современные механизмы отказоустойчивости и повышения производительности.

### 3.5.1 Выбор подходов к мониторингу ###

При выборе подхода к мониторингу руководствуемся следующими принципами:

1. Сохраняем метрики фактов бизнес-событий и их количественные показатели.
2. Сохраняем стандартные технические метрики задействованных в системе серверов и платформ. Для каждого такого объекта мониторинга сохраняем параметры:
- Процент загрузки
- Задержка (видимое)
- Трафик (RPS)
- Ошибки (total errors)
- Метрики насыщения (длина очереди и т.п)
3. Сохраняем специфические ключевые параметры каждой платформы (cache hits rate, кол-во обращений к диску и т.п.)

### 3.5.2 Бизнес-метрики для мониторинга ###

**Сервис заказов (booking service)**

| Код метрики | Описание |
|---|---|
| **booking_service.created** | Заказы поездок (бронирования) |
| **booking_service.started** | Начатые поездки |
| **booking_service.finished** | Законченные поездки |
| **booking_service.canceled** | Отменённые поездки |
| **booking_service.waiting_time** | Время от заказа до начала поездки |
| **booking_service.driver_waiting_time** | Время ожидание пассажира водителем |
| **booking_service.duration** | Продолжительность поездок |

**Сервис водителей (driver service)**
| Код метрики | Описание |
|---|---|
| **driver_service.available_drivers** | Кол-во водителей, готовых принять заказ |
| **driver_service.drivers_in_trips** | Кол-во водителей в поездке |

**Сервис ценообразования (pricing service)**
| Код метрики | Описание |
|---|---|
| **pricing_service.prices_calc_count** | Кол-во расчётов цен поездки |
| **pricing_service.prices_avg** | Средняя расчитанная цена поездки |

**Сервис приёма платежей (payments service)**
| Код метрики | Описание |
|---|---|
| **payments_service.started_payments** | Начатые платежи |
| **payments_service.finished_payments** | Успешные платежи |
| **payments_service.avg_payment_time** | Среднее время проведения платежей |
| **payments_service.refunded_payments** | Возвраты |
| **payments_service.avg_payment** | Средний платёж |
| **payments_service.payments_total_sum** | Сумма платежей |

**Выплаты водителям (payouts service)**
| Код метрики | Описание |
|---|---|
| **payouts_service.started_payouts** | Начатые выплаты |
| **payouts_service.finished_payouts** | Произведённые выплаты |
| **payouts_service.payouts_total_sum** | Сумма всех выплат |
| **payouts_service.avg_payout** | Средняя выплата |

**Сервис уведомлений (notification service)**
| Код метрики | Описание |
|---|---|
| **notification_service.sent_notifications** | Отправленные |
| **notification_service.delivered_notifications** | Доставленные |
| **notification_service.failed_notifications** | Ошибки |
| **notification_service.retries_count** | Среднее кол-во повторных попыток отправки |
| **notification_service.delivery_duration** | Среднее время доставки |

**Антифрод сервис ((anti-)fraud service)**
| Код метрики | Описание |
|---|---|
| **fraud_service.autochecks_count** | Кол-во автоматических проверок |
| **fraud_service.triggered_cases** | Кол-во активированных ручных проверок |
| **fraud_service.real_cases** | Кол-во обнаруженных нарушений |

### 3.5.3 Метрики запросов к внешним ресурсам ###

Для вызовов во все внешние сервисы сохраняем метрики:

| Код метрики | Описание | Источник | Пример |
|---|---|---|---|
| **RPS** | Кол-во запросов в сек. | Каждый микросервис, APIGateway и Auth serever | *yandex_pay.rps* |
| **Errors** | Кол-во ошибок (с кодом ошибки) | Каждый микросервис, APIGateway и Auth serever | *yandex_pay.errors* |
| **Duration** | Среднее время обработки запроса | Каждый микросервис, APIGateway и Auth serever | *yandex_pay.duration* |

Для финансовых серивисов "API Банка" и "Яндекс Пэй" сохраняем среднюю оплату и сумму платежей
| Код метрики | Описание |
|---|---|
| **yandex_pay.payouts_total_sum** | Сумма всех выплат |
| **yandex_pay.avg_payout** | Средняя выплата |

### 3.5.4 Технические метрики для мониторинга ###

**Стандартные метрики для всех микросервисов**

Для всех микросервисов, а также APIGateway и Auth serever снимаем метрики:

| Код метрики | Описание | Источник | Пример |
|---|---|---|---|
| **RPS** | Кол-во запросов в сек. | Каждый микросервис, APIGateway и Auth serever | *booking_service.rps* |
| **Errors** | Кол-во ошибок (с кодом ошибки) | Каждый микросервис, APIGateway и Auth serever | *booking_service.errors* |
| **Duration** | Среднее время обработки запроса | Каждый микросервис, APIGateway и Auth serever | *booking_service.duration* |

**Метрики серверов баз данных сервисов**

Для всех баз данных снимаем метрики:

| Код метрики | Описание | Пример |
|---|---|---|
| **RPS** | Кол-во запросов в сек. | *booking_service_db.rps* |
| **Errors** | Кол-во ошибок | *booking_service_db.errors* |
| **Duration** | Среднее время обработки запроса | *booking_service_db.duration* |

**Метрики Kafka**

| Код метрики | Описание |
|---|---|
| **incomming_messages** | Кол-во входящих сообщений в сек. |
| **incomming_messages_size** | Общий объём байт входящих сообщений в сек. |
| **read_messages** | Кол-во прочитанных сообщений в сек. |
| **read_messages_size** | Общий объём байт прочитанных сообщений в сек. |
| **message_consuming_lag** | Разница во времени между записью сообщения и первым его прочтением |
| **dlq_size** | Размер очереди отсеянных ошибочных сообщений |

**Метрики прочих серверов**

| Код метрики | Описание | Пример |
|---|---|---|
| **RPS** | Кол-во запросов в сек. | *geo_service.redis.rps* |
| **Errors** | Кол-во ошибок | *geo_service.elstics.errors* |
| **Duration** | Среднее время обработки запроса | *geo_service.elstics.duration* |

**Метрики подов Kubernetes и физических серверов**
| Ресурс | Загрузка | Ожидание | Ошибки |
|---|---|---|---|
| **CPU** | cpu_usage_percent | - | cpu_errros |
| **Memory** | mem_usage_bytes | - | mem_errros |
| **Disk** | disk_reads/writes_bytes_per_sec, disk_usage_percent | disk_queue_len | disk_read_errros |
| **Network** | net_bytes_sent/recieved | net_delay | net_errros |


### 3.5.5 Список инструментов для мониторинга ###

**Для мониторинга процессов были выбраны:**

1. **Prometheus** - осуществляет сбор метрик.
2. **Grafana** - позволяет организовать визуализацию метрик в формате удобном сотрудникам отделов мониторинга.
3. **Alertmanager** - позволяет настраивать логику управления алертами.
