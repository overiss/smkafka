# smkafka

`smkafka` — это Go-библиотека для работы с Kafka через `confluent-kafka-go`, где API максимально упрощен для прикладного кода.

Идея проекта:

- создавать `Producer` и `Consumer` как независимые сущности;
- на этапе инициализации задавать понятный конфиг (`hosts`, `security protocol`, `username/password`, сертификаты);
- в рабочем коде оперировать простыми методами (`context` + `Message` / `key []byte` + `[]byte`), без Kafka-типов.

## Возможности

- Отдельная инициализация `Producer` и `Consumer`
- Отправка одного сообщения: `Produce`
- Отправка нескольких сообщений: `ProduceMany`
- Чтение одного сообщения: `Consume`
- Чтение батча: `ConsumeBatch`
- Коммит батча: `CommitBatch`
- Игнорирование ошибки `AssignmentLost` при чтении и коммитах

## Установка

```bash
go get github.com/overiss/smkafka
```

## Структура пакетов

```
github.com/overiss/smkafka/
├── config/    — общий конфиг подключения (Hosts, SSL/SASL, сертификаты)
├── producer/  — producer API
├── consumer/  — consumer API
└── smkafka    — re-export для обратной совместимости (корневой пакет)
```

Рекомендуемый импорт — из подпакетов:

```go
import (
    "github.com/overiss/smkafka/config"
    "github.com/overiss/smkafka/producer"
    "github.com/overiss/smkafka/consumer"
)
```

Старый импорт `github.com/overiss/smkafka` по-прежнему работает через type aliases.

## How-To: Producer

### 1) Создайте producer

```go
producer, err := producer.New(producer.Config{
	Name:  "orders-producer",
	Topic: "orders.events",
	Common: config.Common{
		Hosts:            []string{"localhost:9092"},
		SecurityProtocol: config.SecurityProtocolPlaintext,
	},
	ReadinessTimeout: 2 * time.Second,
	ClientID:         "orders-producer",
})
if err != nil {
	log.Fatal(err)
}
defer producer.Close()
```

### 2) Отправьте одно сообщение

```go
ctx := context.Background()
key := []byte("order-123")
err = producer.Produce(ctx, key, []byte(`{"order_id":123}`))
if err != nil {
	log.Fatal(err)
}
```

### 3) Отправьте несколько сообщений

```go
messages := [][]byte{
	[]byte(`{"order_id":124}`),
	[]byte(`{"order_id":125}`),
	[]byte(`{"order_id":126}`),
}

err = producer.ProduceMany(ctx, key, messages)
if err != nil {
	log.Fatal(err)
}
```

## How-To: Consumer

### 1) Создайте consumer

```go
consumer, err := consumer.New(consumer.Config{
	Name:  "orders-consumer",
	Topic: "orders.events",
	GroupID:         "orders-worker",
	AutoOffsetReset: "earliest",
	Common: config.Common{
		Hosts:            []string{"localhost:9092"},
		SecurityProtocol: config.SecurityProtocolPlaintext,
	},
	BatchSize:     100,
	BatchDeadline: 5 * time.Second,
	ReadinessTimeout: 2 * time.Second,
	ReconnectTimeout: 5 * time.Second,
	ClientID:      "orders-consumer",
})
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()
```

### 2) Прочитайте одно сообщение (`Consume`)

```go
message, err := consumer.Consume(ctx)
if err != nil {
	log.Fatal(err)
}

// message: Key, Value, Headers
_ = message.Key
_ = message.Value
_ = message.Headers
```

### Probe-методы

```go
name := consumer.Name()   // "orders-consumer"
ready := consumer.IsReady() // true/false

_ = name
_ = ready
```

### 3) Прочитайте батч (`ConsumeBatch`)

Батч читается либо до заполнения, либо до истечения `BatchDeadline` (из `ConsumerConfig`).

```go
messages, err := consumer.ConsumeBatch(ctx)
if err != nil {
	log.Fatal(err)
}

for _, message := range messages {
	_ = message.Key
	_ = message.Value
	_ = message.Headers
}
```

### 4) Закоммитьте батч

```go
if err := consumer.CommitBatch(); err != nil {
	log.Fatal(err)
}
```

## Ключевые типы API

- `config.Common` — общий конфиг подключения (`Hosts`, `SecurityProtocol`, `SASLMechanism`, `Username`, `Password`, сертификаты)
- `producer.Config` — конфиг продюсера (`Name`, `Topic`, `Common`, `ReadinessTimeout`, `ClientID`, `Partition`, `Hooks`, `Overrides`)
- `consumer.Config` — конфиг консьюмера (`Name`, `Topic`, `GroupID`, `AutoOffsetReset`, `BatchSize`, `BatchDeadline`, `ReconnectTimeout`, `ReadinessTimeout`, `Common`, `Hooks`, `Overrides`)
- `producer.Producer` — `Produce`, `ProduceMany`, `Flush`, `Close`
- `consumer.Message` — прочитанное сообщение (`Key`, `Value`, `Headers`)
- `consumer.Header` — один заголовок сообщения (`Key`, `Value`)
- `consumer.Consumer` — `Consume`, `ConsumeBatch`, `CommitBatch`, `Commit`, `Close`
- readiness API — `Name() string`, `IsReady() bool` у `Producer` и `Consumer`
- Константы для безопасной настройки: `SecurityProtocol*`, `SASLMechanism*`

## Hooks для метрик

В `producer.Config.Hooks` и `consumer.Config.Hooks` можно задать колбэки, которые вызываются после завершения операции с её длительностью и ошибкой:

- `OnProduce`, `OnProduceMany`
- `OnCommit`, `OnConsume`, `OnConsumeBatch`

Пример:

```go
producer, err := producer.New(producer.Config{
	Topic: "orders.events",
	Common: config.Common{
		Hosts: []string{"localhost:9092"},
	},
	Hooks: producer.Hooks{
		OnProduce: func(duration time.Duration, err error) {
			// ordersProducerLatency.Observe(duration.Seconds())
			// ordersProducerErrors.Inc() if err != nil
		},
	},
})
```

```go
consumer, err := consumer.New(consumer.Config{
	Topic:   "orders.events",
	GroupID: "orders-worker",
	Common: config.Common{
		Hosts: []string{"localhost:9092"},
	},
	Hooks: consumer.Hooks{
		OnConsumeBatch: func(duration time.Duration, err error) {
			// consumerBatchLatency.Observe(duration.Seconds())
		},
	},
})
```

Хук вызывается один раз на вызов метода, включая retry внутри `Commit` после `AssignmentLost`.

## SSL/SASL и сертификаты

Поля `config.Common`:

- `CaLocation` — trust store (CA bundle) для проверки сертификата брокера, не client cert;
- `CertLocation` — опциональный client certificate (`ssl.certificate.location`);
- `KeyLocation` — опциональный приватный ключ client certificate (`ssl.key.location`).

Для `SecurityProtocol: "SSL"` и `"SASL_SSL"` обязателен только `CaLocation`.
Если заданы `CertLocation` и/или `KeyLocation`, они прокидываются в Kafka-конфиг.

Пример `SASL_SSL` только с trust store:

```go
common := config.Common{
	Hosts:            []string{"kafka-1:9093", "kafka-2:9093"},
	SecurityProtocol: config.SecurityProtocolSASLSSL,
	SASLMechanism:    config.SASLMechanismPlain,
	Username:         "my-user",
	Password:         "my-pass",
	CaLocation:       "/etc/certs/ca.pem",
}
```

Пример `SASL_SSL` с mutual TLS (client cert):

```go
common := config.Common{
	Hosts:            []string{"kafka-1:9093", "kafka-2:9093"},
	SecurityProtocol: config.SecurityProtocolSASLSSL,
	SASLMechanism:    config.SASLMechanismPlain,
	Username:         "my-user",
	Password:         "my-pass",
	CaLocation:       "/etc/certs/ca.pem",
	CertLocation:     "/etc/certs/client.pem",
	KeyLocation:      "/etc/certs/client.key",
}
```

Пример `SSL` без SASL:

```go
common := config.Common{
	Hosts:            []string{"kafka-1:9093", "kafka-2:9093"},
	SecurityProtocol: config.SecurityProtocolSSL,
	CaLocation:       "/etc/certs/ca.pem",
}
```

## Поведение при AssignmentLost

При `AssignmentLost` во время коммита `smkafka` выполняет внутренний `reconnect()`:

- читает текущие assignment;
- запрашивает актуальные offsets через `OffsetsForTimes`;
- резюмирует assignment через `Resume`.
- после reconnect делает повторный коммит того же набора offsets (без рекурсии).

Во время чтения (`Consume`, `ConsumeBatch`) ошибка `AssignmentLost` по-прежнему пропускается.

## Почему библиотека оптимизирована

`smkafka` экономит ресурсы за счет нескольких решений на уровне реализации:

- `ProduceMany` использует один delivery-канал на весь батч вместо канала на каждое сообщение;
- batch commit переиспользует сохраненные метаданные и избегает лишних копий слайсов;
- при `AssignmentLost` commit делает controlled retry (reconnect + повторный commit), без рекурсии;
- конфиг компилируется один раз при создании клиента, а не на каждом рабочем вызове.
