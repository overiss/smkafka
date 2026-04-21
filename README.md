# smkafka

`smkafka` — это Go-библиотека для работы с Kafka через `confluent-kafka-go`, где API максимально упрощен для прикладного кода.

Идея проекта:

- создавать `Producer` и `Consumer` как независимые сущности;
- на этапе инициализации задавать понятный конфиг (`hosts`, `security protocol`, `username/password`, сертификаты);
- в рабочем коде оперировать простыми методами (`context` + `[]byte`), без Kafka-типов.

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

## How-To: Producer

### 1) Создайте producer

```go
producer, err := smkafka.NewProducer(smkafka.ProducerConfig{
	Topic: "orders.events",
	Common: smkafka.CommonConfig{
		Hosts:            []string{"localhost:9092"},
		SecurityProtocol: smkafka.SecurityProtocolPlaintext,
	},
	ClientID: "orders-producer",
})
if err != nil {
	log.Fatal(err)
}
defer producer.Close()
```

### 2) Отправьте одно сообщение

```go
ctx := context.Background()
err = producer.Produce(ctx, []byte(`{"order_id":123}`))
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

err = producer.ProduceMany(ctx, messages)
if err != nil {
	log.Fatal(err)
}
```

## How-To: Consumer

### 1) Создайте consumer

```go
consumer, err := smkafka.NewConsumer(smkafka.ConsumerConfig{
	Topic: "orders.events",
	GroupID:         "orders-worker",
	AutoOffsetReset: "earliest",
	Common: smkafka.CommonConfig{
		Hosts:            []string{"localhost:9092"},
		SecurityProtocol: smkafka.SecurityProtocolPlaintext,
	},
	BatchSize:     100,
	BatchDeadline: 5 * time.Second,
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

// message имеет тип []byte
_ = message
```

### 3) Прочитайте батч (`ConsumeBatch`)

Батч читается либо до заполнения, либо до истечения `BatchDeadline` (из `ConsumerConfig`).

```go
batch, err := consumer.ConsumeBatch(ctx)
if err != nil {
	log.Fatal(err)
}

for _, message := range batch.Messages {
	_ = message // []byte
}
```

### 4) Закоммитьте батч

```go
if err := consumer.CommitBatch(); err != nil {
	log.Fatal(err)
}
```

## Ключевые типы API

- `CommonConfig` — общий конфиг подключения (`Hosts`, `SecurityProtocol`, `SASLMechanism`, `Username`, `Password`, сертификаты)
- `ProducerConfig` — конфиг продюсера (`Topic`, `Common`, `ClientID`, `Partition`, `Overrides`)
- `ConsumerConfig` — конфиг консьюмера (`Topic`, `GroupID`, `AutoOffsetReset`, `BatchSize`, `BatchDeadline`, `ReconnectTimeout`, `Common`, `Overrides`)
- `Producer` — `Produce`, `ProduceMany`, `Flush`, `Close`
- `Consumer` — `Consume`, `ConsumeBatch`, `CommitBatch`, `Commit`, `Close`
- Константы для безопасной настройки: `SecurityProtocol*`, `SASLMechanism*`

## SSL/SASL и сертификаты

Если выбран `SecurityProtocol: "SASL_SSL"`, библиотека проверяет сертификаты:

- `CaLocation` и `CertLocation` обязательны;
- если не заданы, конструктор вернет ошибку;
- `KeyLocation` опционален и будет добавлен в Kafka-конфиг, если указан.

Пример:

```go
common := smkafka.CommonConfig{
	Hosts:            []string{"kafka-1:9093", "kafka-2:9093"},
	SecurityProtocol: smkafka.SecurityProtocolSASLSSL,
	SASLMechanism:    smkafka.SASLMechanismPlain,
	Username:         "my-user",
	Password:         "my-pass",
	CaLocation:       "/etc/certs/ca.pem",
	CertLocation:     "/etc/certs/client.pem",
	KeyLocation:      "/etc/certs/client.key",
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
