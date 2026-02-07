# Sistema de Processamento Assíncrono de Jobs

> Processamento em background com filas, workers, retry inteligente e Dead Letter Queue — o core é **fluxo e estado**, não dados.

---

## Por que isso NÃO é CRUD?

| CRUD típico | Este projeto |
|---|---|
| Dados no centro | Fluxo e estado no centro |
| Request → Response síncrono | Fire-and-forget assíncrono |
| Banco como protagonista | Fila como protagonista |

**Mensagem:** _"Sei lidar com sistemas que quebram."_

---

## Stack

| Camada | Tecnologia |
|---|---|
| Runtime | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/dotnetcore/dotnetcore-original.svg" width="20"/> .NET 9 |
| Message Broker | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/rabbitmq/rabbitmq-original.svg" width="20"/> RabbitMQ |
| Cache / Idempotência | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="20"/> Redis |
| Banco (status dos jobs) | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="20"/> PostgreSQL |
| Containerização | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" width="20"/> Docker / Docker Compose |
| Testes | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/dotnetcore/dotnetcore-original.svg" width="20"/> xUnit + Testcontainers |

---

## Arquitetura Geral

```mermaid
flowchart LR
    Client([🌐 Cliente]) -->|POST /jobs| API[API REST]

    subgraph Produtor["📤 Produção do Job"]
        API --> VALIDATE[Valida payload]
        VALIDATE --> PUBLISH[Publica na fila]
    end

    PUBLISH --> QUEUE

    subgraph Broker["🐇 RabbitMQ"]
        QUEUE[📬 Fila Principal]
        DLQ[💀 Dead Letter Queue]
    end

    subgraph Workers["⚙️ Workers"]
        direction TB
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker N...]
    end

    QUEUE --> W1
    QUEUE --> W2
    QUEUE --> W3

    W1 -->|Falha após retries| DLQ
    W2 -->|Sucesso| DB[(PostgreSQL\nStatus do Job)]
    W3 -->|Consulta idempotência| REDIS[(Redis)]
```

---

## Fluxo Completo de um Job

```mermaid
sequenceDiagram
    actor Client as Cliente
    participant API as API REST
    participant RMQ as RabbitMQ
    participant Worker as Worker
    participant Redis as Redis
    participant DB as PostgreSQL

    Client->>API: POST /jobs (payload + Idempotency-Key)
    API->>Redis: Verifica idempotência
    alt Já processado
        Redis-->>API: ✅ Job duplicado
        API-->>Client: 200 OK (resultado anterior)
    else Novo job
        API->>DB: Cria job (status: PENDING)
        API->>RMQ: Publica mensagem
        API-->>Client: 202 Accepted (jobId)
    end

    RMQ->>Worker: Consome mensagem
    Worker->>Redis: Verifica idempotência do consumo
    Worker->>DB: Atualiza status → RUNNING
    Worker->>Worker: Processa job

    alt Sucesso
        Worker->>DB: Atualiza status → DONE
        Worker->>Redis: Marca como processado
        Worker->>RMQ: ACK
    else Falha
        Worker->>DB: Atualiza status → RETRYING
        Worker->>RMQ: NACK (com requeue)
    end

    Client->>API: GET /jobs/{id}
    API->>DB: Consulta status
    DB-->>API: { status, resultado }
    API-->>Client: 200 OK
```

---

## Retry com Exponential Backoff

Quando um job falha, não reprocessamos imediatamente — isso sobrecarregaria o sistema. Usamos **exponential backoff** com jitter.

```mermaid
flowchart TD
    FAIL([❌ Job Falhou]) --> COUNT{Tentativa\n< máximo?}
    COUNT -->|Sim| CALC[Calcula delay\n2^tentativa + jitter]
    CALC --> WAIT[⏳ Aguarda delay]
    WAIT --> RETRY[🔄 Reprocessa]
    RETRY --> RESULT{Sucesso?}
    RESULT -->|Sim| DONE[✅ DONE]
    RESULT -->|Não| FAIL

    COUNT -->|Não — esgotou\ntentativas| DLQ[💀 Dead Letter Queue]
    DLQ --> ALERT[📢 Alerta para\nmonitoramento]
```

| Tentativa | Delay base | Com jitter (exemplo) |
|---|---|---|
| 1 | 2s | 2.3s |
| 2 | 4s | 4.7s |
| 3 | 8s | 7.1s |
| 4 | 16s | 18.2s |
| 5 (máx) | — | → DLQ |

---

## Dead Letter Queue (DLQ)

Jobs que **esgotaram todas as tentativas** caem na DLQ. Eles não desaparecem — ficam disponíveis para:

- Inspeção manual
- Reprocessamento seletivo
- Geração de alertas

```mermaid
flowchart LR
    DLQ[(💀 DLQ)] --> INSPECT[🔍 Inspeção\nManual]
    DLQ --> REPLAY[🔄 Replay\nSeletivo]
    DLQ --> ALERT[📢 Alerta\nMonitoramento]
    REPLAY --> QUEUE[(📬 Fila Principal)]
```

---

## Idempotência no Consumo

A rede não é confiável. Uma mensagem pode ser entregue **mais de uma vez**. Para garantir que o mesmo job não seja executado duas vezes:

```mermaid
flowchart TD
    MSG([📨 Mensagem\nrecebida]) --> CHECK{Redis:\nIdempotency-Key\nexiste?}
    CHECK -->|Sim| SKIP[⏭️ Ignora\n— já processado]
    CHECK -->|Não| LOCK[🔒 Seta key\ncom TTL]
    LOCK --> PROCESS[⚙️ Processa job]
    PROCESS --> RESULT{Sucesso?}
    RESULT -->|Sim| KEEP[Mantém key no Redis]
    RESULT -->|Não| REMOVE[Remove key\n— permite retry]
```

---

## Ciclo de Vida de um Job

```mermaid
stateDiagram-v2
    [*] --> PENDING: Job criado
    PENDING --> RUNNING: Worker consome
    RUNNING --> DONE: Processado ✅
    RUNNING --> RETRYING: Falha temporária
    RETRYING --> RUNNING: Nova tentativa
    RETRYING --> FAILED: Esgotou retries
    FAILED --> [*]: Vai para DLQ 💀
    DONE --> [*]
```

---

## Casos de Uso Implementados

| Caso | Descrição |
|---|---|
| 📄 **Upload de CSV** | Usuário envia arquivo → sistema processa linhas em background |
| 📊 **Geração de Relatório** | Requisição dispara job pesado → resultado disponível via polling |
| 📧 **Envio Massivo de Notificações** | Enfileira milhares de e-mails sem bloquear a API |

---

## Estrutura do Projeto

```
Processamento_async/
├── src/
│   ├── Jobs.API/                      # API REST — recebe e consulta jobs
│   │   ├── Program.cs
│   │   ├── Controllers/
│   │   │   └── JobsController.cs
│   │   └── Filters/
│   │       └── IdempotencyFilter.cs
│   ├── Jobs.Core/                     # Domínio e interfaces
│   │   ├── Models/
│   │   │   ├── Job.cs
│   │   │   └── JobStatus.cs
│   │   ├── Interfaces/
│   │   │   ├── IJobProcessor.cs
│   │   │   └── IMessageBroker.cs
│   │   └── Strategies/
│   │       └── RetryStrategy.cs
│   ├── Jobs.Worker/                   # Consumer — processa jobs da fila
│   │   ├── Program.cs
│   │   └── Consumers/
│   │       ├── CsvProcessorConsumer.cs
│   │       ├── ReportGeneratorConsumer.cs
│   │       └── NotificationConsumer.cs
│   └── Jobs.Infrastructure/           # RabbitMQ, Redis, PostgreSQL
│       ├── Messaging/
│       │   └── RabbitMqBroker.cs
│       ├── Cache/
│       │   └── RedisIdempotencyStore.cs
│       └── Persistence/
│           └── JobRepository.cs
├── tests/
│   ├── Jobs.UnitTests/
│   └── Jobs.IntegrationTests/
├── docker-compose.yml
└── README.md
```

---

## Como Rodar

```bash
# Subir infraestrutura (RabbitMQ, Redis, PostgreSQL)
docker-compose up -d

# Rodar a API
dotnet run --project src/Jobs.API

# Rodar o Worker (em outro terminal)
dotnet run --project src/Jobs.Worker

# Executar testes
dotnet test
```

---

## Configuração

```json
{
  "RabbitMq": {
    "Host": "localhost",
    "Port": 5672,
    "Username": "guest",
    "Password": "guest",
    "Queues": {
      "Main": "jobs-queue",
      "DeadLetter": "jobs-dlq"
    }
  },
  "Retry": {
    "MaxAttempts": 5,
    "BaseDelaySeconds": 2,
    "UseJitter": true
  },
  "Redis": {
    "ConnectionString": "localhost:6379",
    "IdempotencyTtlMinutes": 60
  }
}
```

---

## Observabilidade

| O quê | Como |
|---|---|
| Logs estruturados | Serilog com correlation ID por job |
| Métricas | Jobs processados, falhos, tempo médio de processamento |
| Alertas | Jobs na DLQ disparam notificação |
| Dashboard | RabbitMQ Management UI (`localhost:15672`) |
