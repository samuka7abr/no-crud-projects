# Sistema de Leilão Distribuído

> Microsserviços, cache, idempotência, concorrência pesada e comunicação em tempo real — tudo o que um sistema de produção real exige quando **milissegundos importam**.

---

## Por que isso NÃO é CRUD?

Este sistema **parece** ter entidades (Leilão, Lance, Usuário), mas o diferencial está nos **problemas de infraestrutura distribuída** que ele resolve:

| CRUD típico | Este projeto |
|---|---|
| `POST /bids` e pronto | Controle de concorrência otimista com RowVersion |
| Sem preocupação com duplicatas | Idempotência distribuída por chave única |
| Tudo síncrono | Processamento via filas + workers |
| Polling para atualizar | Real-time via WebSockets |

**Mensagem:** _"Eu construo sistemas que escalam sob pressão real."_

---

## Stack

| Camada | Tecnologia |
|---|---|
| Runtime | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/dotnetcore/dotnetcore-original.svg" width="20"/> .NET 9 (Microsserviços) |
| Banco relacional | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" width="20"/> PostgreSQL |
| Cache distribuído | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="20"/> Redis |
| Message Broker | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/rabbitmq/rabbitmq-original.svg" width="20"/> RabbitMQ |
| Real-time | SignalR (WebSockets) |
| API Gateway | YARP (Reverse Proxy) |
| Containerização | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" width="20"/> Docker / Docker Compose |
| Testes | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/dotnetcore/dotnetcore-original.svg" width="20"/> xUnit + Testcontainers |

---

## Arquitetura de Microsserviços

```mermaid
flowchart TB
    Client([🌐 Cliente]) --> GW[🚪 API Gateway\nYARP]

    GW --> AUTH[🔐 Auth Service]
    GW --> AUCTION[🏛️ Auction Service]
    GW --> BID[💰 Bid Service]
    GW --> NOTIFY[📢 Notification Service]

    AUCTION <--> DB_AUCTION[(PostgreSQL\nAuctions)]
    BID <--> DB_BID[(PostgreSQL\nBids)]
    BID <--> CACHE[(Redis\nCache + Idempotência)]

    BID -->|Evento: BidPlaced| RMQ[🐇 RabbitMQ]
    AUCTION -->|Evento: AuctionClosed| RMQ

    RMQ --> NOTIFY
    RMQ --> AUCTION

    NOTIFY -->|SignalR\nWebSocket| Client

    style GW fill:#4a9eff,color:#fff
    style RMQ fill:#ff6600,color:#fff
    style CACHE fill:#dc382d,color:#fff
```

---

## 1. Concorrência & Race Conditions 🏁

Num leilão, **milissegundos importam**. Para impedir que dois usuários registrem o mesmo lance simultaneamente:

### Controle de Concorrência Otimista com RowVersion

O PostgreSQL rejeita atualizações conflitantes no nível da transação, garantindo integridade **sem travar a tabela inteira**.

```mermaid
sequenceDiagram
    actor User_A as Usuário A
    actor User_B as Usuário B
    participant API as Bid Service
    participant DB as PostgreSQL

    Note over DB: Leilão: maior lance = R$100<br/>RowVersion = 1

    par Requisições simultâneas
        User_A->>API: POST /bids (R$150)
        User_B->>API: POST /bids (R$150)
    end

    API->>DB: UPDATE ... WHERE id = X AND row_version = 1
    Note over DB: ✅ User A: Sucesso!<br/>RowVersion → 2

    API->>DB: UPDATE ... WHERE id = X AND row_version = 1
    Note over DB: ❌ User B: 0 rows affected<br/>RowVersion não bate!

    DB-->>API: ✅ 1 row affected (User A)
    DB-->>API: ⚠️ 0 rows affected (User B)

    API-->>User_A: 201 Created — Lance aceito!
    API-->>User_B: 409 Conflict — Lance conflitante, tente novamente
```

**Como funciona:**
1. Toda entidade de Leilão possui uma coluna `row_version` (`xmin` do PostgreSQL ou coluna explícita)
2. Ao registrar um lance, o `UPDATE` inclui `WHERE row_version = @expected`
3. Se outro lance chegou antes, o `row_version` já mudou → **0 rows affected**
4. O serviço detecta o conflito e retorna **409 Conflict**
5. O cliente pode re-tentar com o valor atualizado

---

## 2. Idempotência 🔄

Falhas de rede fazem requests serem **retransmitidos**. Sem idempotência, o mesmo lance poderia ser registrado **duas vezes**.

### Filtro de Idempotência Distribuído

```mermaid
flowchart TD
    REQ([📨 POST /bids\nX-Idempotency-Key: abc-123]) --> CHECK{Redis:\nKey abc-123\nexiste?}

    CHECK -->|Sim| CACHED[📦 Retorna resposta\ncacheada]
    CACHED --> RES_OK([200 OK\nResultado anterior])

    CHECK -->|Não| LOCK[🔒 Seta key com TTL\nstatus: PROCESSING]
    LOCK --> PROCESS[⚙️ Processa lance]
    PROCESS --> RESULT{Sucesso?}

    RESULT -->|Sim| STORE[💾 Armazena resposta\nno Redis com TTL]
    STORE --> RES_CREATED([201 Created])

    RESULT -->|Não| CLEANUP[🗑️ Remove key\npermite retry]
    CLEANUP --> RES_ERROR([500 Error])
```

**Mecanismo:**
1. O cliente envia um header `X-Idempotency-Key` único por operação
2. O **IdempotencyFilter** verifica no Redis se essa key já foi processada
3. Se sim → retorna a **resposta cacheada** (sem reprocessar)
4. Se não → **processa** e armazena o resultado com TTL
5. Se falhar → **remove a key** para permitir retry legítimo

---

## 3. Consistência Eventual & Resiliência 📉

Em vez de processar tudo na thread da requisição (bloqueando o usuário), operações pesadas são **descarregadas para workers** via RabbitMQ.

### Fluxo de Fechamento de Leilão

```mermaid
sequenceDiagram
    participant Scheduler as ⏰ Scheduler
    participant Auction as 🏛️ Auction Service
    participant RMQ as 🐇 RabbitMQ
    participant Worker as ⚙️ Closure Worker
    participant Payment as 💳 Payment Service
    participant Notify as 📢 Notification Service
    participant DB as PostgreSQL

    Scheduler->>Auction: Leilão expirou
    Auction->>DB: Status → CLOSING
    Auction->>RMQ: Evento: AuctionClosing

    RMQ->>Worker: Consome evento
    Worker->>DB: Identifica lance vencedor
    Worker->>Payment: Inicia cobrança
    Payment-->>Worker: Cobrança confirmada

    Worker->>DB: Status → CLOSED
    Worker->>RMQ: Evento: AuctionClosed

    RMQ->>Notify: Consome AuctionClosed

    par Notificações paralelas
        Notify->>Notify: 📧 E-mail para vencedor
        Notify->>Notify: 📧 E-mail para perdedores
        Notify->>Notify: 📡 Push via SignalR
    end
```

**Benefícios:**
- A API responde **instantaneamente** (não espera envio de e-mail)
- Se o envio de notificação falhar, o **retry automático** da fila cuida
- O fechamento do leilão é **atômico** no banco, **eventual** nas notificações

### Fluxo de Retry com Dead Letter Queue

```mermaid
flowchart TD
    EVENT([📨 Evento na fila]) --> WORKER[⚙️ Worker processa]
    WORKER --> OK{Sucesso?}

    OK -->|Sim| ACK[✅ ACK — remove da fila]
    OK -->|Não| RETRY_COUNT{Tentativa\n< máximo?}

    RETRY_COUNT -->|Sim| BACKOFF[⏳ Exponential Backoff\n2^n + jitter]
    BACKOFF --> NACK[🔄 NACK — requeue]
    NACK --> WORKER

    RETRY_COUNT -->|Não| DLQ[💀 Dead Letter Queue]
    DLQ --> ALERT[📢 Alerta para time]
    DLQ --> REPLAY[🔄 Replay manual\nquando corrigido]
```

---

## 4. Atualizações em Tempo Real 📡

**SignalR** empurra atualizações de lances para todos os clientes conectados **instantaneamente**. Sem "atualizar página". A UI atualiza o preço e notifica se você foi superado — em tempo real.

```mermaid
sequenceDiagram
    actor User_A as Usuário A
    actor User_B as Usuário B
    participant API as Bid Service
    participant Hub as SignalR Hub
    participant RMQ as RabbitMQ

    User_A->>API: POST /bids (R$200)
    API->>API: Valida + Persiste lance
    API->>RMQ: Evento: BidPlaced

    RMQ->>Hub: Consome BidPlaced

    par Push para todos os conectados
        Hub->>User_A: ✅ "Seu lance de R$200 foi aceito!"
        Hub->>User_B: ⚠️ "Você foi superado! Novo lance: R$200"
    end

    Note over Hub: Todos recebem em <100ms<br/>via WebSocket persistente
```

### Arquitetura do Hub SignalR

```mermaid
flowchart LR
    subgraph Clients["🌐 Clientes Conectados"]
        C1[Usuário A]
        C2[Usuário B]
        C3[Usuário C]
    end

    subgraph SignalR["📡 SignalR"]
        HUB[Hub Central]
        G1[Grupo: Leilão #1]
        G2[Grupo: Leilão #2]
    end

    C1 <-->|WebSocket| HUB
    C2 <-->|WebSocket| HUB
    C3 <-->|WebSocket| HUB

    HUB --> G1
    HUB --> G2

    REDIS[(Redis\nBackplane)] <--> HUB

    Note1[Cada leilão = 1 grupo SignalR\nRedis Backplane para escalar\nentre múltiplas instâncias]

    style REDIS fill:#dc382d,color:#fff
```

**Detalhes:**
- Cada leilão ativo é um **grupo SignalR** — só recebe updates quem está assistindo
- **Redis Backplane** sincroniza hubs entre múltiplas instâncias do serviço
- Fallback automático para **Long Polling** se WebSocket não estiver disponível

---

## Visão Geral — Fluxo Completo de um Lance

```mermaid
flowchart TD
    CLIENT([🌐 Cliente]) -->|POST /bids\nX-Idempotency-Key| GW[🚪 API Gateway]
    GW --> BID_SVC[💰 Bid Service]

    BID_SVC --> IDEMP{🔄 Filtro de\nIdempotência\nRedis}
    IDEMP -->|Duplicado| CACHED_RES([📦 Resposta cacheada])
    IDEMP -->|Novo| VALIDATE[Valida lance]

    VALIDATE --> CONCURRENCY{🏁 Concorrência\nOtimista\nRowVersion}
    CONCURRENCY -->|Conflito| CONFLICT([409 Conflict])
    CONCURRENCY -->|OK| PERSIST[💾 Persiste lance]

    PERSIST --> CACHE_UPDATE[Atualiza cache Redis]
    PERSIST --> PUBLISH[📤 Publica BidPlaced\nno RabbitMQ]
    PERSIST --> RESPONSE([201 Created])

    PUBLISH --> RMQ[🐇 RabbitMQ]

    RMQ --> RT[📡 SignalR\nNotifica clientes]
    RMQ --> AUCTION_SVC[🏛️ Auction Service\nAtualiza maior lance]

    style IDEMP fill:#dc382d,color:#fff
    style CONCURRENCY fill:#336791,color:#fff
    style RMQ fill:#ff6600,color:#fff
    style RT fill:#4a9eff,color:#fff
```

---

## Estrutura do Projeto

```
Leilao/
├── src/
│   ├── Gateway/                          # API Gateway (YARP)
│   │   └── Program.cs
│   │
│   ├── Services/
│   │   ├── Auth.API/                     # Autenticação e autorização
│   │   │   ├── Program.cs
│   │   │   └── Controllers/
│   │   │       └── AuthController.cs
│   │   │
│   │   ├── Auction.API/                  # Gestão de leilões
│   │   │   ├── Program.cs
│   │   │   ├── Controllers/
│   │   │   │   └── AuctionsController.cs
│   │   │   ├── Consumers/
│   │   │   │   └── BidPlacedConsumer.cs
│   │   │   └── Workers/
│   │   │       └── AuctionClosureWorker.cs
│   │   │
│   │   ├── Bid.API/                      # Registro de lances
│   │   │   ├── Program.cs
│   │   │   ├── Controllers/
│   │   │   │   └── BidsController.cs
│   │   │   ├── Filters/
│   │   │   │   └── IdempotencyFilter.cs
│   │   │   └── Hubs/
│   │   │       └── AuctionHub.cs
│   │   │
│   │   └── Notification.API/            # Notificações e e-mails
│   │       ├── Program.cs
│   │       └── Consumers/
│   │           ├── AuctionClosedConsumer.cs
│   │           └── BidPlacedConsumer.cs
│   │
│   ├── Shared/                           # Contratos e utilidades compartilhadas
│   │   ├── Contracts/
│   │   │   ├── Events/
│   │   │   │   ├── BidPlacedEvent.cs
│   │   │   │   └── AuctionClosedEvent.cs
│   │   │   └── DTOs/
│   │   └── Infrastructure/
│   │       ├── Idempotency/
│   │       │   └── IdempotencyKeyStore.cs
│   │       └── Messaging/
│   │           └── RabbitMqExtensions.cs
│   │
│   └── Domain/                           # Entidades e regras de negócio
│       ├── Entities/
│       │   ├── Auction.cs
│       │   ├── Bid.cs
│       │   └── User.cs
│       └── ValueObjects/
│           └── Money.cs
│
├── tests/
│   ├── Auction.UnitTests/
│   ├── Bid.UnitTests/
│   ├── Bid.IntegrationTests/
│   └── Architecture.Tests/              # Testes de dependência entre serviços
│
├── docker-compose.yml
├── docker-compose.override.yml
└── README.md
```

---

## Como Rodar

```bash
# Subir toda a infraestrutura + serviços
docker-compose up -d

# Ou rodar serviços individualmente
dotnet run --project src/Gateway
dotnet run --project src/Services/Auction.API
dotnet run --project src/Services/Bid.API
dotnet run --project src/Services/Notification.API

# Executar testes
dotnet test
```

---

## Configuração

```json
{
  "Gateway": {
    "Routes": {
      "/api/auctions": "http://auction-service:5001",
      "/api/bids": "http://bid-service:5002",
      "/api/auth": "http://auth-service:5003"
    }
  },
  "RabbitMq": {
    "Host": "rabbitmq",
    "Port": 5672,
    "Exchanges": {
      "Bids": "bids-exchange",
      "Auctions": "auctions-exchange"
    }
  },
  "Redis": {
    "ConnectionString": "redis:6379",
    "IdempotencyTtlMinutes": 60,
    "SignalRBackplane": true
  },
  "PostgreSQL": {
    "AuctionDb": "Host=postgres;Database=auctions;...",
    "BidDb": "Host=postgres;Database=bids;..."
  }
}
```

---

## Resumo dos Desafios Técnicos

| # | Desafio | Solução | Tecnologia chave |
|---|---|---|---|
| 🏁 | Concorrência | Controle Otimista com RowVersion | PostgreSQL |
| 🔄 | Idempotência | Filtro com chave única distribuída | Redis |
| 📉 | Consistência Eventual | Workers assíncronos com retry | RabbitMQ |
| 📡 | Tempo Real | Push bidirecional com grupos | SignalR + Redis Backplane |
