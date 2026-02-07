# Sistema de Leilão Distribuído

> Microsserviços, cache, idempotência, concorrência pesada e comunicação em tempo real — tudo o que um sistema de produção real exige quando **milissegundos importam**.

---

## Por que isso NÃO é CRUD?

Este sistema **parece** ter entidades (Leilão, Lance, Usuário), mas o diferencial está nos **problemas de infraestrutura distribuída** que ele resolve:

| CRUD típico | Este projeto |
|---|---|
| `POST /bids` e pronto | Controle de concorrência otimista com versioning |
| Sem preocupação com duplicatas | Idempotência distribuída por chave única |
| Tudo síncrono | Processamento via filas + workers |
| Polling para atualizar | Real-time via WebSockets |

**Mensagem:** _"Eu construo sistemas que escalam sob pressão real."_

---

## Stack

| Camada | Tecnologia |
|---|---|
| Runtime | Node.js 20+ |
| Framework | NestJS + TypeScript |
| Banco relacional | PostgreSQL |
| Cache distribuído | Redis |
| Message Broker | RabbitMQ |
| Real-time | Socket.io (WebSockets) |
| API Gateway | NestJS + http-proxy-middleware |
| Containerização | Docker / Docker Compose |
| Testes | Jest + Testcontainers |
| ORM | TypeORM / Prisma |

---

## Arquitetura de Microsserviços

```mermaid
flowchart TB
    Client([🌐 Cliente]) --> GW[🚪 API Gateway\nNestJS]

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

    NOTIFY -->|Socket.io\nWebSocket| Client

    style GW fill:#ef4444,color:#fff
    style RMQ fill:#ff6600,color:#fff
    style CACHE fill:#dc382d,color:#fff
```

---

## 1. Concorrência & Race Conditions 🏁

Num leilão, **milissegundos importam**. Para impedir que dois usuários registrem o mesmo lance simultaneamente:

### Controle de Concorrência Otimista com Versionamento

O PostgreSQL rejeita atualizações conflitantes no nível da transação, garantindo integridade **sem travar a tabela inteira**.

```mermaid
sequenceDiagram
    actor User_A as Usuário A
    actor User_B as Usuário B
    participant API as Bid Service
    participant DB as PostgreSQL

    Note over DB: Leilão: maior lance = R$100<br/>version = 1

    par Requisições simultâneas
        User_A->>API: POST /bids (R$150)
        User_B->>API: POST /bids (R$150)
    end

    API->>DB: UPDATE ... WHERE id = X AND version = 1
    Note over DB: ✅ User A: Sucesso!<br/>version → 2

    API->>DB: UPDATE ... WHERE id = X AND version = 1
    Note over DB: ❌ User B: 0 rows affected<br/>version não bate!

    DB-->>API: ✅ 1 row affected (User A)
    DB-->>API: ⚠️ 0 rows affected (User B)

    API-->>User_A: 201 Created — Lance aceito!
    API-->>User_B: 409 Conflict — Lance conflitante, tente novamente
```

**Como funciona:**
1. Toda entidade de Leilão possui uma coluna `version` (inteiro auto-incrementado)
2. Ao registrar um lance, o `UPDATE` inclui `WHERE version = @expected`
3. Se outro lance chegou antes, a `version` já mudou → **0 rows affected**
4. O serviço detecta o conflito e retorna **409 Conflict**
5. O cliente pode re-tentar com o valor atualizado

### Implementação TypeScript com TypeORM

```typescript
// auction.entity.ts
import { Entity, Column, UpdateDateColumn, VersionColumn } from 'typeorm';

@Entity('auctions')
export class Auction {
  @PrimaryColumn('uuid')
  id: string;

  @Column('text')
  title: string;

  @Column('decimal', { precision: 10, scale: 2 })
  highestBid: number;

  @VersionColumn()
  version: number;

  @UpdateDateColumn()
  updatedAt: Date;
}

// bid.service.ts
export class BidService {
  async placeBid(auctionId: string, amount: number, version: number) {
    const result = await this.auctionRepository.update(
      { id: auctionId, version },
      { highestBid: amount, version: () => 'version + 1' }
    );

    if (result.affected === 0) {
      throw new ConflictException('Lance conflitante. Tente novamente.');
    }

    return { success: true };
  }
}
```

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
2. O **IdempotencyInterceptor** verifica no Redis se essa key já foi processada
3. Se sim → retorna a **resposta cacheada** (sem reprocessar)
4. Se não → **processa** e armazena o resultado com TTL
5. Se falhar → **remove a key** para permitir retry legítimo

### Implementação com NestJS

```typescript
// idempotency.interceptor.ts
import { Injectable, NestInterceptor, ExecutionContext, CallHandler } from '@nestjs/common';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';
import { RedisService } from './redis.service';

@Injectable()
export class IdempotencyInterceptor implements NestInterceptor {
  constructor(private redis: RedisService) {}

  async intercept(
    context: ExecutionContext,
    next: CallHandler,
  ): Promise<Observable<any>> {
    const request = context.switchToHttp().getRequest();
    const idempotencyKey = request.headers['x-idempotency-key'];

    if (!idempotencyKey) {
      return next.handle();
    }

    const cached = await this.redis.get(`idempotency:${idempotencyKey}`);
    if (cached) {
      return cached;
    }

    // Seta status PROCESSING
    await this.redis.setex(`idempotency:${idempotencyKey}`, 60, 'PROCESSING');

    return next.handle().pipe(
      tap(async (response) => {
        // Armazena resposta por 24h
        await this.redis.setex(
          `idempotency:${idempotencyKey}`,
          86400,
          JSON.stringify(response),
        );
      }),
    );
  }
}
```

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
        Notify->>Notify: 📡 Push via Socket.io
    end
```

**Benefícios:**
- A API responde **instantaneamente** (não espera envio de e-mail)
- Se o envio de notificação falhar, o **retry automático** da fila cuida
- O fechamento do leilão é **atômico** no banco, **eventual** nas notificações

### Implementação do Consumer RabbitMQ

```typescript
// auction-closing.consumer.ts
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { AmqpConnection } from '@golevelup/nestjs-rabbitmq';

@Controller()
export class AuctionClosingConsumer {
  constructor(private amqp: AmqpConnection) {}

  @EventPattern('auction.closing')
  async handleAuctionClosing(@Payload() payload: AuctionClosingEvent) {
    try {
      const winner = await this.auctionService.getWinner(payload.auctionId);
      await this.paymentService.charge(winner.id, winner.amount);
      await this.auctionService.updateStatus(payload.auctionId, 'CLOSED');
      
      // Publica novo evento
      await this.amqp.publish('auctions-exchange', 'auction.closed', {
        auctionId: payload.auctionId,
        winnerId: winner.id,
      });
    } catch (error) {
      // Será retentado automaticamente pela fila
      throw error;
    }
  }
}
```

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

**Socket.io** empurra atualizações de lances para todos os clientes conectados **instantaneamente**. Sem "atualizar página". A UI atualiza o preço e notifica se você foi superado — em tempo real.

```mermaid
sequenceDiagram
    actor User_A as Usuário A
    actor User_B as Usuário B
    participant API as Bid Service
    participant Hub as Socket.io Hub
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

### Arquitetura do Hub Socket.io

```mermaid
flowchart LR
    subgraph Clients["🌐 Clientes Conectados"]
        C1[Usuário A]
        C2[Usuário B]
        C3[Usuário C]
    end

    subgraph Socket["📡 Socket.io"]
        HUB[Hub Central]
        G1["Sala: auction:1"]
        G2["Sala: auction:2"]
    end

    C1 <-->|WebSocket| HUB
    C2 <-->|WebSocket| HUB
    C3 <-->|WebSocket| HUB

    HUB --> G1
    HUB --> G2

    REDIS[(Redis\nAdapter)] <--> HUB

    Note1[Cada leilão = 1 sala Socket.io<br/>Redis Adapter para escalar<br/>entre múltiplas instâncias]

    style REDIS fill:#dc382d,color:#fff
```

**Detalhes:**
- Cada leilão ativo é uma **sala Socket.io** — só recebe updates quem está na sala
- **Redis Adapter** sincroniza hubs entre múltiplas instâncias do serviço
- Fallback automático para **polling** se WebSocket não estiver disponível

### Implementação com Socket.io

```typescript
// auction.gateway.ts
import { WebSocketGateway, WebSocketServer, SubscribeMessage } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';

@WebSocketGateway({ namespace: 'auctions', cors: { origin: '*' } })
export class AuctionGateway {
  @WebSocketServer() server: Server;

  constructor(private amqp: AmqpConnection) {
    this.setupMessageConsumer();
  }

  @SubscribeMessage('join-auction')
  handleJoinAuction(client: Socket, data: { auctionId: string }) {
    client.join(`auction:${data.auctionId}`);
    return { status: 'joined' };
  }

  private setupMessageConsumer() {
    this.amqp.consume('bids-queue', async (msg) => {
      const event = JSON.parse(msg.content.toString());
      
      // Broadcast para todos na sala do leilão
      this.server
        .to(`auction:${event.auctionId}`)
        .emit('bid-placed', {
          amount: event.amount,
          winnerId: event.userId,
          timestamp: new Date(),
        });
    });
  }
}
```

---

## Visão Geral — Fluxo Completo de um Lance

```mermaid
flowchart TD
    CLIENT([🌐 Cliente]) -->|POST /bids\nX-Idempotency-Key| GW[🚪 API Gateway]
    GW --> BID_SVC[💰 Bid Service]

    BID_SVC --> IDEMP{🔄 Filtro de\nIdempotência\nRedis}
    IDEMP -->|Duplicado| CACHED_RES([📦 Resposta cacheada])
    IDEMP -->|Novo| VALIDATE[Valida lance]

    VALIDATE --> CONCURRENCY{🏁 Concorrência\nOtimista\nVersion}
    CONCURRENCY -->|Conflito| CONFLICT([409 Conflict])
    CONCURRENCY -->|OK| PERSIST[💾 Persiste lance]

    PERSIST --> CACHE_UPDATE[Atualiza cache Redis]
    PERSIST --> PUBLISH[📤 Publica BidPlaced\nno RabbitMQ]
    PERSIST --> RESPONSE([201 Created])

    PUBLISH --> RMQ[🐇 RabbitMQ]

    RMQ --> RT[📡 Socket.io\nNotifica clientes]
    RMQ --> AUCTION_SVC[🏛️ Auction Service\nAtualiza maior lance]

    style IDEMP fill:#dc382d,color:#fff
    style CONCURRENCY fill:#336791,color:#fff
    style RMQ fill:#ff6600,color:#fff
    style RT fill:#ef4444,color:#fff
```

---

## Estrutura do Projeto

```
leilao/
├── src/
│   ├── gateway/                          # API Gateway (http-proxy-middleware)
│   │   ├── main.ts
│   │   └── gateway.module.ts
│   │
│   ├── services/
│   │   ├── auth/                         # Autenticação e autorização
│   │   │   ├── auth.controller.ts
│   │   │   ├── auth.service.ts
│   │   │   └── auth.module.ts
│   │   │
│   │   ├── auction/                      # Gestão de leilões
│   │   │   ├── auction.controller.ts
│   │   │   ├── auction.service.ts
│   │   │   ├── auction.entity.ts
│   │   │   ├── auction-closing.consumer.ts
│   │   │   ├── auction-closing.scheduler.ts
│   │   │   └── auction.module.ts
│   │   │
│   │   ├── bid/                          # Registro de lances
│   │   │   ├── bid.controller.ts
│   │   │   ├── bid.service.ts
│   │   │   ├── bid.entity.ts
│   │   │   ├── bid.gateway.ts
│   │   │   ├── idempotency.interceptor.ts
│   │   │   └── bid.module.ts
│   │   │
│   │   └── notification/                 # Notificações e e-mails
│   │       ├── notification.service.ts
│   │       ├── auction-closed.consumer.ts
│   │       ├── bid-placed.consumer.ts
│   │       └── notification.module.ts
│   │
│   ├── shared/                           # Contratos e utilidades compartilhadas
│   │   ├── contracts/
│   │   │   ├── events/
│   │   │   │   ├── bid-placed.event.ts
│   │   │   │   └── auction-closed.event.ts
│   │   │   └── dtos/
│   │   │       ├── place-bid.dto.ts
│   │   │       └── auction.dto.ts
│   │   ├── infrastructure/
│   │   │   ├── redis/
│   │   │   │   └── redis.service.ts
│   │   │   ├── rabbitmq/
│   │   │   │   └── rabbitmq.service.ts
│   │   │   └── database/
│   │   │       └── database.module.ts
│   │   └── entities/
│   │       ├── auction.entity.ts
│   │       ├── bid.entity.ts
│   │       └── user.entity.ts
│   │
│   └── app.module.ts
│
├── test/
│   ├── auction.spec.ts
│   ├── bid.spec.ts
│   ├── bid.integration.spec.ts
│   └── architecture.spec.ts
│
├── docker-compose.yml
├── docker-compose.override.yml
├── package.json
├── tsconfig.json
├── .prettierrc
└── README.md
```

---

## Como Rodar

```bash
# Instalar dependências
npm install

# Subir toda a infraestrutura + serviços
docker-compose up -d

# Ou rodar serviços individualmente
npm run start:gateway
npm run start:auction
npm run start:bid
npm run start:notification

# Executar testes
npm run test

# Modo desenvolvimento com watch
npm run start:dev
```

---

## Configuração

```json
{
  "gateway": {
    "port": 3000,
    "routes": {
      "/api/auctions": "http://localhost:3001",
      "/api/bids": "http://localhost:3002",
      "/api/auth": "http://localhost:3003"
    }
  },
  "rabbitmq": {
    "uri": "amqp://guest:guest@rabbitmq:5672",
    "exchanges": {
      "bids": "bids-exchange",
      "auctions": "auctions-exchange"
    }
  },
  "redis": {
    "host": "redis",
    "port": 6379,
    "idempotencyTtlSeconds": 3600,
    "socketioAdapter": true
  },
  "postgres": {
    "auctionDb": {
      "host": "postgres",
      "port": 5432,
      "database": "auctions",
      "username": "postgres",
      "password": "postgres"
    },
    "bidDb": {
      "host": "postgres",
      "port": 5432,
      "database": "bids",
      "username": "postgres",
      "password": "postgres"
    }
  }
}
```

---

## Resumo dos Desafios Técnicos

| # | Desafio | Solução | Tecnologia chave |
|---|---|---|---|
| 🏁 | Concorrência | Controle Otimista com Versionamento | PostgreSQL + TypeORM |
| 🔄 | Idempotência | Filtro com chave única distribuída | Redis + Interceptor |
| 📉 | Consistência Eventual | Workers assíncronos com retry | RabbitMQ + @nestjs/microservices |
| 📡 | Tempo Real | Push bidirecional com salas | Socket.io + Redis Adapter |

---

## Dependências Principais

```json
{
  "dependencies": {
    "@nestjs/common": "^10.0.0",
    "@nestjs/core": "^10.0.0",
    "@nestjs/platform-express": "^10.0.0",
    "@nestjs/websockets": "^10.0.0",
    "@nestjs/microservices": "^10.0.0",
    "@golevelup/nestjs-rabbitmq": "^3.7.0",
    "@nestjs/typeorm": "^10.0.0",
    "typeorm": "^0.3.0",
    "pg": "^8.11.0",
    "redis": "^4.6.0",
    "socket.io": "^4.7.0",
    "amqplib": "^0.10.0",
    "class-validator": "^0.14.0",
    "class-transformer": "^0.5.0"
  },
  "devDependencies": {
    "@nestjs/testing": "^10.0.0",
    "@types/jest": "^29.5.0",
    "jest": "^29.5.0",
    "ts-jest": "^29.1.0",
    "testcontainers": "^9.12.0"
  }
}
```
