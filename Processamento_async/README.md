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
| Runtime | Node.js 20+ |
| Framework | NestJS + TypeScript |
| Message Broker | RabbitMQ |
| Cache / Idempotência | Redis |
| Banco (status dos jobs) | PostgreSQL |
| ORM | TypeORM / Prisma |
| Containerização | Docker / Docker Compose |
| Testes | Jest + Testcontainers |

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

## Implementação: Job Entity e Status

```typescript
// job.entity.ts
import { Entity, Column, PrimaryGeneratedColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

export enum JobStatus {
  PENDING = 'PENDING',
  RUNNING = 'RUNNING',
  RETRYING = 'RETRYING',
  DONE = 'DONE',
  FAILED = 'FAILED',
}

@Entity('jobs')
export class Job {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column('varchar')
  type: string; // 'CSV_PROCESS', 'REPORT_GENERATE', 'SEND_NOTIFICATIONS'

  @Column('jsonb')
  payload: Record<string, any>;

  @Column('enum', { enum: JobStatus })
  status: JobStatus = JobStatus.PENDING;

  @Column('integer', { default: 0 })
  attempts: number = 0;

  @Column('integer', { default: 5 })
  maxAttempts: number = 5;

  @Column('text', { nullable: true })
  error: string;

  @Column('jsonb', { nullable: true })
  result: Record<string, any>;

  @Column('varchar')
  idempotencyKey: string; // Único por requisição

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
```

---

## Implementação: API REST com Idempotência

```typescript
// jobs.controller.ts
import { Controller, Post, Get, Body, Param, Headers, HttpCode } from '@nestjs/common';
import { JobsService } from './jobs.service';
import { CreateJobDto } from './dto/create-job.dto';

@Controller('jobs')
export class JobsController {
  constructor(private jobsService: JobsService) {}

  @Post()
  @HttpCode(202) // Accepted — não é resposta imediata
  async createJob(
    @Body() createJobDto: CreateJobDto,
    @Headers('x-idempotency-key') idempotencyKey: string,
  ) {
    const job = await this.jobsService.createJob(createJobDto, idempotencyKey);
    return {
      jobId: job.id,
      status: job.status,
      createdAt: job.createdAt,
    };
  }

  @Get(':id')
  async getJobStatus(@Param('id') jobId: string) {
    const job = await this.jobsService.getJob(jobId);
    return {
      jobId: job.id,
      status: job.status,
      attempts: job.attempts,
      maxAttempts: job.maxAttempts,
      result: job.result,
      error: job.error,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
    };
  }
}

// jobs.service.ts
import { Injectable } from '@nestjs/common';
import { Repository } from 'typeorm';
import { InjectRepository } from '@nestjs/typeorm';
import { AmqpConnection } from '@golevelup/nestjs-rabbitmq';
import { Job, JobStatus } from './entities/job.entity';
import { RedisService } from '../shared/redis/redis.service';

@Injectable()
export class JobsService {
  constructor(
    @InjectRepository(Job) private jobRepository: Repository<Job>,
    private amqp: AmqpConnection,
    private redis: RedisService,
  ) {}

  async createJob(payload: any, idempotencyKey: string) {
    // Verifica se job já foi criado com essa chave
    const cached = await this.redis.get(`job:idempotency:${idempotencyKey}`);
    if (cached) {
      const jobId = JSON.parse(cached);
      return this.getJob(jobId);
    }

    // Cria novo job
    const job = this.jobRepository.create({
      type: payload.type,
      payload,
      status: JobStatus.PENDING,
      idempotencyKey,
    });

    await this.jobRepository.save(job);

    // Armazena idempotência por 24h
    await this.redis.setex(
      `job:idempotency:${idempotencyKey}`,
      86400,
      JSON.stringify(job.id),
    );

    // Publica mensagem na fila
    await this.amqp.publish('jobs-exchange', `job.${payload.type}`, {
      jobId: job.id,
      payload: job.payload,
      idempotencyKey,
    });

    return job;
  }

  async getJob(jobId: string) {
    const job = await this.jobRepository.findOne({ where: { id: jobId } });
    if (!job) throw new Error('Job not found');
    return job;
  }
}
```

---

## Implementação: Worker com Retry

```typescript
// csv-processor.consumer.ts
import { Controller } from '@nestjs/common';
import { EventPattern, Payload, Ctx } from '@nestjs/microservices';
import { RabbitContext } from '@golevelup/nestjs-rabbitmq';
import { Repository } from 'typeorm';
import { InjectRepository } from '@nestjs/typeorm';
import { Job, JobStatus } from '../entities/job.entity';
import { RedisService } from '../shared/redis/redis.service';

@Controller()
export class CsvProcessorConsumer {
  constructor(
    @InjectRepository(Job) private jobRepository: Repository<Job>,
    private redis: RedisService,
  ) {}

  @EventPattern('job.CSV_PROCESS')
  async processCsvJob(
    @Payload() message: { jobId: string; payload: any; idempotencyKey: string },
    @Ctx() context: RabbitContext,
  ) {
    const { jobId, payload, idempotencyKey } = message;

    try {
      // Verifica idempotência do consumo
      const processed = await this.redis.get(`job:consumed:${idempotencyKey}`);
      if (processed) {
        console.log(`Job ${jobId} já foi consumido. Ignorando.`);
        context.getChannelRef().ack(context.getMessage());
        return;
      }

      // Marca como em processamento
      const job = await this.jobRepository.findOne({ where: { id: jobId } });
      job.status = JobStatus.RUNNING;
      job.attempts += 1;
      await this.jobRepository.save(job);

      // Processa CSV
      const result = await this.processcsv(payload.filePath);

      // Marca como processado
      job.status = JobStatus.DONE;
      job.result = result;
      await this.jobRepository.save(job);

      // Armazena no Redis que foi consumido
      await this.redis.setex(`job:consumed:${idempotencyKey}`, 86400, 'true');

      // ACK — remove da fila
      context.getChannelRef().ack(context.getMessage());
    } catch (error) {
      console.error(`Erro ao processar job ${jobId}:`, error);

      const job = await this.jobRepository.findOne({ where: { id: jobId } });

      if (job.attempts < job.maxAttempts) {
        // Retry com exponential backoff
        job.status = JobStatus.RETRYING;
        job.error = error.message;
        await this.jobRepository.save(job);

        const delay = Math.pow(2, job.attempts) * 1000 + Math.random() * 1000;
        await this.sleep(delay);

        // NACK com requeue
        context.getChannelRef().nack(context.getMessage(), false, true);
      } else {
        // Esgotou tentativas — vai para DLQ
        job.status = JobStatus.FAILED;
        job.error = error.message;
        await this.jobRepository.save(job);

        // NACK sem requeue (vai para DLQ)
        context.getChannelRef().nack(context.getMessage(), false, false);
      }
    }
  }

  private async processcsv(filePath: string) {
    // Implementação do processamento
    return { processedRows: 1000, errors: 0 };
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
```

---

## Implementação: Configuração do RabbitMQ

```typescript
// jobs.module.ts
import { Module } from '@nestjs/common';
import { RabbitModule } from '@golevelup/nestjs-rabbitmq';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Job } from './entities/job.entity';
import { JobsController } from './jobs.controller';
import { JobsService } from './jobs.service';
import { CsvProcessorConsumer } from './consumers/csv-processor.consumer';

@Module({
  imports: [
    TypeOrmModule.forFeature([Job]),
    RabbitModule.forRoot(RabbitModule.forAsyncOptions({
      useFactory: () => ({
        uri: process.env.RABBITMQ_URI || 'amqp://guest:guest@localhost:5672',
        connectionInitOptions: { wait: true },
        defaultRpcTimeout: 10000,
        exchanges: [
          {
            name: 'jobs-exchange',
            type: 'topic',
            options: { durable: true },
          },
        ],
        queues: [
          {
            name: 'csv-processing-queue',
            options: { durable: true },
            exchange: { name: 'jobs-exchange', binding: 'job.CSV_PROCESS' },
            deadLetterExchange: {
              name: 'jobs-dlx',
              type: 'topic',
              routing: 'job.CSV_PROCESS.dlq',
            },
          },
          {
            name: 'report-generation-queue',
            options: { durable: true },
            exchange: { name: 'jobs-exchange', binding: 'job.REPORT_GENERATE' },
            deadLetterExchange: {
              name: 'jobs-dlx',
              type: 'topic',
              routing: 'job.REPORT_GENERATE.dlq',
            },
          },
          {
            name: 'jobs-dlq',
            options: { durable: true },
            exchange: { name: 'jobs-dlx', binding: 'job.*.dlq' },
          },
        ],
      }),
    })),
  ],
  controllers: [JobsController, CsvProcessorConsumer],
  providers: [JobsService],
})
export class JobsModule {}
```

---

## Monitoramento: Dead Letter Queue Handler

```typescript
// dlq.consumer.ts
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { Repository } from 'typeorm';
import { InjectRepository } from '@nestjs/typeorm';
import { Job } from '../entities/job.entity';
import { AlertService } from '../shared/alert/alert.service';

@Controller()
export class DlqConsumer {
  constructor(
    @InjectRepository(Job) private jobRepository: Repository<Job>,
    private alertService: AlertService,
  ) {}

  @EventPattern('job.*.dlq')
  async handleDlq(@Payload() message: { jobId: string; error: string }) {
    const { jobId, error } = message;

    // Consulta job no banco
    const job = await this.jobRepository.findOne({ where: { id: jobId } });

    if (job) {
      console.error(`[DLQ] Job ${jobId} esgotou todas as tentativas:`, error);

      // Envia alerta
      await this.alertService.sendAlert({
        severity: 'CRITICAL',
        title: `Job ${jobId} falhou permanentemente`,
        message: `Job ${job.type} falhou após ${job.maxAttempts} tentativas. Erro: ${error}`,
      });
    }
  }
}
```

---

## Estrutura do Projeto

```
jobs-async/
├── src/
│   ├── jobs/                           # API REST — recebe e consulta jobs
│   │   ├── jobs.controller.ts
│   │   ├── jobs.service.ts
│   │   ├── jobs.module.ts
│   │   ├── dto/
│   │   │   └── create-job.dto.ts
│   │   └── entities/
│   │       └── job.entity.ts
│   │
│   ├── consumers/                       # Workers — processam jobs da fila
│   │   ├── csv-processor.consumer.ts
│   │   ├── report-generator.consumer.ts
│   │   ├── notification.consumer.ts
│   │   └── dlq.consumer.ts
│   │
│   ├── shared/                          # Infraestrutura compartilhada
│   │   ├── redis/
│   │   │   └── redis.service.ts
│   │   ├── rabbitmq/
│   │   │   └── rabbitmq.config.ts
│   │   ├── database/
│   │   │   └── database.module.ts
│   │   └── alert/
│   │       └── alert.service.ts
│   │
│   └── app.module.ts
│
├── test/
│   ├── jobs.spec.ts
│   ├── consumer.integration.spec.ts
│   └── retry-strategy.spec.ts
│
├── docker-compose.yml
├── package.json
├── tsconfig.json
└── README.md
```

---

## Como Rodar

```bash
# Instalar dependências
npm install

# Subir infraestrutura (RabbitMQ, Redis, PostgreSQL)
docker-compose up -d

# Rodar a API
npm run start

# Rodar o worker (em outro terminal/container)
npm run start:worker

# Executar testes
npm run test

# Modo desenvolvimento com watch
npm run start:dev
```

---

## Configuração

```yaml
# .env
RABBITMQ_URI=amqp://guest:guest@rabbitmq:5672
REDIS_HOST=redis
REDIS_PORT=6379
DATABASE_URL=postgresql://postgres:postgres@postgres:5432/jobs

# Retry strategy
JOB_MAX_ATTEMPTS=5
JOB_BASE_DELAY_MS=2000
JOB_USE_JITTER=true

# Observabilidade
LOG_LEVEL=info
SENTRY_DSN=https://...
```

---

## Docker Compose

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: jobs
      POSTGRES_PASSWORD: postgres
    ports:
      - '5432:5432'
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - '6379:6379'

  rabbitmq:
    image: rabbitmq:3.12-management-alpine
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    ports:
      - '5672:5672'
      - '15672:15672' # Management UI
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq

  api:
    build: .
    command: npm run start
    ports:
      - '3000:3000'
    depends_on:
      - postgres
      - redis
      - rabbitmq
    environment:
      NODE_ENV: development
      DATABASE_URL: postgresql://postgres:postgres@postgres:5432/jobs
      RABBITMQ_URI: amqp://guest:guest@rabbitmq:5672

  worker:
    build: .
    command: npm run start:worker
    depends_on:
      - postgres
      - redis
      - rabbitmq
    environment:
      NODE_ENV: development
      DATABASE_URL: postgresql://postgres:postgres@postgres:5432/jobs
      RABBITMQ_URI: amqp://guest:guest@rabbitmq:5672
    deploy:
      replicas: 2 # Escala horizontalmente

volumes:
  postgres_data:
  rabbitmq_data:
```

---

## Observabilidade

| O quê | Como |
|---|---|
| Logs estruturados | Winston com correlation ID por job |
| Métricas | Jobs processados, falhos, tempo médio, taxa de retry |
| Alertas | Jobs na DLQ disparam notificação via Slack/Email |
| Dashboard | RabbitMQ Management UI (`localhost:15672`) + Prometheus |
| Tracing | OpenTelemetry para rastrear job em múltiplos serviços |

---

## Exemplo: Chamar a API

```bash
# Criar um job CSV
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: user-123-2024-02-01" \
  -d '{
    "type": "CSV_PROCESS",
    "filePath": "s3://bucket/data.csv",
    "options": {
      "delimiter": ",",
      "skipRows": 0
    }
  }'

# Resposta
# HTTP 202 Accepted
# {
#   "jobId": "550e8400-e29b-41d4-a716-446655440000",
#   "status": "PENDING",
#   "createdAt": "2024-02-01T10:00:00Z"
# }

# Consultar status
curl http://localhost:3000/jobs/550e8400-e29b-41d4-a716-446655440000

# Resposta eventual
# {
#   "jobId": "550e8400-e29b-41d4-a716-446655440000",
#   "status": "DONE",
#   "attempts": 1,
#   "maxAttempts": 5,
#   "result": {
#     "processedRows": 10000,
#     "errors": 0
#   },
#   "createdAt": "2024-02-01T10:00:00Z",
#   "updatedAt": "2024-02-01T10:05:30Z"
# }
```

---

## Resumo dos Padrões Implementados

| # | Padrão | Descrição | Tecnologia chave |
|---|---|---|---|
| 🎯 | Fire-and-Forget | API responde imediatamente (202 Accepted) | HTTP Status Codes |
| 🔄 | Idempotência | Mesma requisição = mesmo resultado | Redis + Idempotency-Key |
| 🔁 | Retry com Backoff | Reprocessamento automático com delay | RabbitMQ + setTimeout |
| 💀 | Dead Letter Queue | Jobs que falharam vão para DLQ | RabbitMQ DLX |
| 📊 | Polling | Cliente consulta status do job | GET /jobs/{id} |
| 🔐 | Transactional Outbox | Job salvo no banco + fila = atomicidade | PostgreSQL + RabbitMQ |
