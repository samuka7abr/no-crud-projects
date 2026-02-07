# Rate Limiter + API Gateway Simplificado

> Um serviço que fica **na frente** de outras APIs, protegendo-as contra abuso, sobrecarga e requisições mal-intencionadas — sem banco de dados, sem CRUD.

---

## Por que isso NÃO é CRUD?

| CRUD típico | Este projeto |
|---|---|
| Entidades persistentes (User, Product…) | Sem entidade persistente |
| Listar / Criar / Editar / Deletar | Algoritmo + Infraestrutura |
| Banco relacional como centro | Cache em memória como centro |

**Mensagem:** _"Eu penso em proteção de sistema, não em telinha."_

---

## Stack

| Camada | Tecnologia |
|---|---|
| Runtime | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/dotnetcore/dotnetcore-original.svg" width="20"/> .NET 9 (Minimal API) |
| Cache distribuído | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="20"/> Redis |
| Cache local (fallback) | `IMemoryCache` (in-process) |
| Containerização | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" width="20"/> Docker / Docker Compose |
| Testes | <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/dotnetcore/dotnetcore-original.svg" width="20"/> xUnit + Testcontainers |

---

## Arquitetura Geral

```mermaid
flowchart LR
    Client([🌐 Cliente]) -->|HTTP Request| GW[API Gateway]

    subgraph Gateway["⚙️ API Gateway"]
        direction TB
        GW --> MW{Middleware\nRate Limit}
        MW -->|Permitido| Proxy[Reverse Proxy]
        MW -->|Bloqueado| R429[429 Too Many\nRequests]
    end

    subgraph Cache["💾 Camada de Cache"]
        direction TB
        Redis[(Redis)]
        MemCache[(IMemoryCache\nFallback)]
    end

    MW <-->|Consulta/Incrementa\ncontador| Redis
    Redis -. falha .-> MemCache
    Proxy -->|Repassa| API1[API Downstream A]
    Proxy -->|Repassa| API2[API Downstream B]
```

---

## Algoritmo de Rate Limiting

### Token Bucket

O algoritmo escolhido é o **Token Bucket** — simples, eficiente e amplamente usado em produção (AWS, Cloudflare, Kong).

```mermaid
flowchart TD
    REQ([Nova Requisição]) --> CHECK{Bucket tem\ntokens?}
    CHECK -->|Sim| CONSUME[Consome 1 token]
    CONSUME --> ALLOW[✅ Requisição\nPermitida]
    CHECK -->|Não| DENY[❌ 429 Too Many\nRequests]

    REFILL([⏰ Timer]) -->|Reabastece tokens\na cada intervalo| BUCKET[(Token Bucket\nmax: N tokens)]
    BUCKET --> CHECK
```

**Como funciona:**
1. Cada IP/token recebe um **bucket** com capacidade máxima de `N` tokens
2. Cada requisição **consome 1 token**
3. Tokens são **reabastecidos** a uma taxa fixa (ex: 10 tokens/segundo)
4. Se o bucket estiver vazio → **429 Too Many Requests**

---

## Headers Customizados

Toda resposta inclui headers de rate limit seguindo o padrão de mercado:

```
HTTP/1.1 200 OK
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 73
X-RateLimit-Reset: 1672531200
Retry-After: 30          ← (apenas no 429)
```

---

## Fallback — Quando o Redis Cai

```mermaid
flowchart TD
    REQ([Requisição]) --> TRY[Tenta acessar Redis]
    TRY -->|✅ Online| REDIS[(Redis)]
    TRY -->|❌ Timeout / Erro| FALLBACK[Fallback para\nIMemoryCache]
    FALLBACK --> LOCAL[(Cache Local\nin-process)]
    REDIS --> DECIDE{Dentro do\nlimite?}
    LOCAL --> DECIDE
    DECIDE -->|Sim| PASS[✅ Passa]
    DECIDE -->|Não| BLOCK[❌ 429]

    HEALTH([Health Check\nperiódico]) -->|Redis voltou?| RECOVER[Restaura conexão\ncom Redis]
```

**Estratégia:**
- Health check periódico tenta reconectar ao Redis
- Enquanto o Redis estiver fora, o `IMemoryCache` assume com limites locais por instância
- Logs estruturados registram toda troca de provider

---

## Estrutura do Projeto

```
Rate_limiter_API_gateway/
├── src/
│   ├── Gateway.API/                  # Entry point — Minimal API
│   │   ├── Program.cs
│   │   ├── Middlewares/
│   │   │   └── RateLimitMiddleware.cs
│   │   └── Extensions/
│   │       └── ServiceCollectionExtensions.cs
│   ├── Gateway.Core/                 # Algoritmos e interfaces
│   │   ├── Interfaces/
│   │   │   └── IRateLimitStrategy.cs
│   │   ├── Strategies/
│   │   │   └── TokenBucketStrategy.cs
│   │   └── Models/
│   │       └── RateLimitResult.cs
│   └── Gateway.Infrastructure/       # Redis, fallback, proxy
│       ├── Cache/
│       │   ├── RedisCacheProvider.cs
│       │   └── InMemoryFallbackProvider.cs
│       └── Proxy/
│           └── ReverseProxyHandler.cs
├── tests/
│   ├── Gateway.UnitTests/
│   └── Gateway.IntegrationTests/
├── docker-compose.yml
└── README.md
```

---

## Como Rodar

```bash
# Subir a infraestrutura
docker-compose up -d

# Rodar a API
dotnet run --project src/Gateway.API

# Executar testes
dotnet test
```

---

## Configuração

```json
{
  "RateLimit": {
    "MaxTokens": 100,
    "RefillRate": 10,
    "RefillIntervalSeconds": 1,
    "KeyStrategy": "IP"
  },
  "Redis": {
    "ConnectionString": "localhost:6379",
    "FallbackToMemory": true,
    "HealthCheckIntervalSeconds": 5
  },
  "DownstreamApis": [
    { "Path": "/api/service-a", "Target": "http://localhost:5001" },
    { "Path": "/api/service-b", "Target": "http://localhost:5002" }
  ]
}
```
