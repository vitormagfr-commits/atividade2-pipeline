# Atividade 2 — Pipeline de Dados com Medallion Architecture (S3 + Athena)

Pipeline completo de **ingestão**, **validação de qualidade (Data Quality)**, **quarentena de anomalias**,
**transformação em camadas (Raw → Silver → Gold)** e **auditoria de consistência** usando
Amazon S3 e Amazon Athena (AWS Academy Learner Lab).

- Linguagem: Python 3 (`pandas`, `pyarrow`, `boto3`)
- Formatos: CSV (Raw), JSON Lines (quarentena), Parquet + Snappy (Silver e Gold)

## 1. Arquitetura

```
 gerador de dados           Data Quality               JOIN + valor_total        agregação
 (com anomalias)            (quantidade <= 0,          (pedidos + clientes       (uf x categoria)
                             chaves inexistentes)        + produtos)
      │                          │                            │                       │
      ▼                          ├── inválidos ──►  quarantine/ (JSON)                │
   raw/ (CSV)  ───────────────►  └── válidos ────►  processed/ (Parquet) ───────►  gold/ (Parquet)
                                                              │
                                                              ▼
                                          Athena: tabelas externas + auditoria
                                          ("$path", "$file_size", Raw = Silver + Quarentena)
```

## 2. Estrutura do repositório

```
.
├── README.md
├── requirements.txt
├── src/
│   ├── config.py              # argumentos e configurações
│   ├── storage.py             # leitura/escrita no S3 (ou pasta local para testes)
│   ├── ingestao_raw.py        # Etapa 1: gera dados com anomalias e grava a camada Raw
│   ├── processamento.py       # Etapa 2: Data Quality, quarentena, Silver e Gold
│   ├── auditoria_athena.py    # Etapa 3: DDL no Athena + queries de auditoria
│   └── run_pipeline.py        # Executa as 3 etapas em sequência
├── sql/
│   ├── 01_ddl_tabelas.sql     # CREATE DATABASE / CREATE EXTERNAL TABLE / MSCK REPAIR
│   └── 02_auditoria.sql       # Queries de auditoria e conciliação
└── docs/
    ├── prints/                # Capturas de tela do console do Athena (entrega)
    └── resultados/            # CSVs das consultas (gerados pelo script)
```

## 3. Estrutura gerada no S3

```
s3://<seu-bucket>/
├── raw/
│   ├── clientes/ingest_date=YYYY-MM-DD/clientes.csv
│   ├── produtos/ingest_date=YYYY-MM-DD/produtos.csv
│   └── pedidos/ingest_date=YYYY-MM-DD/pedidos.csv
├── quarantine/
│   └── pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json
├── processed/
│   └── fato_vendas/ingest_date=YYYY-MM-DD/fato_vendas.parquet
├── gold/
│   └── vendas_uf_categoria/ingest_date=YYYY-MM-DD/vendas_uf_categoria.parquet
└── athena-results/            # saída das queries do Athena
```

## 4. Regras implementadas

### Ingestão (Raw)
Geração de `clientes` (200), `produtos` (50) e `pedidos` (5.000) com semente fixa (`--seed`).
São injetadas anomalias propositais em ~3% dos pedidos **por tipo** (podem se sobrepor):

| Anomalia | Como é gerada |
|---|---|
| Quantidade inválida | `quantidade` ∈ {0, -1, -3, -10} |
| Cliente inexistente | `cliente_id` entre 9000 e 9999 (não existe em `clientes`) |
| Produto inexistente | `product_id` entre 9000 e 9999 (não existe em `produtos`) |

### Data Quality e Quarentena
Um pedido é **rejeitado** se `quantidade <= 0`, ou se `cliente_id` / `product_id` não existirem nas tabelas
dimensionais. Cada registro rejeitado recebe o campo `motivo_rejeicao`
(`QUANTIDADE_INVALIDA`, `CLIENTE_INEXISTENTE`, `PRODUTO_INEXISTENTE`; combinados com `; ` quando há mais de um).

O arquivo `rejeitados.json` usa **JSON Lines** (um objeto JSON por linha), porque é o formato que o Athena
consegue ler com o SerDe JSON.

### Silver (`processed/fato_vendas/`)
`pedidos válidos` ⨝ `clientes` ⨝ `produtos`, com `valor_total = quantidade * preco`. Parquet + Snappy.
O script falha se o JOIN perder linhas ou se as dimensões tiverem chaves duplicadas.

### Gold (`gold/vendas_uf_categoria/`)
Agregação por `uf` e `categoria`: `qtd_pedidos`, `qtd_itens`, `receita_total`, `ticket_medio`. Parquet + Snappy.

### Auditoria (Athena)
Tabelas externas para todas as camadas, particionadas (Hive-style) e consultas que comprovam:
1. arquivos físicos e tamanhos via `"$path"` e `"$file_size"`;
2. **conciliação `Raw = Silver + Quarentena`**;
3. ausência de quantidades inválidas e de chaves órfãs na Silver;
4. receita idêntica entre Silver e Gold.

## 5. Como executar (AWS Academy Learner Lab)

### 5.1 Pré-requisitos
- Python 3.9+ instalado.
- Learner Lab iniciado (**Start Lab**, aguardar o indicador verde).

### 5.2 Credenciais temporárias
No Learner Lab, clique em **AWS Details → AWS CLI → Show** e copie o bloco para o arquivo de credenciais:

- Linux/macOS: `~/.aws/credentials`
- Windows: `C:\Users\<seu-usuario>\.aws\credentials`

```ini
[default]
aws_access_key_id=...
aws_secret_access_key=...
aws_session_token=...
```

> As credenciais expiram quando a sessão do lab termina. Ao reiniciar o lab, copie o bloco novamente.

### 5.3 Criar o bucket
No console do S3, crie um bucket com nome único na região **us-east-1** (ex.: `atividade2-<seu-nome>-<ra>`).
Mantenha o *Block all public access* ativado.

### 5.4 Instalar dependências

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows (PowerShell): .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 5.5 Rodar o pipeline completo

```bash
python src/run_pipeline.py --bucket <seu-bucket>
```

Opções úteis:

| Opção | Descrição |
|---|---|
| `--ingest-date 2025-06-01` | Data da partição (padrão: hoje) |
| `--database atividade2` | Nome do database no Athena |
| `--skip-athena` | Roda só Raw, Quarentena, Silver e Gold |
| `--local-dir s3_local` | Teste local, sem AWS (não roda a etapa Athena) |

Cada etapa também pode ser executada isoladamente:

```bash
python src/ingestao_raw.py --bucket <seu-bucket>
python src/processamento.py --bucket <seu-bucket>
python src/auditoria_athena.py --bucket <seu-bucket>
```

Rodar de novo com a mesma `--ingest-date` sobrescreve os arquivos daquela partição (idempotente).
Com outra data, uma nova partição é criada.

### 5.6 Rodar as queries no console do Athena (para os prints)
1. Abra o Athena → **Settings → Manage** → *Query result location*: `s3://<seu-bucket>/athena-results/`.
2. Selecione o database `atividade2`.
3. Abra `sql/02_auditoria.sql`, substitua `{{DATABASE}}` por `atividade2` e `{{INGEST_DATE}}` pela data usada,
   e execute as consultas.
4. Tire prints de, no mínimo, **Q1** (`"$path"` e `"$file_size"`) e **Q2** (conciliação), e salve em `docs/prints/`.

## 6. Relatório de execução

| Item | Valor |
|---|---|
| Bucket | `datalake-turma-c-vitorfreitas-10781026` |
| Data de ingestão | `2026-09-20` |
| Linhas em `raw/pedidos` | 5000 |
| Linhas rejeitadas (quarentena) | 437 |
| Linhas em `processed/fato_vendas` | 4563 |
| Linhas em `gold/vendas_uf_categoria` | 30 |
| Conciliação (Q2) | OK |

Prints do console do Athena:

- `docs/prints/q1_pseudo_colunas.png`
- `docs/prints/q2_conciliacao.png`
- `docs/prints/q3_motivos_rejeicao.png` (opcional)

## 7. Problemas comuns

| Sintoma | Causa provável / solução |
|---|---|
| `ExpiredToken` / `InvalidClientTokenId` | Sessão do lab expirou. Reinicie o lab e copie as credenciais novamente. |
| `BucketAlreadyExists` / `AccessDenied` no bucket | Nome do bucket já usado por outra conta. Escolha um nome único. |
| Athena: `No output location provided` | Defina o *Query result location* nas configurações do Athena (passo 5.6). |
| Athena: tabela sem linhas | Partições não registradas. Execute `MSCK REPAIR TABLE atividade2.<tabela>;`. |
| Athena: `HIVE_BAD_DATA` em Parquet | Tipo do DDL diferente do tipo gravado. Use os DDLs de `sql/01_ddl_tabelas.sql`. |
| `AccessDenied` ao criar tabela/database | Confirme que o lab está ativo e que está na região `us-east-1`. |
