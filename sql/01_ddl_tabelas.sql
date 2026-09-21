-- Placeholders substituídos pelo script: {{BUCKET}}, {{DATABASE}}
-- Para rodar manualmente no console do Athena, troque {{BUCKET}} e {{DATABASE}} pelos seus valores.

-- Database
CREATE DATABASE IF NOT EXISTS {{DATABASE}};

-- RAW: clientes (CSV com cabeçalho, particionado por ingest_date)
CREATE EXTERNAL TABLE IF NOT EXISTS {{DATABASE}}.raw_clientes (
  cliente_id bigint,
  nome string,
  email string,
  uf string,
  cidade string,
  data_cadastro string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://{{BUCKET}}/raw/clientes/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- RAW: produtos
CREATE EXTERNAL TABLE IF NOT EXISTS {{DATABASE}}.raw_produtos (
  product_id bigint,
  nome_produto string,
  categoria string,
  preco double
)
PARTITIONED BY (ingest_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://{{BUCKET}}/raw/produtos/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- RAW: pedidos
CREATE EXTERNAL TABLE IF NOT EXISTS {{DATABASE}}.raw_pedidos (
  pedido_id bigint,
  cliente_id bigint,
  product_id bigint,
  quantidade bigint,
  data_pedido string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://{{BUCKET}}/raw/pedidos/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- QUARENTENA: pedidos rejeitados (JSON Lines, particionado por data)
CREATE EXTERNAL TABLE IF NOT EXISTS {{DATABASE}}.quarantine_pedidos_rejeitados (
  pedido_id bigint,
  cliente_id bigint,
  product_id bigint,
  quantidade bigint,
  data_pedido string,
  motivo_rejeicao string
)
PARTITIONED BY (`data` string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{{BUCKET}}/quarantine/pedidos_rejeitados/';

-- SILVER: fato_vendas (Parquet/Snappy)
CREATE EXTERNAL TABLE IF NOT EXISTS {{DATABASE}}.silver_fato_vendas (
  pedido_id bigint,
  data_pedido string,
  cliente_id bigint,
  nome_cliente string,
  uf string,
  cidade string,
  product_id bigint,
  nome_produto string,
  categoria string,
  quantidade bigint,
  preco double,
  valor_total double
)
PARTITIONED BY (ingest_date string)
STORED AS PARQUET
LOCATION 's3://{{BUCKET}}/processed/fato_vendas/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- GOLD: vendas por uf e categoria (Parquet/Snappy)
CREATE EXTERNAL TABLE IF NOT EXISTS {{DATABASE}}.gold_vendas_uf_categoria (
  uf string,
  categoria string,
  qtd_pedidos bigint,
  qtd_itens bigint,
  receita_total double,
  ticket_medio double
)
PARTITIONED BY (ingest_date string)
STORED AS PARQUET
LOCATION 's3://{{BUCKET}}/gold/vendas_uf_categoria/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Registrar partições Hive-style (ingest_date=... e data=...)
MSCK REPAIR TABLE {{DATABASE}}.raw_clientes;

MSCK REPAIR TABLE {{DATABASE}}.raw_produtos;

MSCK REPAIR TABLE {{DATABASE}}.raw_pedidos;

MSCK REPAIR TABLE {{DATABASE}}.quarantine_pedidos_rejeitados;

MSCK REPAIR TABLE {{DATABASE}}.silver_fato_vendas;

MSCK REPAIR TABLE {{DATABASE}}.gold_vendas_uf_categoria;
