-- Placeholders: {{DATABASE}}, {{INGEST_DATE}}
-- Cada consulta começa com um comentário "-- Qn: ..." usado como título na saída do script.

-- Q1: Arquivos fisicos por camada (pseudo-colunas "$path" e "$file_size")
SELECT 'raw_clientes' AS tabela, "$path" AS arquivo_s3, "$file_size" AS tamanho_bytes, COUNT(*) AS linhas
FROM {{DATABASE}}.raw_clientes
WHERE ingest_date = '{{INGEST_DATE}}'
GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'raw_produtos', "$path", "$file_size", COUNT(*)
FROM {{DATABASE}}.raw_produtos
WHERE ingest_date = '{{INGEST_DATE}}'
GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'raw_pedidos', "$path", "$file_size", COUNT(*)
FROM {{DATABASE}}.raw_pedidos
WHERE ingest_date = '{{INGEST_DATE}}'
GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'quarantine_pedidos_rejeitados', "$path", "$file_size", COUNT(*)
FROM {{DATABASE}}.quarantine_pedidos_rejeitados
WHERE "data" = '{{INGEST_DATE}}'
GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'silver_fato_vendas', "$path", "$file_size", COUNT(*)
FROM {{DATABASE}}.silver_fato_vendas
WHERE ingest_date = '{{INGEST_DATE}}'
GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'gold_vendas_uf_categoria', "$path", "$file_size", COUNT(*)
FROM {{DATABASE}}.gold_vendas_uf_categoria
WHERE ingest_date = '{{INGEST_DATE}}'
GROUP BY "$path", "$file_size"
ORDER BY 1;

-- Q2: CONCILIACAO DE INTEGRIDADE - Raw = Silver + Quarentena
SELECT r.total AS total_raw,
       s.total AS total_silver,
       q.total AS total_quarentena,
       s.total + q.total AS silver_mais_quarentena,
       r.total - (s.total + q.total) AS diferenca,
       CASE WHEN r.total = s.total + q.total
            THEN 'OK - INTEGRIDADE CONFIRMADA'
            ELSE 'DIVERGENCIA' END AS status
FROM (SELECT COUNT(*) AS total FROM {{DATABASE}}.raw_pedidos WHERE ingest_date = '{{INGEST_DATE}}') r
CROSS JOIN (SELECT COUNT(*) AS total FROM {{DATABASE}}.silver_fato_vendas WHERE ingest_date = '{{INGEST_DATE}}') s
CROSS JOIN (SELECT COUNT(*) AS total FROM {{DATABASE}}.quarantine_pedidos_rejeitados WHERE "data" = '{{INGEST_DATE}}') q;

-- Q3: Rejeicoes por motivo
SELECT motivo_rejeicao, COUNT(*) AS qtd
FROM {{DATABASE}}.quarantine_pedidos_rejeitados
WHERE "data" = '{{INGEST_DATE}}'
GROUP BY motivo_rejeicao
ORDER BY qtd DESC;

-- Q4: Amostra da quarentena
SELECT pedido_id, cliente_id, product_id, quantidade, motivo_rejeicao
FROM {{DATABASE}}.quarantine_pedidos_rejeitados
WHERE "data" = '{{INGEST_DATE}}'
ORDER BY pedido_id
LIMIT 10;

-- Q5: Validacao da Silver - nenhuma quantidade invalida e valor_total = quantidade * preco
SELECT COUNT(*) AS linhas_silver,
       SUM(CASE WHEN quantidade <= 0 THEN 1 ELSE 0 END) AS linhas_quantidade_invalida,
       SUM(CASE WHEN ABS(valor_total - quantidade * preco) > 0.01 THEN 1 ELSE 0 END) AS linhas_valor_total_divergente,
       ROUND(SUM(valor_total), 2) AS receita_total
FROM {{DATABASE}}.silver_fato_vendas
WHERE ingest_date = '{{INGEST_DATE}}';

-- Q6: Integridade referencial da Silver - deve retornar 0 (nenhum pedido sem cliente ou produto)
SELECT COUNT(*) AS silver_sem_cliente_ou_produto
FROM {{DATABASE}}.silver_fato_vendas s
LEFT JOIN {{DATABASE}}.raw_clientes c
       ON s.cliente_id = c.cliente_id AND c.ingest_date = '{{INGEST_DATE}}'
LEFT JOIN {{DATABASE}}.raw_produtos p
       ON s.product_id = p.product_id AND p.ingest_date = '{{INGEST_DATE}}'
WHERE s.ingest_date = '{{INGEST_DATE}}'
  AND (c.cliente_id IS NULL OR p.product_id IS NULL);

-- Q7: Consistencia Silver x Gold - a receita deve ser igual nas duas camadas
SELECT s.receita AS receita_silver,
       g.receita AS receita_gold,
       ROUND(s.receita - g.receita, 2) AS diferenca
FROM (SELECT ROUND(SUM(valor_total), 2) AS receita
      FROM {{DATABASE}}.silver_fato_vendas WHERE ingest_date = '{{INGEST_DATE}}') s
CROSS JOIN (SELECT ROUND(SUM(receita_total), 2) AS receita
            FROM {{DATABASE}}.gold_vendas_uf_categoria WHERE ingest_date = '{{INGEST_DATE}}') g;

-- Q8: Gold - top 10 combinacoes uf x categoria por receita
SELECT uf, categoria, qtd_pedidos, qtd_itens, receita_total, ticket_medio
FROM {{DATABASE}}.gold_vendas_uf_categoria
WHERE ingest_date = '{{INGEST_DATE}}'
ORDER BY receita_total DESC
LIMIT 10;
