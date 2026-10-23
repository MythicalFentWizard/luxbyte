-- The fidelity corpus: statements PostgreSQL accepts, against the dataset
-- `setup.py` lays out. Each is run through a proxy and directly against
-- `fid_truth`, which holds every row in one database; the proxy's answer is
-- correct, refused, or wrong. A write runs between BEGIN and ROLLBACK and is
-- judged by its command tag.
--
-- Each entry starts with a `name:` line: its name, group, and read or write.

-- name: point_read | basics | read
SELECT * FROM users WHERE tenant_id = 't06'

-- name: in_list_two_regions | basics | read
SELECT * FROM users WHERE tenant_id IN ('t02', 't07')

-- name: no_key | basics | read
SELECT * FROM users WHERE vip

-- name: count_all | basics | read
SELECT count(*) FROM orders

-- name: group_by | basics | read
SELECT currency, sum(total) FROM orders GROUP BY currency

-- name: order_limit | basics | read
SELECT id, total FROM orders ORDER BY total DESC, id LIMIT 5

-- name: distinct | basics | read
SELECT DISTINCT currency FROM orders

-- name: inheritance_star | grammar | read
SELECT * FROM users* WHERE tenant_id = 't03'

-- name: only | grammar | read
SELECT * FROM ONLY users WHERE tenant_id = 't10'

-- name: between_symmetric | grammar | read
SELECT * FROM users WHERE tenant_id = 't05' AND id BETWEEN SYMMETRIC 59 AND 51

-- name: rows_from | grammar | read
SELECT * FROM ROWS FROM (generate_series(1, 3)) AS g(x)

-- name: table_command | grammar | read
TABLE currencies

-- name: dollar_quoted_key | grammar | read
SELECT * FROM users WHERE tenant_id = $$t08$$

-- name: any_array | grammar | read
SELECT * FROM users WHERE tenant_id = ANY (ARRAY['t01', 't02'])

-- name: row_equality | grammar | read
SELECT * FROM users WHERE (tenant_id, id) = ('t04', 41)

-- name: fetch_with_ties | grammar | read
SELECT id FROM orders WHERE tenant_id = 't02' ORDER BY total FETCH FIRST 1 ROW WITH TIES

-- name: is_not_distinct_from | grammar | read
SELECT * FROM users WHERE tenant_id IS NOT DISTINCT FROM 't09'

-- name: cte_with_key | subqueries | read
WITH o AS (SELECT * FROM orders WHERE tenant_id = 't11') SELECT count(*) FROM o

-- name: cte_without_key | subqueries | read
WITH u AS (SELECT * FROM users) SELECT count(*) FROM u

-- name: exists_correlated_with_key | subqueries | read
SELECT u.id FROM users u WHERE u.tenant_id = 't05'
  AND EXISTS (SELECT 1 FROM orders o WHERE o.tenant_id = u.tenant_id AND o.total > 30)

-- name: exists_correlated_without_key | subqueries | read
SELECT u.id FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.tenant_id = u.tenant_id AND o.total > 100)

-- name: scalar_subquery_correlated | subqueries | read
SELECT u.id, (SELECT count(*) FROM orders o WHERE o.tenant_id = u.tenant_id)
FROM users u WHERE u.tenant_id = 't12'

-- name: in_subquery_with_key | subqueries | read
SELECT * FROM users WHERE tenant_id = 't01'
  AND id IN (SELECT buyer FROM orders WHERE tenant_id = 't01')

-- name: in_subquery_across_regions | subqueries | read
SELECT * FROM users WHERE id IN (SELECT buyer FROM orders WHERE total = 5)

-- name: replicated_filtered_by_partitioned | subqueries | read
SELECT code FROM currencies WHERE code IN (SELECT currency FROM orders)

-- name: not_exists_not_on_key | subqueries | read
SELECT id FROM users u WHERE tenant_id = 't05'
  AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.buyer = u.id)

-- name: lateral_with_key | subqueries | read
SELECT u.id, l.total FROM users u
CROSS JOIN LATERAL (SELECT total FROM orders o WHERE o.tenant_id = u.tenant_id
                    ORDER BY total DESC LIMIT 1) l
WHERE u.tenant_id = 't08'

-- name: derived_table | subqueries | read
SELECT count(*) FROM (SELECT * FROM orders WHERE tenant_id = 't03') s

-- name: join_on_key_with_key | joins | read
SELECT u.name, o.total FROM users u JOIN orders o ON o.tenant_id = u.tenant_id AND o.buyer = u.id
WHERE u.tenant_id = 't07'

-- name: join_on_key_without_key | joins | read
SELECT u.name, o.total FROM users u JOIN orders o ON o.tenant_id = u.tenant_id AND o.buyer = u.id

-- name: join_not_on_key | joins | read
SELECT u.name, o.id FROM orders o JOIN users u ON u.id = o.buyer WHERE o.tenant_id = 't04'

-- name: join_replicated_without_key | joins | read
SELECT o.id, c.rate FROM orders o JOIN currencies c ON c.code = o.currency

-- name: left_join_with_key | joins | read
SELECT u.id, o.id FROM users u
LEFT JOIN orders o ON o.tenant_id = u.tenant_id AND o.buyer = u.id
WHERE u.tenant_id = 't09'

-- name: self_join_on_key | joins | read
SELECT a.name, b.name FROM users a JOIN users b ON b.tenant_id = a.tenant_id AND b.id = a.manager

-- name: join_grouped_without_key | joins | read
SELECT u.vip, count(*) FROM users u
JOIN orders o ON o.tenant_id = u.tenant_id AND o.buyer = u.id GROUP BY u.vip

-- name: union_all_two_regions | sets | read
SELECT id FROM users WHERE tenant_id = 't01' UNION ALL SELECT id FROM users WHERE tenant_id = 't10'

-- name: union_two_tables | sets | read
SELECT tenant_id FROM users UNION SELECT tenant_id FROM orders

-- name: intersect_with_key | sets | read
SELECT buyer FROM orders WHERE tenant_id = 't02' INTERSECT SELECT id FROM users WHERE tenant_id = 't02'

-- name: replicated_read | replicated | read
SELECT * FROM currencies ORDER BY code

-- name: replicated_scalar | replicated | read
SELECT (SELECT rate FROM currencies WHERE code = 'EUR')

-- name: update_with_key | writes | write
UPDATE orders SET total = total + 1 WHERE tenant_id = 't06'

-- name: update_without_key | writes | write
UPDATE orders SET total = total + 1 WHERE total > 50

-- name: update_correlated_without_key | writes | write
UPDATE orders SET total = 0
WHERE EXISTS (SELECT 1 FROM users u WHERE u.tenant_id = orders.tenant_id AND u.vip)

-- name: update_replicated_from_partitioned | writes | write
UPDATE currencies SET rate = rate + 1
WHERE code IN (SELECT currency FROM orders WHERE tenant_id = 't07')

-- name: delete_with_key | writes | write
DELETE FROM orders WHERE tenant_id = 't11' AND total = 5

-- name: insert_two_regions | writes | write
INSERT INTO users (tenant_id, id, name) VALUES ('t01', 999, 'x'), ('t12', 998, 'y')

-- name: insert_select | writes | write
INSERT INTO orders (tenant_id, id, buyer, currency, total)
SELECT tenant_id, id + 1000, buyer, currency, total FROM orders WHERE tenant_id = 't02'

-- name: update_only | writes | write
UPDATE ONLY users SET vip = true WHERE tenant_id = 't05'

-- name: delete_using_with_key | writes | write
DELETE FROM orders o USING users u
WHERE u.tenant_id = o.tenant_id AND u.id = o.buyer AND o.tenant_id = 't08' AND NOT u.vip

-- name: merge_update | writes | write
MERGE INTO users u USING orders o ON u.tenant_id = o.tenant_id AND u.id = o.buyer
WHEN MATCHED AND o.tenant_id = 't03' THEN UPDATE SET vip = true

-- name: insert_overriding | writes | write
INSERT INTO users (tenant_id, id, name) OVERRIDING SYSTEM VALUE VALUES ('t06', 997, 'z')
