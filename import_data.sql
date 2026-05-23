-- ============================================================
-- TPC-H 数据重导与验证 SQL 脚本 (MySQL)
-- 执行前请确保：SET GLOBAL local_infile = ON;
-- 客户端需启用 --local-infile
-- ============================================================

-- 1. 禁用外键检查，避免删除和导入时的约束冲突
SET FOREIGN_KEY_CHECKS = 0;

-- 2. 清空所有表（按外键依赖逆序 TRUNCATE，更快且重置自增）
TRUNCATE TABLE lineitem;
TRUNCATE TABLE orders;
TRUNCATE TABLE customer;
TRUNCATE TABLE partsupp;
TRUNCATE TABLE part;
TRUNCATE TABLE supplier;
TRUNCATE TABLE nation;
TRUNCATE TABLE region;

-- 3. 导入数据（按依赖顺序，避免外键暂时缺失）

LOAD DATA LOCAL INFILE 'tpch_data/region.tbl'
INTO TABLE region
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(r_regionkey, r_name, r_comment);

LOAD DATA LOCAL INFILE 'tpch_data/nation.tbl'
INTO TABLE nation
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(n_nationkey, n_name, n_regionkey, n_comment);

LOAD DATA LOCAL INFILE 'tpch_data/supplier.tbl'
INTO TABLE supplier
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment);

LOAD DATA LOCAL INFILE 'tpch_data/part.tbl'
INTO TABLE part
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment);

LOAD DATA LOCAL INFILE 'tpch_data/partsupp.tbl'
INTO TABLE partsupp
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment);

LOAD DATA LOCAL INFILE 'tpch_data/customer.tbl'
INTO TABLE customer
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment);

-- orders 包含日期列 o_orderdate
LOAD DATA LOCAL INFILE 'tpch_data/orders.tbl'
INTO TABLE orders
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(o_orderkey, o_custkey, o_orderstatus, o_totalprice, @o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
SET o_orderdate = STR_TO_DATE(@o_orderdate, '%Y-%m-%d');

-- lineitem 包含三个日期列
LOAD DATA LOCAL INFILE 'tpch_data/lineitem.tbl'
INTO TABLE lineitem
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
 @l_shipdate, @l_commitdate, @l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
SET l_shipdate    = STR_TO_DATE(@l_shipdate,    '%Y-%m-%d'),
    l_commitdate  = STR_TO_DATE(@l_commitdate,  '%Y-%m-%d'),
    l_receiptdate = STR_TO_DATE(@l_receiptdate, '%Y-%m-%d');

-- 4. 重新启用外键检查
SET FOREIGN_KEY_CHECKS = 1;

-- 5. 验证各表行数（与预期值对比）
SELECT 'region'   AS table_name, COUNT(*) AS row_count FROM region
UNION ALL
SELECT 'nation',   COUNT(*) FROM nation
UNION ALL
SELECT 'supplier', COUNT(*) FROM supplier
UNION ALL
SELECT 'part',     COUNT(*) FROM part
UNION ALL
SELECT 'partsupp', COUNT(*) FROM partsupp
UNION ALL
SELECT 'customer', COUNT(*) FROM customer
UNION ALL
SELECT 'orders',   COUNT(*) FROM orders
UNION ALL
SELECT 'lineitem', COUNT(*) FROM lineitem;