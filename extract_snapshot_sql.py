#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 input_data_all.csv 提取指定快照点的数据，同时生成：
1. MySQL INSERT/DELETE 脚本 (.sql)
2. 对应的 Flink 输入数据流 (.csv)
用法: python extract_snapshot_sql.py 0.5   (提取 50% 数据)
"""
import os
import sys

# TPC-H 表结构映射：前缀代码 -> (表名, [列名], [主键列名])
TABLE_MAP = {
    'RI': ('region', ['r_regionkey', 'r_name', 'r_comment'], ['r_regionkey']),
    'NA': ('nation', ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment'], ['n_nationkey']),
    'SU': ('supplier', ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'], ['s_suppkey']),
    'PA': ('part', ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment'], ['p_partkey']),
    'PS': ('partsupp', ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'], ['ps_partkey', 'ps_suppkey']),
    'CU': ('customer', ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'], ['c_custkey']),
    'OR': ('orders', ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'], ['o_orderkey']),
    'LI': ('lineitem', ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'], ['l_orderkey', 'l_linenumber'])
}

def sql_escape(val):
    """MySQL 字符串转义"""
    if val is None: return 'NULL'
    val = str(val).strip()
    return f"'{val.replace(chr(39), chr(39)*2)}'"  # 单引号转义

def generate_snapshot(input_file, ratio=0.5, sql_output=None, csv_output=None):
    if not os.path.exists(input_file):
        print(f"错误: 找不到文件 {input_file}")
        return

    # 计算总有效行数
    with open(input_file, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for line in f if line.strip())

    cutoff = max(1, int(total_lines * ratio))
    print(f"总有效行数: {total_lines} | 截取比例: {ratio:.0%} | 目标行数: {cutoff}")

    # 默认输出文件名
    if sql_output is None: sql_output = f'snapshot_{int(ratio*100)}pct.sql'
    if csv_output is None: csv_output = f'input_data_snapshot_{int(ratio*100)}pct.csv'

    inserts = deletes = 0

    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(sql_output, 'w', encoding='utf-8') as f_sql, \
         open(csv_output, 'w', encoding='utf-8') as f_csv:

        # 写入 SQL 文件头
        f_sql.write("-- ==================================================\n")
        f_sql.write(f"-- 自动生成的快照 SQL 脚本 (比例: {ratio:.0%}, 目标: {cutoff} 行)\n")
        f_sql.write("-- 执行前请务必备份数据库！\n")
        f_sql.write("-- ==================================================\n")
        f_sql.write("SET FOREIGN_KEY_CHECKS = 0;\n\n")

        processed = 0
        for line in f_in:
            raw_line = line.rstrip('\n').rstrip('\r')
            if not raw_line: continue
            if processed >= cutoff: break
            f_csv.write(raw_line + "\n")
            processed += 1
            if len(raw_line) < 4: continue
            prefix = raw_line[:3]
            op, code = prefix[0], prefix[1:]
            payload = raw_line[3:]

            if code not in TABLE_MAP: continue

            table, cols, pk_cols = TABLE_MAP[code]
            # 处理 TPC-H 末尾多余的 '|' 并截取正确数量的字段
            fields = [v.strip() for v in payload.split('|')[:len(cols)]]
            if len(fields) != len(cols): continue

            if op == '+':
                placeholders = ', '.join(sql_escape(v) for v in fields)
                sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders});"
                inserts += 1
            elif op == '-':
                where_clauses = [f"{pk} = {sql_escape(fields[cols.index(pk)])}" for pk in pk_cols]
                sql = f"DELETE FROM {table} WHERE {' AND '.join(where_clauses)};"
                deletes += 1
            else:
                continue

            f_sql.write(sql + "\n")

            if processed % 50000 == 0:
                print(f"已处理 {processed} 条 (CSV & SQL 同步生成中)...", end='\r')

        f_sql.write("\nSET FOREIGN_KEY_CHECKS = 1;\nCOMMIT;\n")

    print(f"\n成功生成快照文件:")
    print(f"SQL 脚本: {sql_output} (包含 {inserts} 条 INSERT, {deletes} 条 DELETE)")
    print(f"CSV 数据流: {csv_output} (共 {processed} 行，可直接作为 Flink 输入)")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python extract_snapshot_sql.py [ratio] [input_csv] [sql_output] [csv_output]")
        print("示例: python extract_snapshot_sql.py 0.5")
        sys.exit(1)

    ratio = float(sys.argv[1])
    in_csv = sys.argv[2] if len(sys.argv) > 2 else 'input_data_all.csv'
    out_sql = sys.argv[3] if len(sys.argv) > 3 else None
    out_csv = sys.argv[4] if len(sys.argv) > 4 else None

    generate_snapshot(in_csv, ratio, out_sql, out_csv)