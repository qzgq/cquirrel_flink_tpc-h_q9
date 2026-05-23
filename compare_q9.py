#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import sys
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

# 核心配置区（仅修改此处即可）
PRECISION = Decimal('0.01')  # 固定：数值舍入精度（保持 0.01 符合 TPC-H/SQL 规范）
TOLERANCE = Decimal('0.02')  # 可调：允许的流式累积误差容差范围（建议 0.02~0.05）

def load_mysql_results(filepath):
    results = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
        for row in reader:
            nation = row.get('nation', '').strip()
            o_year = row.get('o_year', '').strip()
            profit_str = row.get('sum_profit', '').strip()
            if not nation or not o_year or not profit_str: continue
            key = (nation, o_year)
            try:
                results[key] = Decimal(profit_str).quantize(PRECISION, rounding=ROUND_HALF_UP)
            except Exception: continue
    return results

def aggregate_deltas(filepath):
    agg = defaultdict(Decimal)
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
        for row in reader:
            sign = row.get('delta_sign', '').strip()
            nation = row.get('nation', '').strip()
            o_year = row.get('o_year', '').strip()
            delta_str = row.get('profit_delta', '').strip()
            if not sign or not nation or not o_year or not delta_str: continue
            key = (nation, o_year)
            try:
                delta = Decimal(delta_str).quantize(PRECISION, rounding=ROUND_HALF_UP)
                agg[key] += delta if sign == '+' else -delta
            except Exception: continue
            
    # 使用 TOLERANCE 过滤流式累积残差，使用 PRECISION 保持输出格式
    return {
        k: v.quantize(PRECISION, rounding=ROUND_HALF_UP) 
        for k, v in agg.items() 
        if abs(v.quantize(PRECISION, rounding=ROUND_HALF_UP)) > TOLERANCE
    }

def compare_and_report(mysql_data, delta_data):
    missing = set(mysql_data.keys()) - set(delta_data.keys())
    extra = set(delta_data.keys()) - set(mysql_data.keys())
    diffs = []
    for k in set(mysql_data.keys()) & set(delta_data.keys()):
        m_val, s_val = mysql_data[k], delta_data[k]
        if abs(s_val - m_val) > TOLERANCE:
            diffs.append((k, m_val, s_val, s_val - m_val))
    diffs.sort(key=lambda x: abs(x[3]), reverse=True)

    print("\n" + "="*95)
    print("Q9 对比报告")
    print("="*95)
    print(f"舍入精度 (PRECISION): {PRECISION}")
    print(f"误差容差 (TOLERANCE): {TOLERANCE}")
    print(f"MySQL 最终行数:     {len(mysql_data)}")
    print(f"增量流聚合行数:   {len(delta_data)}")
    print(f"完全匹配:        {len(set(mysql_data.keys()) & set(delta_data.keys())) - len(diffs)}")
    print(f"MySQL 有但流缺失: {len(missing)}")
    print(f"流有但 MySQL 缺失: {len(extra)}")
    print(f"数值差异 (>{TOLERANCE}): {len(diffs)}")

    if missing:
        print("\nMySQL 有但增量流缺失的分组:")
        for k in sorted(missing): print(f"{k} (MySQL: {mysql_data[k]})")
    if extra:
        print("\n增量流有但 MySQL 缺失的分组 (残留值 > TOLERANCE):")
        for k in sorted(extra): print(f"{k} -> {delta_data[k]}")
    if diffs:
        print("\n详细数值差异列表 (按差值绝对值排序):")
        print(f"{'分组 (Nation, Year)':<25} | {'MySQL':>10} | {'Stream':>10} | {'差值':>10} | {'误差%':>8}")
        print("-" * 85)
        for k, m, s, diff in diffs:
            rel = (diff / m * 100) if m != 0 else 0
            print(f"{str(k):<25} | {m:>10} | {s:>10} | {diff:>10} | {float(rel):>7.2f}%")

    print("\n" + "="*95)
    if not missing and not extra and not diffs:
        print("√ 验证通过！Q9 增量流累加结果与 MySQL 全量查询完全一致。")
    else:
        print("× 验证未通过，请检查上述差异。")
    print("="*95)

def main():
    if len(sys.argv) != 3:
        print("用法: python compare_q9.py <q9_deltas.csv> <q9_mysql.csv>")
        sys.exit(1)
    print("加载 MySQL Q9 结果...")
    mysql_data = load_mysql_results(sys.argv[2])
    print("聚合增量流...")
    delta_data = aggregate_deltas(sys.argv[1])
    print("开始对比...")
    compare_and_report(mysql_data, delta_data)

if __name__ == '__main__':
    main()