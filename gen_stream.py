import os
import random


def generate_cquirrel_stream(tbl_dir, output_csv, window_size=100000, shuffle=False, seed=42):
    """
    生成适用于 Cquirrel 全表测试的增量流数据 (input_data_all.csv)

    :param tbl_dir: TPC-H .tbl 文件所在目录
    :param output_csv: 输出 CSV 路径
    :param window_size: 滑动窗口大小 (控制 λ 值。默认 100000 对应标准 FIFO 场景 λ=1)
    :param shuffle: 是否打乱插入顺序 (设为 True 可模拟 λ>1 的 adversarial 更新序列)
    :param seed: 随机种子 (shuffle=True 时生效)
    """
    table_config = [
        ('region.tbl', 'RE'),  # 根节点
        ('nation.tbl', 'NA'),  # 依赖 region
        ('part.tbl', 'PA'),  # 无依赖
        ('customer.tbl', 'CU'),  # 依赖 nation
        ('supplier.tbl', 'SU'),  # 依赖 nation
        ('partsupp.tbl', 'PS'),  # 依赖 part, supplier
        ('orders.tbl', 'OR'),  # 依赖 customer
        ('lineitem.tbl', 'LI')  # 依赖 orders, partsupp, supplier, part
    ]

    all_records = []
    print("开始加载 .tbl 文件...")
    for tbl_name, prefix in table_config:
        tbl_path = os.path.join(tbl_dir, tbl_name)
        if not os.path.exists(tbl_path):
            print(f"警告: 未找到 {tbl_path}，已跳过。")
            continue

        count = 0
        with open(tbl_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    # 移除 TPC-H dbgen 生成的行末多余 '|'，使 Java split("\\|", -1) 更干净
                    if line.endswith('|'):
                        line = line[:-1]
                    all_records.append((prefix, line))
                    count += 1
        print(f"已加载 {count:>8} 条记录: {tbl_name}")

    if not all_records:
        print("未加载到任何数据，请检查 .tbl 文件路径。")
        return

    # 可选：打乱顺序用于测试非 FIFO 场景 (λ > 1)
    if shuffle:
        print(f"正在打乱记录顺序 (seed={seed})...")
        random.seed(seed)
        random.shuffle(all_records)

    total = len(all_records)
    mode = "FIFO (λ=1)" if not shuffle else f"Randomized (λ>1, seed={seed})"
    print(f"\n 总记录数: {total:,} | 窗口大小: {window_size:,} | 模式: {mode}")
    print(f"开始生成流数据...")

    with open(output_csv, 'w', encoding='utf-8') as out:
        for i, (prefix, payload) in enumerate(all_records):
            # 1. 输出插入事件 (+)
            out.write(f"+{prefix}{payload}\n")
            # 2. 滑动窗口触发删除事件 (-)
            if i >= window_size:
                old_prefix, old_payload = all_records[i - window_size]
                out.write(f"-{old_prefix}{old_payload}\n")
            # 进度监控
            if (i + 1) % 500000 == 0:
                print(f"    已生成 {i + 1:,}/{total:,} 行...")
        # 3. 流结束时，清空窗口内所有剩余活跃记录 (Flush)
        flush_start = max(0, total - window_size)
        for i in range(flush_start, total):
            old_prefix, old_payload = all_records[i]
            out.write(f"-{old_prefix}{old_payload}\n")
    print(f"\n流数据生成完毕！")
    print(f"输出路径: {output_csv}")


if __name__ == "__main__":
    # ================= 配置区 =================
    TBL_DIR = "./tpch_data"  # 替换为你的 .tbl 文件目录
    OUTPUT_CSV = "input_data_all.csv"  # 输出文件名
    WINDOW_SIZE = 5000000  # 滑动窗口大小 (建议设为总数据量的 20%~60%)
    SHUFFLE = False  # 是否打乱插入顺序 (测试 λ>1 时设为 True)
    # ==========================================

    generate_cquirrel_stream(TBL_DIR, OUTPUT_CSV, WINDOW_SIZE, SHUFFLE)