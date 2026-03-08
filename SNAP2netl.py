import gzip
import sys
import os
import subprocess
import tempfile
import shutil
import gc

# =================配置區=================
# 針對 128GB RAM 的頂級配置
SORT_BUFFER_SIZE = "80G"   # 分配 80GB 給 Linux sort，盡量在記憶體內完成排序
CPU_CORES = "12"           # 設定使用的 CPU 核心數 (請依據你的實際規格調整)
# =======================================

def external_sort(input_file, output_file, key_args, temp_dir):
    """
    呼叫 Linux sort，並強制使用大量 RAM 加速與指定的暫存目錄
    """
    print(f"  > [System Sort] Sorting {input_file}...")
    print(f"    (Using {SORT_BUFFER_SIZE} RAM buffer and {CPU_CORES} threads)")

    # 加入 -T 參數，將 sort 的暫存目錄指向我們建立的 temp_dir，避免塞爆系統 /tmp
    cmd = (
        f"sort -n {key_args} "
        f"-S {SORT_BUFFER_SIZE} "
        f"--parallel={CPU_CORES} "
        f"-T '{temp_dir}' "
        f"'{input_file}' -o '{output_file}'"
    )

    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Sort command failed: {e}")
        sys.exit(1)

def run_high_speed_conversion(input_gz, final_output):
    # 使用當前目錄作為暫存，並確保有足夠的 SSD 空間 (建議至少預留 60GB)
    temp_dir = tempfile.mkdtemp(dir=".")
    print(f"Working directory for temp files: {temp_dir}")

    try:
        # =========================================================
        # 階段 1: 快速解壓 (Extract)
        # =========================================================
        step1_file = os.path.join(temp_dir, "step1_raw.txt")
        print("Step 1: Extracting GZ (Streaming)...")

        with gzip.open(input_gz, 'rt', encoding='utf-8') as fin, \
             open(step1_file, 'w') as fout:
            for i, line in enumerate(fin):
                if line.startswith(('#', '%')): continue
                parts = line.split()
                if len(parts) < 2: continue
                try:
                    # 直接寫入 NetID NodeID (順序很重要，為了下一步排序)
                    fout.write(f"{parts[1]} {parts[0]}\n")
                except IndexError:
                    continue
                # 每 500 萬行回報一次進度，避免洗畫面
                if i % 5000000 == 0 and i > 0:
                    print(f"  Extracted {i} lines...", end='\r')
        print("\nExtraction done.")

        # =========================================================
        # 階段 2: 高速排序 (Sort by NetID)
        # 128GB RAM 發揮威力的時刻，80G 緩衝區會讓這一步快非常多
        # =========================================================
        step2_sorted = os.path.join(temp_dir, "step2_sorted_by_net.txt")
        # 依第一欄(Net)數字排序, 若相同則依第二欄(Node)排序。傳入 temp_dir
        external_sort(step1_file, step2_sorted, "-k1,1 -k2,2", temp_dir)

        # 刪除舊檔釋放磁碟空間
        os.remove(step1_file)

        # =========================================================
        # 階段 3: Python 過濾 (Filter)
        # 移除平行邊、空邊、自環迴圈
        # =========================================================
        print("Step 3: Filtering (Removing Parallel/Empty/Loops)...")
        step3_file = os.path.join(temp_dir, "step3_valid_pairs.txt")

        seen_signatures = set()
        current_net_id = None
        current_nodes = []
        new_net_counter = 0

        with open(step2_sorted, 'r') as fin, open(step3_file, 'w') as fout:
            for line in fin:
                parts = line.split()
                net_val = parts[0]
                node_val = parts[1]

                if net_val != current_net_id:
                    # 處理上一組 Net
                    if current_net_id is not None:
                        # 去重節點 (list已有序)
                        unique_nodes = []
                        if current_nodes:
                            unique_nodes.append(current_nodes[0])
                            for x in current_nodes[1:]:
                                if x != unique_nodes[-1]:
                                    unique_nodes.append(x)

                        # 規則檢查
                        if len(unique_nodes) >= 2:
                            # 建立簽名 (Tuple of strings)
                            sig = tuple(unique_nodes)
                            if sig not in seen_signatures:
                                seen_signatures.add(sig)
                                new_net_counter += 1
                                # 寫入: NodeID NewNetID (為下一步做準備)
                                for n in unique_nodes:
                                    fout.write(f"{n} {new_net_counter}\n")

                    # 重置
                    current_net_id = net_val
                    current_nodes = [node_val]
                else:
                    current_nodes.append(node_val)

            # 處理最後一組
            if current_net_id is not None and len(current_nodes) >= 2:
                unique_nodes = sorted(list(set(current_nodes))) 
                if len(unique_nodes) >= 2:
                    sig = tuple(unique_nodes)
                    if sig not in seen_signatures:
                        new_net_counter += 1
                        for n in unique_nodes:
                            fout.write(f"{n} {new_net_counter}\n")

        # 重要：主動釋放 Python 佔用的 RAM
        del seen_signatures
        gc.collect()
        os.remove(step2_sorted)
        print(f"  Valid Nets: {new_net_counter}")

        # =========================================================
        # 階段 4: 再次高速排序 (Sort by NodeID)
        # 為了輸出格式 Node -> [Net1, Net2...]
        # =========================================================
        step4_sorted = os.path.join(temp_dir, "step4_sorted_by_node.txt")
        # 傳入 temp_dir 作為 sort 的安全暫存區
        external_sort(step3_file, step4_sorted, "-k1,1 -k2,2", temp_dir)
        os.remove(step3_file)

        # =========================================================
        # 階段 5: 寫入最終格式 (.net)
        # =========================================================
        print("Step 5: Writing Final Output...")

        final_node_map = {}
        next_node_id = 1
        body_file = os.path.join(temp_dir, "body.tmp")
        current_node_val = None
        current_net_list = []

        with open(step4_sorted, 'r') as fin, open(body_file, 'w') as fbody:
            for line in fin:
                parts = line.split()
                node_val = parts[0]
                net_val = parts[1]

                if node_val != current_node_val:
                    if current_node_val is not None:
                        # 記錄這個 Node 有效，並給予連續編號
                        final_node_map[current_node_val] = next_node_id
                        next_node_id += 1

                        fbody.write("1") # Node Weight
                        for n_id in current_net_list:
                            fbody.write(f" {n_id} 1") # NetID NetWeight
                        fbody.write("\n")

                    current_node_val = node_val
                    current_net_list = [net_val]
                else:
                    current_net_list.append(net_val)

            # 最後一行
            if current_node_val is not None:
                final_node_map[current_node_val] = next_node_id
                fbody.write("1")
                for n_id in current_net_list:
                    fbody.write(f" {n_id} 1")
                fbody.write("\n")

        total_nodes = len(final_node_map)
        del final_node_map
        gc.collect()

        print(f"Finalizing: {total_nodes} Nodes, {new_net_counter} Nets.")
        with open(final_output, 'w') as f_final:
            f_final.write(f"{total_nodes} {new_net_counter} 11\n")
            with open(body_file, 'r') as f_body_in:
                shutil.copyfileobj(f_body_in, f_final)

        print(f"Completed! Output: {final_output}")

    finally:
        # 確保程式無論成功或失敗，都會把佔用硬碟的暫存資料清乾淨
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    # 確保把 com-friendster.ungraph.txt.gz 放在和這支 Python 程式同一個資料夾下
    input_gz_file = "com-friendster.ungraph.txt.gz"
    output_net_file = "comfriendster_hypergraph.netl"

    if not os.path.exists(input_gz_file):
        print(f"錯誤：找不到檔案 '{input_gz_file}'！請確認檔案路徑是否正確。")
    else:
        run_high_speed_conversion(input_gz_file, output_net_file)