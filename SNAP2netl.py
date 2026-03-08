import gzip
import sys
import os
import subprocess
import tempfile
import shutil

# =================配置區 (針對 128GB RAM 優化)=================
# 分配 80GB 給外部排序緩衝區，確保絕大部分排序能在記憶體內完成
SORT_BUFFER_SIZE = "80G"  
# 假設你有充足的核心，這裡設定為 12 核心加速平行排序 (可依實際狀況調整)
CPU_CORES = "12"           

# 暫存目錄設定
# 注意：com-friendster 解壓與對稱化後檔案極大。
# 加上 80GB 的排序緩衝，若全放 RAM Disk (/dev/shm) 會導致 128GB RAM OOM (Out Of Memory)。
# 建議保持在當前目錄 (dir=".")，並確保該目錄位於高速 SSD 上。
TEMP_DIR_BASE = "." 
# =============================================================

def external_sort(input_file, output_file, key_args):
    """呼叫 Linux sort 進行高速外部排序與去重"""
    print(f"  > [System Sort] 啟動外部排序 (緩衝區: {SORT_BUFFER_SIZE}, 核心數: {CPU_CORES})...")
    cmd = (
        f"sort {key_args} "
        f"-S {SORT_BUFFER_SIZE} "
        f"--parallel={CPU_CORES} "
        f"'{input_file}' -o '{output_file}'"
    )
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Sort command failed: {e}")
        sys.exit(1)

def run_row_net_conversion(input_gz, final_output):
    temp_dir = tempfile.mkdtemp(dir=TEMP_DIR_BASE)
    print(f"Working directory: {temp_dir}")

    try:
        # =========================================================
        # 階段 1: 串流解壓並產生對稱邊 (Symmetrization)
        # =========================================================
        step1_file = os.path.join(temp_dir, "step1_sym.txt")
        print("🚀 Step 1: 串流解壓縮並產生對稱邊 (Row-Net 模型前置作業)...")

        max_node = 0
        min_node = float('inf')

        with gzip.open(input_gz, 'rt', encoding='utf-8') as fin, \
             open(step1_file, 'w') as fout:
            for i, line in enumerate(fin):
                if line.startswith(('#', '%')): continue
                parts = line.split()
                if len(parts) < 2: continue

                try:
                    u, v = int(parts[0]), int(parts[1])
                except ValueError:
                    continue

                # 追蹤圖的絕對大小
                if u > max_node: max_node = u
                if v > max_node: max_node = v
                if u < min_node: min_node = u
                if v < min_node: min_node = v

                # 過濾原始的 Self-loops (對角線會在 Step 3 統一以結構化方式補回)
                if u == v: continue

                # 寫入雙向邊 (u->v 和 v->u)
                fout.write(f"{u} {v}\n")
                fout.write(f"{v} {u}\n")

                if i % 5000000 == 0 and i > 0:
                    print(f"  已處理 {i:>12,} 行原始資料...", end='\r')
        print(f"\n✅ Step 1 完成。")

        # 計算偏移量：如果資料集是 0-based，我們將所有 ID +1 轉為 1-based (多數分割器要求)
        offset = 1 if min_node == 0 else 0
        total_nodes = max_node + offset
        num_nets = total_nodes  # Row-Net Model 的核心: Nets 數量 = Nodes 數量

        print(f"  📊 圖形統計: Min Node={min_node}, Max Node={max_node}, Total Size={total_nodes}")

        # =========================================================
        # 階段 2: 外部排序與 Edge-level 去重 (Sort Unique)
        # =========================================================
        step2_sorted = os.path.join(temp_dir, "step2_sorted_unique.txt")
        print("🚀 Step 2: 外部排序與去重 (剔除重複的平行邊)...")
        # -k1,1n -k2,2n 確保以數值方式精準遞增排序，-u 確保完全相同的邊會被剔除
        external_sort(step1_file, step2_sorted, "-k1,1n -k2,2n -u")
        os.remove(step1_file) # 釋放磁碟空間

        # =========================================================
        # 階段 3: 生成 Row-Net 矩陣與補齊對角線/孤立點
        # =========================================================
        print("🚀 Step 3: 依據 Row-Net 模型生成超圖結構 (補齊對角線與孤立點)...")
        body_file = os.path.join(temp_dir, "body.tmp")

        final_pins_count = 0
        current_net_id = -1
        current_nodes = []

        # Helper function: 寫入單一 Net (代表矩陣的某一個 Row)
        def write_net(net_id, nodes, f_out):
            nonlocal final_pins_count
            # --- 關鍵：強制加入對角線 (Diagonal) ---
            if net_id not in nodes:
                nodes.append(net_id)
                nodes.sort() # 保持 Node ID 遞增排序
            
            # 格式: 1 node1 1 node2 1 ... (對應 Format 11: NetWeight NodeID NodeWeight)
            line_str = "1 " + " ".join(f"{n} 1" for n in nodes)
            f_out.write(line_str + "\n")
            final_pins_count += len(nodes)

        with open(step2_sorted, 'r') as fin, open(body_file, 'w') as fbody:
            expected_next_net = 1 # 用於偵測跳號的指標

            for i, line in enumerate(fin):
                parts = line.split()
                # 加上 offset，轉為 1-based 系統
                net_val = int(parts[0]) + offset
                node_val = int(parts[1]) + offset

                if net_val != current_net_id:
                    if current_net_id != -1:
                        write_net(current_net_id, current_nodes, fbody)
                        expected_next_net = current_net_id + 1

                    # --- 關鍵：填補 GAP (孤立點) ---
                    # 如果有跳號，這些空缺的 Row 仍然存在，只包含對角線自己
                    while expected_next_net < net_val:
                        write_net(expected_next_net, [], fbody)
                        expected_next_net += 1

                    current_net_id = net_val
                    current_nodes = [node_val]
                else:
                    current_nodes.append(node_val)
                    
                if i % 10000000 == 0 and i > 0:
                    print(f"  已寫入 {i:>12,} 條雙向邊...", end='\r')

            # 處理讀取結束後的最後一組
            if current_net_id != -1:
                write_net(current_net_id, current_nodes, fbody)
                expected_next_net = current_net_id + 1

            # 填補尾部直到最大 Node ID 的 GAP
            while expected_next_net <= total_nodes:
                write_net(expected_next_net, [], fbody)
                expected_next_net += 1
                
        print(f"\n✅ Step 3 完成。")
        os.remove(step2_sorted)

        # =========================================================
        # 階段 4: 合併 Header 與 Body，完成輸出
        # =========================================================
        print("🚀 Step 4: 組合最終 .netl 檔案...")
        with open(final_output, 'w') as f_final:
            # 寫入 Header
            f_final.write(f"{total_nodes} {num_nets} 11\n")
            # 寫入 Body
            with open(body_file, 'r') as f_body_in:
                shutil.copyfileobj(f_body_in, f_final)

        # 輸出統計報告
        print("\n" + "="*40)
        print("📊 Row-Net Model (Symmetrized) 最終統計報告")
        print("="*40)
        print(f"🔹 總節點數 (Nodes): {total_nodes:>15,}")
        print(f"🔹 總超邊數 (Nets) : {num_nets:>15,} (絕對等於 Nodes 數)")
        print(f"🔹 總 Pin 數 (Pins): {final_pins_count:>15,}")
        print("="*40)
        print(f"✅ 轉換大功告成！檔案已儲存至: {final_output}")

    finally:
        # 清理暫存目錄
        if os.path.exists(temp_dir):
            print(f"🧹 清理暫存檔案目錄: {temp_dir}")
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    # 輸入與輸出設定
    input_gz_file = "com-friendster.ungraph.txt.gz"
    output_net_file = "comfriendster_rownet.netl"

    if not os.path.exists(input_gz_file):
        print(f"❌ 錯誤: 找不到輸入檔案 '{input_gz_file}'。")
    else:
        run_row_net_conversion(input_gz_file, output_net_file)