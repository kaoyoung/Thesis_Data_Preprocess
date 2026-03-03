import gzip
import sys
import os
import subprocess
import tempfile
import shutil
import gc

# =================配置區=================
# 你的 RAM 是 24GB，我們分配 16GB 給排序緩衝區。
# 預留 8GB 給系統和 Python 的去重 Set 使用，這樣最安全。
SORT_BUFFER_SIZE = "16G" 
CPU_CORES = "6"  # 根據你的 htop 截圖，你似乎有 8 核，留一點給系統
# =======================================

def external_sort(input_file, output_file, key_args):
    """
    呼叫 Linux sort，並強制使用大量 RAM 加速
    """
    print(f"  > [System Sort] Sorting {input_file}...")
    print(f"    (Using {SORT_BUFFER_SIZE} RAM buffer and {CPU_CORES} threads)")
    
    # -S: 設定記憶體緩衝區大小
    # --parallel: 設定執行緒數量
    cmd = (
        f"sort -n {key_args} "
        f"-S {SORT_BUFFER_SIZE} "
        f"--parallel={CPU_CORES} "
        f"'{input_file}' -o '{output_file}'"
    )
    
    try:
        subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Sort command failed: {e}")
        sys.exit(1)

def run_high_speed_conversion(input_gz, final_output):
    # 使用當前目錄作為暫存，避免 /tmp 空間不足 (有些系統 /tmp 很小)
    temp_dir = tempfile.mkdtemp(dir=".")
    print(f"Working directory: {temp_dir}")
    
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
                if i % 2000000 == 0:
                    print(f"  Extracted {i} lines...", end='\r')
        print("\nExtraction done.")

        # =========================================================
        # 階段 2: 高速排序 (Sort by NetID)
        # 這一步會吃掉設定的 16GB RAM，但速度會很快
        # =========================================================
        step2_sorted = os.path.join(temp_dir, "step2_sorted_by_net.txt")
        # 依第一欄(Net)數字排序, 若相同則依第二欄(Node)排序
        external_sort(step1_file, step2_sorted, "-k1,1 -k2,2")
        
        # 刪除舊檔釋放磁碟空間
        os.remove(step1_file)

        # =========================================================
        # 階段 3: Python 過濾 (Filter)
        # 這一步 Python 會佔用 RAM 來存 seen_signatures (去重)
        # 但因為 sort 已經結束，RAM 已經被釋放出來給 Python 用了
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
                # 這裡不轉 int，直接用字串處理比較快，除非你需要數值排序
                # 但為了安全起見，我們假設前面 sort -n 已經排好數字序了
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
                unique_nodes = sorted(list(set(current_nodes))) # Fallback
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
        # 這次我們需要按照 NodeID (第一欄) 排序
        external_sort(step3_file, step4_sorted, "-k1,1 -k2,2")
        os.remove(step3_file)

        # =========================================================
        # 階段 5: 寫入最終格式
        # =========================================================
        print("Step 5: Writing Final Output...")
        
        final_node_map = {} 
        next_node_id = 1
        
        # 使用 temp file 儲存 body，因為我們最後才知道 Total Nodes
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
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    # 請修改這裡
    input_gz_file = "com-friendster.ungraph.txt.gz"
    output_net_file = "comfriendster_hypergraph.net"
    
    if not os.path.exists(input_gz_file):
        print("Input file not found.")
    else:
        run_high_speed_conversion(input_gz_file, output_net_file)