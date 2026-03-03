import sys
import os
import subprocess
from tqdm import tqdm

# --- 檔案路徑設定 ---
INPUT_FILE = "sk-2005_edges.txt"
TEMP_SYM_FILE = "temp_step1_symmetrized.txt"
SORTED_FINAL_FILE = "temp_step2_sorted_by_node.txt"
OUTPUT_FILE = "sk-2005_paper.netl"

def step1_symmetrize_and_sort():
    print(f"🚀 Phase 1: 對稱化 (Symmetrization) 並去除重複邊...")
    
    # 1. 產生雙向邊 (u->v 和 v->u)
    with open(TEMP_SYM_FILE, 'w') as out_f, open(INPUT_FILE, 'r') as in_f:
        for line in tqdm(in_f, desc="Symmetrizing", mininterval=1.0):
            parts = line.split()
            if not parts: continue
            u, v = parts[0], parts[1]
            
            # 過濾原始資料的 Self-loops (論文所述 Removed self-loops)
            if u == v: continue

            # 輸出雙向
            out_f.write(f"{u} {v}\n")
            out_f.write(f"{v} {u}\n")
    
    # 2. 外部排序並去重 (Sort Unique)
    # -u 確保去除重複邊 (Parallel Edges)
    print(f"   正在排序並去重 (Sort Unique)...")
    cmd = f"sort -n -k1,1 -k2,2 -u -S 50% {TEMP_SYM_FILE} -o {TEMP_SYM_FILE}"
    subprocess.run(cmd, shell=True, check=True)
    print(f"✅ Phase 1 完成。")

def step2_invert_and_sort_by_node():
    print(f"🚀 Phase 2: 反轉並依照 Node 排序 (準備生成 Netlist)...")
    
    # 目前 TEMP_SYM_FILE 格式是: <NetID/Source> <NodeID/Target>
    # 我們需要轉成: <NodeID> <NetID> 並且依照 NodeID 排序
    
    with open(SORTED_FINAL_FILE, 'w') as out_f, open(TEMP_SYM_FILE, 'r') as in_f:
        for line in tqdm(in_f, desc="Swapping Columns", mininterval=1.0):
            parts = line.split()
            net_id = int(parts[0])
            node_id = int(parts[1])
            
            # 寫入反轉後的順序: Node Net
            # NetID + 1 (轉為 1-based)
            out_f.write(f"{node_id} {net_id + 1}\n")
            
    print(f"   正在依照 Node ID 排序...")
    cmd = f"sort -n -k1,1 -S 50% {SORTED_FINAL_FILE} -o {SORTED_FINAL_FILE}"
    subprocess.run(cmd, shell=True, check=True)
    print(f"✅ Phase 2 完成。")

def step3_generate_with_diagonal():
    print(f"🚀 Phase 3: 生成最終 .netl (加入結構性對角線)...")
    
    current_node = -1
    current_net_list = []
    
    final_pins_count = 0
    max_node_id = 0
    
    # 快速抓取最大 ID
    print("   正在讀取最大 Node ID...")
    try:
        last_line = subprocess.check_output(['tail', '-1', SORTED_FINAL_FILE]).decode().strip()
        if last_line:
            max_node_id = int(last_line.split()[0])
    except:
        max_node_id = 0
    
    total_nodes = max_node_id + 1
    num_nets = total_nodes # 對稱矩陣，Nets數 = Nodes數

    with open(OUTPUT_FILE, 'w') as out_f:
        # Header: <NumNodes> <NumNets> 11
        out_f.write(f"{total_nodes} {num_nets} 11\n")
        
        with open(SORTED_FINAL_FILE, 'r') as in_f:
            for line in tqdm(in_f, desc="Writing .netl", mininterval=1.0):
                parts = line.split()
                node = int(parts[0])
                net = int(parts[1])
                
                if node != current_node:
                    if current_node != -1:
                        # --- 關鍵：加入對角線 (Diagonal) ---
                        # 這是為了符合 Row-Net Model 要求 (每個 Node 都在自己的 Net 中)
                        diagonal_net = current_node + 1
                        current_net_list.append(diagonal_net)
                        current_net_list.sort() # 保持順序一致性
                        
                        # 寫入
                        line_str = "1 " + " ".join(f"{n} 1" for n in current_net_list)
                        out_f.write(line_str + "\n")
                        final_pins_count += len(current_net_list)
                        
                        # 補 GAP (孤立點)
                        gap = node - current_node
                        for i in range(1, gap):
                            missing_node = current_node + i
                            # 孤立點也要有對角線
                            out_f.write(f"1 {missing_node + 1} 1\n")
                            final_pins_count += 1 

                    elif node > 0:
                        # 開頭 GAP
                        for i in range(node):
                            out_f.write(f"1 {i + 1} 1\n")
                            final_pins_count += 1
                    
                    current_node = node
                    current_net_list = []
                
                current_net_list.append(net)
            
            # 處理最後一個 Node
            if current_node != -1:
                diagonal_net = current_node + 1
                current_net_list.append(diagonal_net)
                current_net_list.sort()
                
                line_str = "1 " + " ".join(f"{n} 1" for n in current_net_list)
                out_f.write(line_str + "\n")
                final_pins_count += len(current_net_list)
            
            # 尾部 GAP
            remaining = max_node_id - current_node
            for i in range(1, remaining + 1):
                missing_node = current_node + i
                out_f.write(f"1 {missing_node + 1} 1\n")
                final_pins_count += 1

    # --- 依照您的要求輸出統計 ---
    print("\n" + "="*40)
    print("📊 UK-2005 (Standard Symmetrized) 統計報告")
    print("="*40)
    print(f"🔹 Nodes: {total_nodes:>15,} (預期: ~39.4M)")
    print(f"🔹 Nets:  {num_nets:>15,} (預期: ~39.4M)")
    print(f"🔹 Pins:  {final_pins_count:>15,} (預期: ~1.6B)")
    print("="*40)
    
    # 清理暫存檔
    if os.path.exists(TEMP_SYM_FILE): os.remove(TEMP_SYM_FILE)
    if os.path.exists(SORTED_FINAL_FILE): os.remove(SORTED_FINAL_FILE)

if __name__ == "__main__":
    step1_symmetrize_and_sort()
    step2_invert_and_sort_by_node()
    step3_generate_with_diagonal()