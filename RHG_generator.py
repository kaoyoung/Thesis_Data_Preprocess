import networkit as nk
import time
import os

# =================配置區 (專為 128GB RAM 神機設計)=================
# 💡 建議：第一次跑可以先把 NUM_NODES 改成 1_000_000 測試系統穩定度與格式
NUM_NODES = 100_000_000   # 1 億個節點 (真實 RHG1 規模)
AVG_DEGREE = 20           # 平均分支度 (生成約 10 億條邊)
GAMMA = 3.0               # 冪律分佈指數 (模擬真實世界高度群聚特性)

# 檔案輸出路徑
ORIGINAL_GRAPH_FILE = "RHG1_original_graph.txt"   # 原始圖輸出 (Edge List 格式)
HYPERGRAPH_FILE = "RHG1_hypergraph.net"           # 超圖輸出 (hMetis Format 11)
# =================================================================

def generate_and_save_two_stages():
    # 讓 NetworKit 發揮最大效能，吃滿所有 CPU 核心
    num_threads = nk.getMaxNumberOfThreads()
    nk.setNumberOfThreads(num_threads)
    print(f"⚡ 偵測到 {num_threads} 個執行緒，準備火力全開！\n")

    # ==========================================
    # 階段 1：生成原始雙曲圖並獨立存檔
    # ==========================================
    print(f"🚀 [階段 1] 開始生成隨機雙曲圖 (Random Hyperbolic Graph)...")
    start_time = time.time()
    
    # 呼叫底層 C++ 高效能生成器
    generator = nk.generators.HyperbolicGenerator(NUM_NODES, k=AVG_DEGREE, gamma=GAMMA)
    G = generator.generate()
    
    gen_time = time.time() - start_time
    print(f"   ✅ 圖形生成完畢！耗時: {gen_time:.2f} 秒")
    print(f"   📊 原始圖形統計: {G.numberOfNodes():,} 個節點, {G.numberOfEdges():,} 條邊\n")

    print(f"💾 開始將「原始圖」寫入檔案: {ORIGINAL_GRAPH_FILE}")
    start_write_g = time.time()
    
    # 使用 NetworKit 內建的高效能寫入器 (EdgeList 格式，以空白分隔，節點 ID 從 1 開始)
    writer = nk.graphio.EdgeListWriter(separator=' ', firstNode=1)
    writer.write(G, ORIGINAL_GRAPH_FILE)
    
    print(f"   ✅ 原始圖寫入完成！耗時: {time.time() - start_write_g:.2f} 秒")
    print(f"   📂 檔案位置: {os.path.abspath(ORIGINAL_GRAPH_FILE)}\n")

    # ==========================================
    # 階段 2：套用 Row-Net 模型轉換並存成超圖 (Format 11 嚴謹標準)
    # ==========================================
    print(f"🚀 [階段 2] 開始進行 Row-Net 模型轉換 (補齊對角線 + Format 11)...")
    print(f"💾 開始將「超圖」寫入檔案: {HYPERGRAPH_FILE}")
    
    start_write_h = time.time()
    
    # 根據 Row-Net 模型，超邊數與節點數相同
    total_nets = G.numberOfNodes()
    total_nodes = G.numberOfNodes()
    final_pins_count = 0
    
    with open(HYPERGRAPH_FILE, 'w') as f:
        # 寫入超圖表頭，加上 '11' 標籤 (代表有節點權重與超邊權重)
        f.write(f"{total_nodes} {total_nets} 11\n")
        
        # 遍歷原始圖的每個節點
        for u in G.iterNodes():
            # 取得鄰居 (使用 iterNeighbors)，並將 ID 轉換為從 1 開始
            neighbors = [v + 1 for v in G.iterNeighbors(u)]
            net_id = u + 1
            
            # --- 關鍵：強制加入對角線 (Diagonal) ---
            if net_id not in neighbors:
                neighbors.append(net_id)
            
            # 排序 Node ID (確保數值嚴格遞增排序)
            neighbors.sort()
            
            # 格式化為 Format 11: 1 (Node Weight) nodeA 1 (Net Weight) nodeB 1 ...
            line_str = "1 " + " ".join(f"{n} 1" for n in neighbors)
            
            # 寫入這條超邊
            f.write(line_str + "\n")
            
            final_pins_count += len(neighbors)
            
            # 進度回報 (每處理 500 萬個節點印出一次)
            if net_id % 5_000_000 == 0:
                print(f"   已轉換並寫入 {net_id:>10,} 條超邊... (階段耗時: {time.time() - start_write_h:.2f} 秒)")

    print(f"\n   ✅ 超圖轉換寫入完成！耗時: {time.time() - start_write_h:.2f} 秒")
    print(f"   📊 Row-Net 超圖統計: {total_nodes:,} Nodes, {total_nets:,} Nets, {final_pins_count:,} Pins")
    print(f"   📂 檔案位置: {os.path.abspath(HYPERGRAPH_FILE)}\n")

if __name__ == "__main__":
    # 確保系統安裝了 networkit: pip install networkit
    generate_and_save_two_stages()