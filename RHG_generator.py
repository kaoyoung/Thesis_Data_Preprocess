import networkit as nk
import time
import os
import gc

def generate_and_convert(graph_name, num_nodes, avg_degree, gamma):
    """
    核心生成與轉換函數
    """
    print(f"\n{'='*55}")
    print(f"🎬 開始處理任務: {graph_name}")
    print(f"   參數設定: 節點數={num_nodes:,}, 平均分支度={avg_degree}, Gamma={gamma}")
    print(f"{'='*55}")

    original_graph_file = f"{graph_name}_original_graph.txt"
    hypergraph_file = f"{graph_name}_hypergraph.net"

    # ==========================================
    # 階段 1：生成原始雙曲圖並獨立存檔
    # ==========================================
    print(f"🚀 [階段 1] 生成隨機雙曲圖 ({graph_name})...")
    start_time = time.time()
    
    generator = nk.generators.HyperbolicGenerator(num_nodes, k=avg_degree, gamma=gamma)
    G = generator.generate()
    
    gen_time = time.time() - start_time
    print(f"   ✅ 圖形生成完畢！耗時: {gen_time:.2f} 秒")
    print(f"   📊 原始圖形統計: {G.numberOfNodes():,} 個節點, {G.numberOfEdges():,} 條邊\n")

    print(f"💾 開始將「原始圖」寫入檔案: {original_graph_file}")
    start_write_g = time.time()
    
    writer = nk.graphio.EdgeListWriter(separator=' ', firstNode=1)
    writer.write(G, original_graph_file)
    
    print(f"   ✅ 原始圖寫入完成！耗時: {time.time() - start_write_g:.2f} 秒")
    print(f"   📂 檔案位置: {os.path.abspath(original_graph_file)}\n")

    # ==========================================
    # 階段 2：套用 Row-Net 模型轉換並存成超圖
    # ==========================================
    print(f"🚀 [階段 2] 進行 Row-Net 模型轉換 (補齊對角線 + Format 11)...")
    print(f"💾 開始將「超圖」寫入檔案: {hypergraph_file}")
    
    start_write_h = time.time()
    
    total_nets = G.numberOfNodes()
    total_nodes = G.numberOfNodes()
    final_pins_count = 0
    
    with open(hypergraph_file, 'w') as f:
        f.write(f"{total_nodes} {total_nets} 11\n")
        
        for u in G.iterNodes():
            neighbors = [v + 1 for v in G.iterNeighbors(u)]
            net_id = u + 1
            
            # --- 強制加入對角線 (Diagonal) ---
            if net_id not in neighbors:
                neighbors.append(net_id)
            
            # 排序 Node ID
            neighbors.sort()
            
            # 寫入 Format 11 格式
            line_str = "1 " + " ".join(f"{n} 1" for n in neighbors)
            f.write(line_str + "\n")
            
            final_pins_count += len(neighbors)
            
            # 進度回報 (每處理 1000 萬個節點印出一次)
            if net_id % 10_000_000 == 0:
                print(f"   已轉換並寫入 {net_id:>10,} 條超邊... (階段耗時: {time.time() - start_write_h:.2f} 秒)")

    print(f"\n   ✅ 超圖轉換寫入完成！耗時: {time.time() - start_write_h:.2f} 秒")
    print(f"   📊 Row-Net 超圖統計: {total_nodes:,} Nodes, {total_nets:,} Nets, {final_pins_count:,} Pins")
    print(f"   📂 檔案位置: {os.path.abspath(hypergraph_file)}\n")
    
    # ==========================================
    # 資源釋放 (極度重要！)
    # ==========================================
    print(f"🧹 清理 {graph_name} 的記憶體空間...")
    del G          # 刪除圖形物件
    gc.collect()   # 強制 Python 回收記憶體
    
    print(f"🎉 {graph_name} 任務圓滿結束！總執行時間: {time.time() - start_time:.2f} 秒\n")


if __name__ == "__main__":
    num_threads = nk.getMaxNumberOfThreads()
    nk.setNumberOfThreads(num_threads)
    print(f"⚡ 偵測到 {num_threads} 個執行緒，準備火力全開！\n")

    # =================配置區=================
    # 在這裡定義你要連續生成的圖形任務
    tasks = [
        {
            "graph_name": "RHG1",
            "num_nodes": 100_000_000,
            "avg_degree": 20,          # 產生約 10 億條邊
            "gamma": 3.0
        },
        {
            "graph_name": "RHG2",
            "num_nodes": 100_000_000,
            "avg_degree": 40,          # 產生約 5 億條邊 (若論文圖表數字不同，請在此修改)
            "gamma": 3.0
        }
    ]
    # =======================================

    # 依序執行任務清單
    for task_config in tasks:
        generate_and_convert(**task_config)
        
    print("🏆 恭喜！所有圖形生成任務皆已順利完成！")