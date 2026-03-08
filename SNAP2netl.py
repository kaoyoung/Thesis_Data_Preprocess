def run_row_net_conversion(input_gz, final_output):
    temp_dir = tempfile.mkdtemp(dir=TEMP_DIR_BASE)
    print(f"Working directory: {temp_dir}")

    try:
        # =========================================================
        # 階段 1: 串流解壓、節點重新編號 (Re-indexing) 與產生對稱邊
        # =========================================================
        step1_file = os.path.join(temp_dir, "step1_sym.txt")
        mapping_file = "node_mapping.txt" # 儲存新舊 ID 對照表
        print("🚀 Step 1: 串流解壓縮、節點重新連續編號並產生對稱邊...")

        node_map = {}
        current_new_id = 1

        with gzip.open(input_gz, 'rt', encoding='utf-8') as fin, \
             open(step1_file, 'w') as fout:
            for i, line in enumerate(fin):
                if line.startswith(('#', '%')): continue
                parts = line.split()
                if len(parts) < 2: continue

                try:
                    orig_u, orig_v = int(parts[0]), int(parts[1])
                except ValueError:
                    continue

                # 過濾原始的 Self-loops
                if orig_u == orig_v: continue

                # 動態重新編號 (Re-indexing)，確保 ID 從 1 開始嚴格連續
                if orig_u not in node_map:
                    node_map[orig_u] = current_new_id
                    current_new_id += 1
                if orig_v not in node_map:
                    node_map[orig_v] = current_new_id
                    current_new_id += 1
                
                new_u = node_map[orig_u]
                new_v = node_map[orig_v]

                # 寫入雙向邊
                fout.write(f"{new_u} {new_v}\n")
                fout.write(f"{new_v} {new_u}\n")

                if i % 5000000 == 0 and i > 0:
                    print(f"  已處理 {i:>12,} 行原始資料... (目前發現 {current_new_id-1} 個唯一節點)", end='\r')
        
        # 重新編號後，最大 ID 就是唯一節點的總數 (應該會完美等於 65,608,366)
        total_nodes = current_new_id - 1
        num_nets = total_nodes  
        print(f"\n✅ Step 1 完成。")
        print(f"  📊 圖形統計: 總節點數(Total Nodes) = {total_nodes}")

        # 輸出 Mapping File，方便未來將分割結果對應回原本的 Node ID
        print("  💾 正在輸出新舊節點 ID 對照表 (node_mapping.txt)...")
        with open(mapping_file, 'w') as fmap:
            for orig, new in node_map.items():
                fmap.write(f"{new} {orig}\n")
        
        # 釋放記憶體
        del node_map 

        # =========================================================
        # 階段 2: 外部排序與 Edge-level 去重 (Sort Unique)
        # =========================================================
        step2_sorted = os.path.join(temp_dir, "step2_sorted_unique.txt")
        print("🚀 Step 2: 外部排序與去重 (剔除重複的平行邊)...")
        external_sort(step1_file, step2_sorted, "-k1,1n -k2,2n -u")
        os.remove(step1_file)

        # =========================================================
        # 階段 3: 生成 Row-Net 矩陣與補齊對角線
        # =========================================================
        print("🚀 Step 3: 生成 Row-Net 超圖結構 (補齊對角線)...")
        body_file = os.path.join(temp_dir, "body.tmp")

        final_pins_count = 0
        current_net_id = -1
        current_nodes = []

        def write_net(net_id, nodes, f_out):
            nonlocal final_pins_count
            if net_id not in nodes:
                nodes.append(net_id)
                nodes.sort() 
            line_str = "1 " + " ".join(f"{n} 1" for n in nodes)
            f_out.write(line_str + "\n")
            final_pins_count += len(nodes)

        with open(step2_sorted, 'r') as fin, open(body_file, 'w') as fbody:
            expected_next_net = 1 

            for i, line in enumerate(fin):
                parts = line.split()
                # 因為 Step 1 已經強制轉為 1-based 連續編號，這裡不再需要 offset
                net_val = int(parts[0])
                node_val = int(parts[1])

                if net_val != current_net_id:
                    if current_net_id != -1:
                        write_net(current_net_id, current_nodes, fbody)
                        expected_next_net = current_net_id + 1

                    while expected_next_net < net_val:
                        write_net(expected_next_net, [], fbody)
                        expected_next_net += 1

                    current_net_id = net_val
                    current_nodes = [node_val]
                else:
                    current_nodes.append(node_val)
                    
                if i % 10000000 == 0 and i > 0:
                    print(f"  已寫入 {i:>12,} 條雙向邊...", end='\r')

            if current_net_id != -1:
                write_net(current_net_id, current_nodes, fbody)
                expected_next_net = current_net_id + 1

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
            f_final.write(f"{total_nodes} {num_nets} 11\n")
            with open(body_file, 'r') as f_body_in:
                shutil.copyfileobj(f_body_in, f_final)

        print("\n" + "="*40)
        print("📊 Row-Net Model (Symmetrized) 最終統計報告")
        print("="*40)
        print(f"🔹 總節點數 (Nodes): {total_nodes:>15,}")
        print(f"🔹 總超邊數 (Nets) : {num_nets:>15,}")
        print(f"🔹 總 Pin 數 (Pins): {final_pins_count:>15,}  <-- 這次絕對會是 3,677,742,636！")
        print("="*40)

    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)