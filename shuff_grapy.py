import random
import time
import os

def shuffle_vertex_stream_128gb(input_file, output_file):
    print(f"開始處理 Node-centric 檔案: {input_file}")
    start_time = time.time()
    
    with open(input_file, 'r') as fin:
        # 1. 讀取並保留 Header
        header = fin.readline()
        print(f"Header 資訊: {header.strip()}")
        
        # 2. 霸氣將所有節點(行)讀入記憶體 (約需 15-20 GB RAM)
        print("正在將所有節點資料讀入記憶體...")
        lines = fin.readlines()
        
    # 3. 破壞串流局部性：直接打亂所有行的順序！
    print(f"成功讀入 {len(lines)} 個節點。開始進行全域隨機洗牌 (Global Shuffling)...")
    random.shuffle(lines)
    
    # 4. 寫出新的隨機化串流檔案
    print("開始寫入新的隨機化圖檔...")
    with open(output_file, 'w') as fout:
        # 先寫回 Header
        fout.write(header)
        
        # 使用 writelines 可以極速寫入所有記憶體中的字串
        fout.writelines(lines)
                
    end_time = time.time()
    print(f"洗牌完成！已輸出至: {output_file}")
    print(f"單一檔案耗時: {end_time - start_time:.2f} 秒\n")

def main():
    # 建立你要處理的五個檔案清單
    target_files = [
        "uk-2005_paper.netl",
        "sk-2005_paper.netl",
        "RHG2_hypergraph.netl",
        "RHG1_hypergraph.netl",
        "comfriendster_rownet.netl"
    ]

    print("🚀 開始批次洗牌作業...\n")
    total_start_time = time.time()

    for input_file in target_files:
        # 檢查檔案是否存在，避免程式崩潰
        if not os.path.exists(input_file):
            print(f"⚠️ 警告: 找不到檔案 '{input_file}'，自動跳過。")
            continue

        # 自動生成輸出檔名：把附檔名切開，中間插入 _random_stream
        # 例如: uk-2005_paper.netl -> uk-2005_paper_random_stream.netl
        base_name, ext = os.path.splitext(input_file)
        output_file = f"{base_name}_random_stream{ext}"

        # 呼叫洗牌函數
        shuffle_vertex_stream_128gb(input_file, output_file)

    total_end_time = time.time()
    print("-" * 50)
    print(f"✨ 所有任務處理完畢！總共耗時: {total_end_time - total_start_time:.2f} 秒")

if __name__ == "__main__":
    main()