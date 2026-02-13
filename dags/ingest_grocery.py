import os
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
from pathlib import Path

@dag(
    dag_id='bulk_chunk_upload_to_bronze', 
    start_date=datetime(2026, 1, 1), 
    schedule=None,
    catchup=False
)
def chunk_dag():

    @task
    def process_and_upload():
        hook = WasbHook(wasb_conn_id='azure_blob_bronze')
        
        # 1. Lấy đường dẫn chuẩn đến thư mục dataset bên trong container
        # Dùng Path(__file__) để lấy vị trí file DAG hiện tại, sau đó đi ngược lên
        base_path = Path(__file__).parent.parent 
        input_dir = base_path / 'include' / 'dataset'
        temp_dir = base_path / 'include' / 'temp'
        
        # Tạo thư mục temp nếu chưa có để chứa file tạm
        os.makedirs(temp_dir, exist_ok=True)

        # 2. Kiểm tra nếu thư mục dataset tồn tại
        if not input_dir.exists():
            raise FileNotFoundError(f"Không tìm thấy thư mục tại: {input_dir}")

        # 3. Quét tất cả file .csv trong thư mục dataset (sales.csv, products.csv...)
        csv_files = list(input_dir.glob('*.csv'))
        print(f"Tìm thấy {len(csv_files)} file: {[f.name for f in csv_files]}")

        for file_path in csv_files:
            file_name = file_path.name
            local_temp = temp_dir / f"temp_{file_name}"
            
            print(f"--- Đang xử lý file: {file_name} ---")
            
            # Xóa file tạm cũ nếu có
            if local_temp.exists():
                os.remove(local_temp)

            # Đọc theo chunk 500 dòng để tiết kiệm RAM laptop
            try:
                for chunk in pd.read_csv(file_path, chunksize=500):
                    # Ghi nối (append) vào file tạm
                    chunk.to_csv(
                        local_temp, 
                        mode='a', 
                        index=False, 
                        header=not os.path.exists(local_temp)
                    )
                
                # 4. Upload file đã gộp lên Azure Bronze
                blob_name = f"raw/grocery/{file_name}"
                print(f"Đang upload lên Azure: {blob_name}")
                
                hook.load_file(
                    file_obj=str(local_temp),
                    container_name='bronze',
                    blob_name=blob_name,
                    overwrite=True
                )
                
                # Dọn dẹp file tạm sau khi upload xong file đó
                os.remove(local_temp)
                
            except Exception as e:
                print(f"Lỗi khi xử lý file {file_name}: {e}")

        print("Hoàn thành upload toàn bộ dataset!")

    process_and_upload()

chunk_dag()