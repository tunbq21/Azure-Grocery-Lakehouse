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
        
        base_path = Path(__file__).parent.parent 
        input_dir = base_path / 'include' / 'dataset'
        temp_dir = base_path / 'include' / 'temp'
        
        os.makedirs(temp_dir, exist_ok=True)

        if not input_dir.exists():
            raise FileNotFoundError(f"Không tìm thấy thư mục tại: {input_dir}")

        csv_files = list(input_dir.glob('*.csv'))
        print(f"Tìm thấy {len(csv_files)} file: {[f.name for f in csv_files]}")

        for file_path in csv_files:
            file_name = file_path.name
            local_temp = temp_dir / f"temp_{file_name}"
            
            print(f"--- Đang xử lý file: {file_name} ---")
            
            # Xóa file tạm cũ nếu có
            if local_temp.exists():
                os.remove(local_temp)

            try:
                for chunk in pd.read_csv(file_path, chunksize=500):
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
                
                os.remove(local_temp)
                
            except Exception as e:
                print(f"Lỗi khi xử lý file {file_name}: {e}")

        print("Hoàn thành upload toàn bộ dataset!")

    process_and_upload()

chunk_dag()