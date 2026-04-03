import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def mergeJson(path_source: list | None = None, path_target: str | None = None, target_columns: list | None = None) -> bool:

    if not path_source:
        logger.error("PathSource kosong. Harap berikan list file yang ingin digabungkan.")
        return False
        
    if not path_target:
        logger.error("PathTarget kosong. Harap berikan lokasi penyimpanan file hasil.")
        return False

    logger.info(f"Memulai proses merge untuk {len(path_source)} file JSON...")
    all_dataframes = []

    for file_path in path_source:
        path_obj = Path(file_path)
        
        if not path_obj.exists():
            logger.warning(f"File dilewati karena tidak ditemukan: {path_obj}")
            continue
            
        try:
            df = pd.read_json(path_obj)
            
            if target_columns:
                available_cols = [col for col in target_columns if col in df.columns]
                df = df[available_cols]
                
            all_dataframes.append(df)
            logger.info(f"Berhasil membaca: {path_obj.name}")
        except Exception as e:
            logger.error(f"Gagal memproses file {path_obj.name}: {e}")

    if not all_dataframes:
        logger.error("Tidak ada data JSON dari PathSource.")
        return False

    try:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates()
        final_count = len(combined_df)
        
        if initial_count != final_count:
            logger.info(f"Menghapus {initial_count - final_count} baris data duplikat.")
            
    except Exception as e:
        logger.error(f"Gagal menggabungkan Json: {e}")
        return False

    target_obj = Path(path_target)
    
    target_obj.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        combined_df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! Data merge berhasil disimpan di: {target_obj}")
        return True
    except Exception as e:
        logger.error(f"Gagal menyimpan file ke PathTarget ({target_obj}): {e}")
        return False