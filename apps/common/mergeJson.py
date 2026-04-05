import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def mergeJson(
    pathSource: list | None = None,
    pathTarget: str | None = None,
    keySource: list | None = None
) -> bool:

    if not pathSource:
        logger.error("PathSource kosong. Harap berikan list file yang ingin digabungkan.")
        return False

    if not pathTarget:
        logger.error("PathTarget kosong. Harap berikan lokasi penyimpanan file hasil.")
        return False

    logger.info(f"Memulai proses merge untuk {len(pathSource)} file JSON...")
    all_dataframes = []

    for file_path in pathSource:
        path_obj = Path(file_path)
        
        if not path_obj.exists():
            logger.warning(f"File dilewati karena tidak ditemukan: {path_obj}")
            continue
            
        try:
            df = pd.read_json(path_obj)
            
            if keySource:
                available_cols = [col for col in keySource if col in df.columns]
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