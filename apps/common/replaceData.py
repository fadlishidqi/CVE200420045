import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def replaceData(path_source: str | Path, path_target: str | Path, key_source: list, from_val: list, to_val: list) -> bool:
    
    if not path_source or not path_target:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not key_source:
        logger.error("KeySource tidak boleh kosong.")
        return False

    if not from_val or not to_val:
        logger.error("Parameter 'from_val' atau 'to_val' tidak boleh kosong.")
        return False

    if len(from_val) != len(to_val):
        logger.error(f"Gagal: Jumlah data from ({len(from_val)}) dan to ({len(to_val)}) tidak sama!")
        return False

    source_obj = Path(path_source)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk proses replace data: {source_obj.name}")
        df = pd.read_json(source_obj)

        missing_cols = [col for col in key_source if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False
        
        replace_mapping = dict(zip(from_val, to_val))
        
        for col in key_source:
            df[col] = df[col].replace(replace_mapping)
            
        logger.info(f"Berhasil me-replace value pada kolom {key_source} dengan mapping: {replace_mapping}")

        target_obj = Path(path_target)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil replace data disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses replace data JSON: {e}")
        return False