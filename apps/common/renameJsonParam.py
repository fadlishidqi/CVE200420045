import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def renameJsonParam(path_source: str | Path, path_target: str | Path, key_source: list, key_target: list) -> bool:
    if not path_source or not path_target:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not key_source or not key_target:
        logger.error("KeySource atau KeyTarget tidak boleh kosong.")
        return False

    if len(key_source) != len(key_target):
        logger.error(f"Gagal: Jumlah KeySource ({len(key_source)}) dan KeyTarget ({len(key_target)}) tidak sama!")
        return False

    source_obj = Path(path_source)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk di-rename: {source_obj.name}")
        df = pd.read_json(source_obj)

        missing_cols = [col for col in key_source if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False
        
        rename_mapping = dict(zip(key_source, key_target))
        
        df = df.rename(columns=rename_mapping)
        logger.info(f"Berhasil mengubah nama kolom: {rename_mapping}")

        target_obj = Path(path_target)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil rename disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses rename JSON: {e}")
        return False