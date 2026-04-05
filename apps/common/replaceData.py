import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def replaceData(
    pathSource: str | Path,
    pathTarget: str | Path,
    keySource: list,
    fromData: list,
    toData: list
) -> bool:
    
    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not keySource:
        logger.error("KeySource tidak boleh kosong.")
        return False

    if not fromData or not toData:
        logger.error("Parameter 'fromData' atau 'toData' tidak boleh kosong.")
        return False

    if len(fromData) != len(toData):
        logger.error(f"Gagal: Jumlah data from ({len(fromData)}) dan to ({len(toData)}) tidak sama!")
        return False

    source_obj = Path(pathSource)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk proses replace data: {source_obj.name}")
        df = pd.read_json(source_obj)

        missing_cols = [col for col in keySource if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False
        
        replace_mapping = dict(zip(fromData, toData))
        
        for col in keySource:
            df[col] = df[col].replace(replace_mapping)
            
        logger.info(f"Berhasil me-replace value pada kolom {keySource} dengan mapping: {replace_mapping}")

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil replace data disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses replace data JSON: {e}")
        return False