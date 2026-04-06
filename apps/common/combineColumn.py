import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def combineColumn(
    pathSource: str | Path,
    pathTarget: str | Path,
    colsToCombine: list,
    targetCol: str,
    separator: str = "_"
) -> bool:
    
    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not colsToCombine:
        logger.error("Kolom yang akan digabungkan (colsToCombine) tidak boleh kosong.")
        return False

    if not targetCol:
        logger.error("Target kolom (targetCol) tidak boleh kosong.")
        return False

    source_obj = Path(pathSource)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk digabungkan kolomnya: {source_obj.name}")
        df = pd.read_json(source_obj)

        missing_cols = [col for col in colsToCombine if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False
        
        df[targetCol] = df[colsToCombine].astype(str).agg(separator.join, axis=1)
        
        logger.info(f"Berhasil menggabungkan {colsToCombine} menjadi kolom '{targetCol}' dengan separator '{separator}'")

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil penggabungan disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses penggabungan JSON: {e}")
        return False