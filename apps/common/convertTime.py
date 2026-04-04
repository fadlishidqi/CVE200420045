import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def convertTime(pathSource: str | Path, pathTarget: str | Path, keySource: list, formatTime: str) -> bool:
    
    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not keySource:
        logger.error("KeySource tidak boleh kosong.")
        return False

    if not formatTime:
        logger.error("FormatTime tidak boleh kosong.")
        return False

    source_obj = Path(pathSource)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk di-convert format waktunya: {source_obj.name}")
        df = pd.read_json(source_obj)

        missing_cols = [col for col in keySource if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False
        
        for col in keySource:

            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime(formatTime)
            
        logger.info(f"Berhasil mengubah format waktu pada kolom {keySource} menjadi '{formatTime}'")

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil convert waktu disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses convert waktu JSON: {e}")
        return False