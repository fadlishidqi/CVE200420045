import pandas as pd
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def convertTime(
    pathSource: str | Path,
    pathTarget: str | Path | None = None,
    keySource: list | None = None,
    formatTime: str | None = None
) -> bool | str:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not keySource:
        logger.error("KeySource tidak boleh kosong.")
        return False

    if not formatTime:
        logger.error("FormatTime tidak boleh kosong.")
        return False

    try:
        df = None
        
        if isinstance(pathSource, str):
            cleaned_str = pathSource.strip()
            if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                logger.info("Input terdeteksi sebagai String JSON. Membaca dari memori...")
                df = pd.read_json(io.StringIO(cleaned_str))
            else:
                source_obj = Path(pathSource)
                if not source_obj.exists():
                    logger.error(f"File sumber tidak ditemukan: {source_obj}")
                    return False
                logger.info(f"Membaca file untuk di-convert format waktunya: {source_obj.name}")
                df = pd.read_json(source_obj)
                
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk di-convert format waktunya: {pathSource.name}")
            df = pd.read_json(pathSource)
            
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        missing_cols = [col for col in keySource if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False
        
        for col in keySource:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime(formatTime)
            
        logger.info(f"Berhasil mengubah format waktu pada kolom {keySource} menjadi '{formatTime}'")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil convert waktu disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil convert sebagai String JSON.")
            return df.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses convert waktu JSON: {e}")
        return False