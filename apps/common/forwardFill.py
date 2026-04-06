import pandas as pd
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def forwardFill(
    pathSource: str | Path, 
    pathTarget: str | Path | None = None, 
    groupCols: list | None = None, 
    timeCol: str | None = None,
    formatTime: str = '%d-%m-%Y %H:%M:%S'
) -> bool | str:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False
        
    if not groupCols or not timeCol:
        logger.error("groupCols dan timeCol tidak boleh kosong.")
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
                logger.info(f"Membaca file untuk proses Forward Fill: {source_obj.name}")
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk proses Forward Fill: {pathSource.name}")
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        df[timeCol] = pd.to_datetime(df[timeCol], errors='coerce', dayfirst=True)
        
        df = df.sort_values(by=groupCols + [timeCol])

        exclude_cols = set(groupCols)
        exclude_cols.add(timeCol)
        cols_to_fill = [c for c in df.columns if c not in exclude_cols]

        if cols_to_fill:
            df[cols_to_fill] = df.groupby(groupCols)[cols_to_fill].ffill()

        df[timeCol] = df[timeCol].dt.strftime(formatTime)

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil Forward Fill disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil Forward Fill sebagai String JSON.")
            return df.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses forward fill: {e}")
        return False