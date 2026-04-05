import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def forwardFill(
    pathSource: str | Path, 
    pathTarget: str | Path, 
    groupCols: list, 
    timeCol: str,
    formatTime: str = '%d-%m-%Y %H:%M:%S'
) -> bool:
    
    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    source_obj = Path(pathSource)
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk proses Forward Fill: {source_obj.name}")
        df = pd.read_json(source_obj)

        df[timeCol] = pd.to_datetime(df[timeCol], errors='coerce', dayfirst=True)
        
        df = df.sort_values(by=groupCols + [timeCol])

        exclude_cols = set(groupCols)
        exclude_cols.add(timeCol)
        cols_to_fill = [c for c in df.columns if c not in exclude_cols]

        if cols_to_fill:
            df[cols_to_fill] = df.groupby(groupCols)[cols_to_fill].ffill()

        df[timeCol] = df[timeCol].dt.strftime(formatTime)

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil Forward Fill disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses forward fill: {e}")
        return False