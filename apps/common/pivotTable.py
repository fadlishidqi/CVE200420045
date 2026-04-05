import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def pivotTable(
    pathSource: str | Path, 
    pathTarget: str | Path, 
    indexCols: list, 
    pivotCol: str, 
    valueCol: str,
    aggFunc: str = 'first',
    timeCol: str | None = None,
    timeFormat: str = '%d-%m-%Y %H:%M:%S'
) -> bool:
    
    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not indexCols or not pivotCol or not valueCol:
        logger.error("Parameter indexCols, pivotCol, dan valueCol tidak boleh kosong.")
        return False

    source_obj = Path(pathSource)
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk proses Pivot: {source_obj.name}")
        df = pd.read_json(source_obj)

        for col in indexCols + [pivotCol, valueCol]:
            if col not in df.columns:
                logger.error(f"Kolom wajib '{col}' tidak ditemukan di dataset.")
                return False
            
        if timeCol and timeCol in df.columns:
            df[timeCol] = pd.to_datetime(df[timeCol], format=timeFormat, errors='coerce')

        df_pivoted = df.pivot_table(
            index=indexCols, 
            columns=pivotCol, 
            values=valueCol, 
            aggfunc=aggFunc  # type: ignore
        )

        df_pivoted = df_pivoted.reset_index()
        df_pivoted.columns.name = None

        if timeCol and timeCol in df_pivoted.columns:
            df_pivoted = df_pivoted.sort_values(by=timeCol, ascending=True)

        if timeCol and timeCol in df_pivoted.columns:
            df_pivoted[timeCol] = df_pivoted[timeCol].dt.strftime(timeFormat)

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df_pivoted.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil pivot disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses pivot tabel: {e}")
        return False