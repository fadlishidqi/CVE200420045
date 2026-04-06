import pandas as pd
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def pivotTable(
    pathSource: str | Path, 
    pathTarget: str | Path | None = None, 
    indexCols: list | None = None, 
    pivotCol: str | None = None, 
    valueCol: str | list | None = None,
    aggFunc: str = 'first',
    timeCol: str | None = None,
    timeFormat: str = '%d-%m-%Y %H:%M:%S'
) -> bool | str:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not indexCols or not pivotCol or not valueCol:
        logger.error("Parameter indexCols, pivotCol, dan valueCol tidak boleh kosong.")
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
                logger.info(f"Membaca file untuk proses Pivot: {source_obj.name}")
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk proses Pivot: {pathSource.name}")
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        val_cols_check = [valueCol] if isinstance(valueCol, str) else valueCol
        for col in indexCols + [pivotCol] + val_cols_check:
            if col not in df.columns:
                logger.error(f"Kolom wajib '{col}' tidak ditemukan di dataset.")
                return False
            
        if timeCol and timeCol in df.columns:
            df[timeCol] = pd.to_datetime(df[timeCol], format=timeFormat, errors='coerce')

        for col in indexCols:
            df[col] = df[col].fillna("N/A")
            
        df_pivoted = df.pivot_table(
            index=indexCols, 
            columns=pivotCol, 
            values=valueCol, 
            aggfunc=aggFunc # type: ignore
        )

        if isinstance(df_pivoted.columns, pd.MultiIndex):
            df_pivoted.columns = [f"{col[1]}_{col[0]}" if pd.notna(col[1]) and col[1] != '' else col[0] for col in df_pivoted.columns.values]

        df_pivoted = df_pivoted.reset_index()
        df_pivoted.columns.name = None

        if timeCol and timeCol in df_pivoted.columns:
            df_pivoted = df_pivoted.sort_values(by=timeCol, ascending=True)

        if timeCol and timeCol in df_pivoted.columns:
            df_pivoted[timeCol] = df_pivoted[timeCol].dt.strftime(timeFormat)

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df_pivoted.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil pivot disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil pivot sebagai String JSON.")
            return df_pivoted.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses pivot tabel: {e}")
        return False