import pandas as pd
from pathlib import Path
import logging
from typing import List
import io

logger = logging.getLogger(__name__)

def aggregateTime(
    pathSource: str | Path, 
    pathTarget: str | Path | None = None, 
    keyTime: str | None = None, 
    keyValues: List[str] | None = None,
    groupKeys: List[str] | None = None,
    timeFormat: str = '%d-%m-%Y %H:%M:%S' 
) -> bool | str:
    
    if groupKeys is None:
        groupKeys = []
        
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not keyTime or not keyValues:
        logger.error("KeyTime atau KeyValues tidak boleh kosong.")
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
                logger.info(f"Membaca file untuk agregasi waktu: {source_obj.name}")
                df = pd.read_json(source_obj)
                
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk agregasi waktu: {pathSource.name}")
            df = pd.read_json(pathSource)
            
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        if keyTime not in df.columns:
            logger.error(f"Kolom waktu '{keyTime}' tidak ditemukan di dataset.")
            return False

        initial_row_count = len(df)
        
        original_columns_order = df.columns.tolist()

        df[keyTime] = pd.to_datetime(df[keyTime], errors='coerce', dayfirst=True)
        df = df.dropna(subset=[keyTime])

        agg_dict = {col: 'mean' for col in keyValues}
        grouping_list = [keyTime] + groupKeys
        
        other_cols = [col for col in df.columns if col not in keyValues and col not in grouping_list]
        for col in other_cols:
            agg_dict[col] = 'first'

        df_grouped = df.groupby(grouping_list, as_index=False).agg(agg_dict)

        final_columns_order = [col for col in original_columns_order if col in df_grouped.columns]
        df_grouped = df_grouped[final_columns_order]

        final_row_count = len(df_grouped)
        merged_count = initial_row_count - final_row_count
        
        logger.info(f"Agregasi Selesai. {merged_count} baris data duplikat digabungkan.")

        df_grouped[keyTime] = df_grouped[keyTime].dt.strftime(timeFormat)

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df_grouped.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil agregasi waktu disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil agregasi sebagai String JSON.")
            return df_grouped.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses agregasi waktu JSON: {e}")
        return False