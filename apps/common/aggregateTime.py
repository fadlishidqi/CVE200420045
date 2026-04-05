import pandas as pd
from pathlib import Path
import logging
from typing import List

logger = logging.getLogger(__name__)

def aggregateTime(
    pathSource: str | Path, 
    pathTarget: str | Path, 
    keyTime: str, 
    keyValues: List[str],
    groupKeys: List[str] | None = None,
    timeFormat: str = '%d-%m-%Y %H:%M:%S' 
) -> bool:
    
    if groupKeys is None:
        groupKeys = []
        
    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not keyTime or not keyValues:
        logger.error("KeyTime atau KeyValues tidak boleh kosong.")
        return False

    source_obj = Path(pathSource)
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk agregasi waktu: {source_obj.name}")
        df = pd.read_json(source_obj)

        if keyTime not in df.columns:
            logger.error(f"Kolom waktu '{keyTime}' tidak ditemukan di dataset.")
            return False

        initial_row_count = len(df)

        df[keyTime] = pd.to_datetime(df[keyTime], errors='coerce', dayfirst=True)
        df = df.dropna(subset=[keyTime])

        agg_dict = {col: 'mean' for col in keyValues}
        
        grouping_list = [keyTime] + groupKeys
        
        other_cols = [col for col in df.columns if col not in keyValues and col not in grouping_list]
        for col in other_cols:
            agg_dict[col] = 'first'

        df_grouped = df.groupby(grouping_list, as_index=False).agg(agg_dict)

        final_row_count = len(df_grouped)
        merged_count = initial_row_count - final_row_count
        
        logger.info(f"Agregasi Selesai. {merged_count} baris data duplikat digabungkan.")

        df_grouped[keyTime] = df_grouped[keyTime].dt.strftime(timeFormat)

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df_grouped.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil agregasi waktu disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses agregasi waktu JSON: {e}")
        return False