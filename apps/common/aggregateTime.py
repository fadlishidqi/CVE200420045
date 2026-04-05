import pandas as pd
from pathlib import Path
import logging
from typing import List

logger = logging.getLogger(__name__)

def aggregateTime(
    path_source: str | Path, 
    path_target: str | Path, 
    key_time: str, 
    key_values: List[str],
    extra_group_keys: List[str] | None = None,
    time_format: str = '%d-%m-%Y %H:%M:%S' 
) -> bool:
    
    if extra_group_keys is None:
        extra_group_keys = []
        
    if not path_source or not path_target:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not key_time or not key_values:
        logger.error("KeyTime atau KeyValues tidak boleh kosong.")
        return False

    source_obj = Path(path_source)
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk agregasi waktu: {source_obj.name}")
        df = pd.read_json(source_obj)

        if key_time not in df.columns:
            logger.error(f"Kolom waktu '{key_time}' tidak ditemukan di dataset.")
            return False

        initial_row_count = len(df)

        df[key_time] = pd.to_datetime(df[key_time], errors='coerce', dayfirst=True)
        df = df.dropna(subset=[key_time])

        # Setup target kolom yang dirata-rata
        agg_dict = {col: 'mean' for col in key_values}
        
        # Setup kombinasi kolom untuk pengelompokan (Misal: waktu + technum + param)
        grouping_list = [key_time] + extra_group_keys
        
        # Kolom sisanya diambil baris pertamanya saja
        other_cols = [col for col in df.columns if col not in key_values and col not in grouping_list]
        for col in other_cols:
            agg_dict[col] = 'first'

        # Proses Grouping dengan kriteria berlapis
        df_grouped = df.groupby(grouping_list, as_index=False).agg(agg_dict)

        final_row_count = len(df_grouped)
        merged_count = initial_row_count - final_row_count
        
        logger.info(f"Agregasi Selesai. {merged_count} baris data duplikat digabungkan.")

        # Kembalikan format waktu ke string rapi
        df_grouped[key_time] = df_grouped[key_time].dt.strftime(time_format)

        # Menyimpan Hasil
        target_obj = Path(path_target)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df_grouped.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil agregasi waktu disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses agregasi waktu JSON: {e}")
        return False