import pandas as pd
from pathlib import Path
import logging
from typing import Literal

logger = logging.getLogger(__name__)

def filterTime(
    pathSource: str | Path, 
    pathTarget: str | Path, 
    keyStart: str, 
    keyEnd: str, 
    maxDelta: int | float, 
    unitDelta: Literal['days', 'day', 'hours', 'hour', 'minutes', 'minute', 'seconds', 'second', 'W', 'D', 'h', 'm', 's']
) -> bool:
    
    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not keyStart or not keyEnd:
        logger.error("KeyStart atau KeyEnd tidak boleh kosong.")
        return False

    source_obj = Path(pathSource)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        try:
            max_td = pd.Timedelta(value=maxDelta, unit=unitDelta)
        except Exception as e:
            logger.error(f"Gagal mendefinisikan Timedelta. Pastikan parameter valid: {e}")
            return False

        logger.info(f"Membaca file untuk proses filter time delta: {source_obj.name}")
        df = pd.read_json(source_obj)

        if keyStart not in df.columns or keyEnd not in df.columns:
            logger.error(f"Kolom '{keyStart}' atau '{keyEnd}' tidak ditemukan di dataset.")
            return False

        initial_row_count = len(df)

        dt_start = pd.to_datetime(df[keyStart], errors='coerce', dayfirst=True)
        dt_end = pd.to_datetime(df[keyEnd], errors='coerce', dayfirst=True)

        cond_valid_date = dt_start.notna() & dt_end.notna()
        cond_not_future = dt_start <= dt_end
        cond_max_delta = (dt_end - dt_start) <= max_td
        
        df_filtered = df[cond_valid_date & cond_not_future & cond_max_delta].copy()
        
        final_row_count = len(df_filtered)
        dropped_count = initial_row_count - final_row_count
        
        logger.info(f"Filter Selesai. Menghapus {dropped_count} baris yang tidak memenuhi syarat/berformat salah.")

        if final_row_count == 0:
            logger.warning("Peringatan: Seluruh data terhapus oleh filter. File JSON akan kosong.")

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df_filtered.to_json(target_obj, orient="records", indent=4, date_format="iso")
        logger.info(f"SUKSES! File hasil filter time delta disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses filter time delta JSON: {e}")
        return False