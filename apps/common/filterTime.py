import pandas as pd
from pathlib import Path
import logging
from typing import Literal

logger = logging.getLogger(__name__)

def filterTime(
    path_source: str | Path, 
    path_target: str | Path, 
    key_start: str, 
    key_end: str, 
    max_delta: int | float, 
    unit_delta: Literal['days', 'day', 'hours', 'hour', 'minutes', 'minute', 'seconds', 'second', 'W', 'D', 'h', 'm', 's']
) -> bool:
    """
    Memfilter baris data berdasarkan selisih waktu antar dua kolom.
    """
    
    # 1. Validasi Input Dasar
    if not path_source or not path_target:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not key_start or not key_end:
        logger.error("KeyStart atau KeyEnd tidak boleh kosong.")
        return False

    source_obj = Path(path_source)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        try:
            max_td = pd.Timedelta(value=max_delta, unit=unit_delta)
        except Exception as e:
            logger.error(f"Gagal mendefinisikan Timedelta. Pastikan parameter valid: {e}")
            return False

        logger.info(f"Membaca file untuk proses filter time delta: {source_obj.name}")
        df = pd.read_json(source_obj)

        # 2. Pengecekan Eksistensi Kolom
        if key_start not in df.columns or key_end not in df.columns:
            logger.error(f"Kolom '{key_start}' atau '{key_end}' tidak ditemukan di dataset.")
            return False

        initial_row_count = len(df)

        # 3. Konversi ke Variabel Sementara (Aman dari String/Datetime conflict)
        # Menggunakan errors='coerce' agar data error berubah jadi NaT, bukan String
        # dayfirst=True membantu Pandas membaca format seperti DD-MM-YYYY
        dt_start = pd.to_datetime(df[key_start], errors='coerce', dayfirst=True)
        dt_end = pd.to_datetime(df[key_end], errors='coerce', dayfirst=True)

        # 4. Proses Eksekusi Filter pada variabel sementara
        # Kondisi 1: Pastikan datanya bisa terbaca waktu (bukan NaT)
        cond_valid_date = dt_start.notna() & dt_end.notna()
        
        # Kondisi 2: Waktu awal tidak boleh melebihi waktu akhir
        cond_not_future = dt_start <= dt_end
        
        # Kondisi 3: Selisih maksimal sesuai max_delta
        cond_max_delta = (dt_end - dt_start) <= max_td
        
        # Terapkan filter ke DataFrame asli
        df_filtered = df[cond_valid_date & cond_not_future & cond_max_delta].copy()
        
        final_row_count = len(df_filtered)
        dropped_count = initial_row_count - final_row_count
        
        logger.info(f"Filter Selesai. Menghapus {dropped_count} baris yang tidak memenuhi syarat/berformat salah.")

        # 5. Menyimpan Hasil
        if final_row_count == 0:
            logger.warning("Peringatan: Seluruh data terhapus oleh filter. File JSON akan kosong.")

        target_obj = Path(path_target)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        # Data yang disimpan mempertahankan format aslinya tanpa diacak-acak oleh Pandas
        df_filtered.to_json(target_obj, orient="records", indent=4, date_format="iso")
        logger.info(f"SUKSES! File hasil filter time delta disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses filter time delta JSON: {e}")
        return False