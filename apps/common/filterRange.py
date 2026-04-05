import pandas as pd
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def filterRange(
    path_source: str | Path, 
    path_target: str | Path, 
    path_preset: str | Path, 
    key_value: str = "nvalue", 
    key_param: str = "param"
) -> bool:
    """
    Memfilter baris data berdasarkan rangeMin dan rangeMax yang diambil otomatis 
    dari file preset.json. Mencocokkan batas nilai berdasarkan nama parameter.
    """
    
    # 1. Validasi Input Path
    if not path_source or not path_target or not path_preset:
        logger.error("PathSource, PathTarget, atau PathPreset tidak boleh kosong.")
        return False

    source_obj = Path(path_source)
    preset_obj = Path(path_preset)
    
    if not source_obj.exists():
        logger.error(f"File sumber data tidak ditemukan: {source_obj}")
        return False

    if not preset_obj.exists():
        logger.error(f"File preset tidak ditemukan: {preset_obj}")
        return False

    try:
        # 2. Membaca File Preset
        logger.info(f"Membaca file preset: {preset_obj.name}")
        with open(preset_obj, 'r') as f:
            preset_data = json.load(f)
            
        param_settings = preset_data.get("parameters", {})
        if not param_settings:
            logger.error("Tidak ada data 'parameters' di dalam file preset.")
            return False

        # Membuat kamus (dictionary) batas min & max untuk mapping cepat
        min_dict = {k: v.get("rangeMin") for k, v in param_settings.items()}
        max_dict = {k: v.get("rangeMax") for k, v in param_settings.items()}

        # 3. Membaca Data JSON
        logger.info(f"Membaca file data: {source_obj.name}")
        df = pd.read_json(source_obj)

        if key_value not in df.columns or key_param not in df.columns:
            logger.error(f"Kolom '{key_value}' atau '{key_param}' tidak ditemukan di dataset.")
            return False

        initial_row_count = len(df)
        df[key_value] = pd.to_numeric(df[key_value], errors='coerce')

        # 4. Mapping Batas Nilai ke DataFrame
        # Membuat kolom sementara (akan dihapus nanti) berisi nilai min & max sesuai nama 'param'
        df['__temp_min'] = df[key_param].map(min_dict)
        df['__temp_max'] = df[key_param].map(max_dict)

        # 5. Proses Filter Eksekusi
        # Kondisi 1: Pastikan angka valid
        cond_not_null = df[key_value].notna()
        # Kondisi 2: Pastikan parameter tersebut ADA di preset.json (jika tidak ada, data dianggap tidak valid / dibuang)
        cond_has_preset = df['__temp_min'].notna() & df['__temp_max'].notna()
        # Kondisi 3: Cek apakah nvalue >= rangeMin dan nvalue <= rangeMax
        cond_in_range = (df[key_value] >= df['__temp_min']) & (df[key_value] <= df['__temp_max'])

        # Terapkan semua kondisi
        df_filtered = df[cond_not_null & cond_has_preset & cond_in_range].copy()
        
        # Hapus kolom sementara agar JSON kembali bersih
        df_filtered = df_filtered.drop(columns=['__temp_min', '__temp_max'])

        final_row_count = len(df_filtered)
        dropped_count = initial_row_count - final_row_count
        
        logger.info(f"Filter Range Selesai. {dropped_count} baris dibuang (Di luar batas preset atau parameter tidak terdaftar).")

        # 6. Menyimpan Hasil
        target_obj = Path(path_target)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df_filtered.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil filter range dengan preset disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses filter range via preset: {e}")
        return False