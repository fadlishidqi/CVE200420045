import pandas as pd
import json
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def filterRange(
    pathSource: str | Path, 
    pathTarget: str | Path | None = None, 
    pathPreset: str | Path | None = None, 
    keyValue: str = "nvalue", 
    keyParam: str = "param"
) -> bool | str:
    
    if not pathSource or not pathPreset:
        logger.error("PathSource atau PathPreset tidak boleh kosong.")
        return False

    preset_obj = Path(pathPreset)
    if not preset_obj.exists():
        logger.error(f"File preset tidak ditemukan: {preset_obj}")
        return False

    try:
        logger.info(f"Membaca file preset: {preset_obj.name}")
        with open(preset_obj, 'r') as f:
            preset_data = json.load(f)
            
        param_settings = preset_data.get("parameters", {})
        if not param_settings:
            logger.error("Tidak ada data 'parameters' di dalam file preset.")
            return False

        min_dict = {k: v.get("rangeMin") for k, v in param_settings.items()}
        max_dict = {k: v.get("rangeMax") for k, v in param_settings.items()}

        df = None
        
        if isinstance(pathSource, str):
            cleaned_str = pathSource.strip()
            if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                logger.info("Input terdeteksi sebagai String JSON. Membaca dari memori...")
                df = pd.read_json(io.StringIO(cleaned_str))
            else:
                source_obj = Path(pathSource)
                if not source_obj.exists():
                    logger.error(f"File sumber data tidak ditemukan: {source_obj}")
                    return False
                logger.info(f"Membaca file data: {source_obj.name}")
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file data: {pathSource.name}")
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        if keyValue not in df.columns or keyParam not in df.columns:
            logger.error(f"Kolom '{keyValue}' atau '{keyParam}' tidak ditemukan di dataset.")
            return False

        initial_row_count = len(df)
        df[keyValue] = pd.to_numeric(df[keyValue], errors='coerce')

        df['__temp_min'] = df[keyParam].map(min_dict)
        df['__temp_max'] = df[keyParam].map(max_dict)

        cond_not_null = df[keyValue].notna()
        cond_has_preset = df['__temp_min'].notna() & df['__temp_max'].notna()
        cond_in_range = (df[keyValue] >= df['__temp_min']) & (df[keyValue] <= df['__temp_max'])

        df_filtered = df[cond_not_null & cond_has_preset & cond_in_range].copy()
        df_filtered = df_filtered.drop(columns=['__temp_min', '__temp_max'])

        final_row_count = len(df_filtered)
        dropped_count = initial_row_count - final_row_count
        
        logger.info(f"Filter Range Selesai. {dropped_count} baris dibuang (Di luar batas preset atau parameter tidak terdaftar).")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df_filtered.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil filter range dengan preset disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil filter range sebagai String JSON.")
            return df_filtered.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses filter range via preset: {e}")
        return False