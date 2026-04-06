import pandas as pd
import json
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def apply_iqr_outlier(
    pathSource: str | Path | pd.DataFrame, 
    pathTarget: str | Path | None = None,
    groupCols: list | None = None, 
    valueCol: str | None = None, 
    paramCol: str | None = None, 
    pathPreset: str | Path | dict | None = None
) -> bool | str | pd.DataFrame:
    
    if pathPreset is None:
        logger.error("PathPreset tidak boleh kosong.")
        return False

    try:
        preset_data = {}
        if isinstance(pathPreset, dict):
            preset_data = pathPreset
        elif isinstance(pathPreset, str) and (pathPreset.strip().startswith('{') or pathPreset.strip().startswith('[')):
            preset_data = json.loads(pathPreset)
        else:
            preset_obj = Path(pathPreset)
            if not preset_obj.exists():
                logger.error(f"File preset tidak ditemukan: {preset_obj}")
                return False
            with open(preset_obj, 'r') as f:
                preset_data = json.load(f)
            
        param_settings = preset_data.get("parameters", {})
        if not param_settings:
            logger.error("Tidak ada 'parameters' di dalam file preset.")
            return False

        min_dict = {k: v.get("rangeMin") for k, v in param_settings.items()}
        max_dict = {k: v.get("rangeMax") for k, v in param_settings.items()}

        df = None
        if isinstance(pathSource, pd.DataFrame):
            df = pathSource.copy()
        elif isinstance(pathSource, str):
            cleaned_str = pathSource.strip()
            if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                logger.info("Input terdeteksi sebagai String JSON. Membaca dari memori...")
                df = pd.read_json(io.StringIO(cleaned_str))
            else:
                source_obj = Path(pathSource)
                if not source_obj.exists():
                    logger.error(f"File sumber tidak ditemukan: {source_obj}")
                    return False
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        if not paramCol or not valueCol:
            logger.error("paramCol dan valueCol tidak boleh kosong.")
            return False
        
        if paramCol not in df.columns or valueCol not in df.columns:
            logger.error(f"Kolom '{paramCol}' atau '{valueCol}' tidak ditemukan di data.")
            return False

        df['__temp_min'] = df[paramCol].map(min_dict)
        df['__temp_max'] = df[paramCol].map(max_dict)

        df[valueCol] = pd.to_numeric(df[valueCol], errors="coerce")
        df['__temp_min'] = pd.to_numeric(df['__temp_min'], errors="coerce")
        df['__temp_max'] = pd.to_numeric(df['__temp_max'], errors="coerce")

        Q1 = df.groupby(groupCols)[valueCol].transform(lambda x: x.quantile(0.25))
        Q3 = df.groupby(groupCols)[valueCol].transform(lambda x: x.quantile(0.75))

        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        cond_min = df['__temp_min'].isna() | (df[valueCol] >= df['__temp_min'])
        cond_max = df['__temp_max'].isna() | (df[valueCol] <= df['__temp_max'])
        cond_iqr = (df[valueCol] >= lower) & (df[valueCol] <= upper)

        before_iqr = len(df)
        df_clean = df[cond_min & cond_max & cond_iqr].copy()
        
        df_clean = df_clean.drop(columns=['__temp_min', '__temp_max'])
        
        after_iqr = len(df_clean)
        logger.info(f"IQR Outlier & Preset Filtered: {before_iqr} -> {after_iqr} baris dibersihkan.")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df_clean.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil outlier disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil outlier sebagai String JSON.")
            return df_clean.to_json(orient="records")

    except Exception as e:
        logger.error(f"Error saat menjalankan fungsi IQR Outlier via Preset: {e}")
        return False