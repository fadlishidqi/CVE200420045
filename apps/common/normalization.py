import pandas as pd
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def apply_minmax_normalization(
    df: pd.DataFrame, 
    valueCol: str, 
    paramCol: str,
    pathPreset: str | Path
) -> pd.DataFrame:
    
    preset_obj = Path(pathPreset)
    if not preset_obj.exists():
        logger.error(f"File preset tidak ditemukan: {preset_obj}")
        return df
        
    try:
        with open(preset_obj, 'r') as f:
            preset_data = json.load(f)
            
        param_settings = preset_data.get("parameters", {})
        min_dict = {k: v.get("rangeMin") for k, v in param_settings.items()}
        max_dict = {k: v.get("rangeMax") for k, v in param_settings.items()}

        if paramCol not in df.columns or valueCol not in df.columns:
            logger.error(f"Kolom '{paramCol}' atau '{valueCol}' tidak ditemukan di data.")
            return df

        df['__temp_min'] = df[paramCol].map(min_dict)
        df['__temp_max'] = df[paramCol].map(max_dict)

        df[valueCol] = pd.to_numeric(df[valueCol], errors="coerce")
        df['__temp_min'] = pd.to_numeric(df['__temp_min'], errors="coerce")
        df['__temp_max'] = pd.to_numeric(df['__temp_max'], errors="coerce")
            
        range_diff = df['__temp_max'] - df['__temp_min']
        
        mask = (range_diff > 0) & df['__temp_min'].notnull() & df['__temp_max'].notnull() & df[valueCol].notnull()
        
        df.loc[mask, valueCol] = (df.loc[mask, valueCol] - df.loc[mask, '__temp_min']) / range_diff[mask]
        
        df = df.drop(columns=['__temp_min', '__temp_max'])
        
        logger.info("Min-Max Normalization via Preset berhasil diterapkan.")
        return df
        
    except Exception as e:
        logger.error(f"Error saat menjalankan fungsi Normalisasi via Preset: {e}")
        return df