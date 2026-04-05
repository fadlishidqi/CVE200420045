import pandas as pd
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def apply_iqr_outlier(
    df: pd.DataFrame, 
    groupCols: list, 
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
        if not param_settings:
            logger.error("Tidak ada 'parameters' di dalam file preset.")
            return df

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
        return df_clean

    except Exception as e:
        logger.error(f"Error saat menjalankan fungsi IQR Outlier via Preset: {e}")
        return df