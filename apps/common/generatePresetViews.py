import pandas as pd
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def generatePresetViews(
    pathSourceValue: str | Path,
    pathSourceStatus: str | Path,
    pathPreset: str | Path,
    pathTarget: str | Path,
    indexCol: str = 't'
) -> bool:
    try:
        p_val = Path(pathSourceValue)
        p_stat = Path(pathSourceStatus)
        p_preset = Path(pathPreset)

        if not p_val.exists():
            logger.error(f"File source value tidak ditemukan: {p_val}")
            return False
        if not p_stat.exists():
            logger.error(f"File source status tidak ditemukan: {p_stat}")
            return False
        if not p_preset.exists():
            logger.error(f"File preset tidak ditemukan: {p_preset}")
            return False

        logger.info("Membaca konfigurasi preset...")
        with open(p_preset, 'r') as f:
            preset = json.load(f)
            
        wct_val = preset.get("wctid", "UNKNOWN")
        tech_val = preset.get("technum", "UNKNOWN")
        preset_params = preset.get("parameters", {})

        logger.info("Membaca dataframe sumber...")
        df_val = pd.read_json(p_val)
        df_stat = pd.read_json(p_stat)

        if indexCol not in df_val.columns or indexCol not in df_stat.columns:
            logger.error(f"Kolom '{indexCol}' tidak ditemukan di salah satu data.")
            return False

        if 'param' not in df_stat.columns:
            logger.error("Kolom 'param' tidak ditemukan di data source status (filteredRange)!")
            return False

        df_val.set_index(indexCol, inplace=True)

        final_df = pd.DataFrame(index=df_val.index)

        nvalue_cols = []
        critical_cols = []
        threshold_cols = []

        for param_name, param_config in preset_params.items():
            
            if param_name in df_val.columns:
                nval_col = f"nvalue_{wct_val}_{tech_val}_{param_name}"
                final_df[nval_col] = df_val[param_name]
                nvalue_cols.append(nval_col)
            else:
                logger.warning(f"Parameter '{param_name}' tidak ada di source value.")

            if param_name in df_stat['param'].values:
                df_stat_filtered = df_stat[df_stat['param'] == param_name].set_index(indexCol)
                
                if param_config.get('category') is True and 'category' in df_stat_filtered.columns:
                    crit_col = f"critical_{wct_val}_{tech_val}_{param_name}"
                    final_df[crit_col] = df_stat_filtered['category']
                    critical_cols.append(crit_col)
                
                if param_config.get('threshold') is True and 'threshold' in df_stat_filtered.columns:
                    thresh_col = f"threshold_{wct_val}_{tech_val}_{param_name}"
                    final_df[thresh_col] = df_stat_filtered['threshold']
                    threshold_cols.append(thresh_col)
            else:
                logger.warning(f"Parameter '{param_name}' tidak ada di source status.")

        final_df.reset_index(inplace=True)

        ordered_cols = [indexCol] + nvalue_cols + critical_cols + threshold_cols
        
        ordered_cols = [col for col in ordered_cols if col in final_df.columns]
        final_df = final_df[ordered_cols]

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_json(target_obj, orient="records", indent=4)
        
        logger.info(f"SUKSES! View dengan urutan khusus berhasil disimpan di: {target_obj}")
        return True

    except Exception as e:
        logger.error(f"Gagal generate preset views: {e}")
        return False