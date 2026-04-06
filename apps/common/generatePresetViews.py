import pandas as pd
import json
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def generatePresetViews(
    pathSourceValue: str | Path,
    pathSourceStatus: str | Path,
    pathPreset: str | Path | dict,
    pathTarget: str | Path | None = None,
    indexCol: str = 't'
) -> bool | str:
    
    try:
        preset = {}
        if isinstance(pathPreset, dict):
            preset = pathPreset
        elif isinstance(pathPreset, str) and (pathPreset.strip().startswith('{') or pathPreset.strip().startswith('[')):
            logger.info("Membaca konfigurasi preset dari String JSON di memori...")
            preset = json.loads(pathPreset)
        else:
            p_preset = Path(pathPreset)
            if not p_preset.exists():
                logger.error(f"File preset tidak ditemukan: {p_preset}")
                return False
            logger.info("Membaca konfigurasi preset dari file...")
            with open(p_preset, 'r') as f:
                preset = json.load(f)
                
        wct_val = preset.get("wctid", "UNKNOWN")
        tech_val = preset.get("technum", "UNKNOWN")
        preset_params = preset.get("parameters", {})

        logger.info("Membaca dataframe source value...")
        df_val = None
        if isinstance(pathSourceValue, str):
            cleaned = pathSourceValue.strip()
            if cleaned.startswith('{') or cleaned.startswith('['):
                df_val = pd.read_json(io.StringIO(cleaned))
            else:
                p_val = Path(pathSourceValue)
                if not p_val.exists():
                    logger.error(f"File source value tidak ditemukan: {p_val}")
                    return False
                df_val = pd.read_json(p_val)
        elif isinstance(pathSourceValue, Path):
            df_val = pd.read_json(pathSourceValue)

        logger.info("Membaca dataframe source status...")
        df_stat = None
        if isinstance(pathSourceStatus, str):
            cleaned = pathSourceStatus.strip()
            if cleaned.startswith('{') or cleaned.startswith('['):
                df_stat = pd.read_json(io.StringIO(cleaned))
            else:
                p_stat = Path(pathSourceStatus)
                if not p_stat.exists():
                    logger.error(f"File source status tidak ditemukan: {p_stat}")
                    return False
                df_stat = pd.read_json(p_stat)
        elif isinstance(pathSourceStatus, Path):
            df_stat = pd.read_json(pathSourceStatus)

        if df_val is None or df_stat is None:
            logger.error("Gagal membaca dataframe source.")
            return False

        if indexCol not in df_val.columns or indexCol not in df_stat.columns:
            logger.error(f"Kolom '{indexCol}' tidak ditemukan di salah satu data.")
            return False

        if 'param' not in df_stat.columns:
            logger.error("Kolom 'param' tidak ditemukan di data source status (filteredRange)!")
            return False

        join_keys = [indexCol]
        if 'wct' in df_val.columns and 'wct' in df_stat.columns:
            join_keys.append('wct')
        if 'technum' in df_val.columns and 'technum' in df_stat.columns:
            join_keys.append('technum')

        df_val.set_index(join_keys, inplace=True)
        df_val = df_val[~df_val.index.duplicated(keep='first')]

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
                df_stat_filtered = df_stat[df_stat['param'] == param_name].set_index(join_keys)
                
                df_stat_filtered = df_stat_filtered[~df_stat_filtered.index.duplicated(keep='first')]
                
                if param_config.get('category') is True and 'category' in df_stat_filtered.columns:
                    crit_col = f"critical_{wct_val}_{tech_val}_{param_name}"
                    final_df[crit_col] = df_stat_filtered['category']
                    critical_cols.append(crit_col)
                
                if param_config.get('threshold') is True and 'threshold' in df_stat_filtered.columns:
                    thresh_col = f"threshold_{wct_val}_{tech_val}_{param_name}"
                    final_df[thresh_col] = df_stat_filtered['threshold']
                    threshold_cols.append(thresh_col)

        final_df.reset_index(inplace=True)

        ordered_cols = join_keys + nvalue_cols + critical_cols + threshold_cols
        ordered_cols = [col for col in ordered_cols if col in final_df.columns]
        final_df = final_df[ordered_cols]

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            final_df.to_json(target_obj, orient="records", indent=4)
            
            logger.info(f"SUKSES! View dengan urutan khusus berhasil disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan view data sebagai String JSON.")
            return final_df.to_json(orient="records")

    except Exception as e:
        logger.error(f"Gagal generate preset views: {e}")
        return False