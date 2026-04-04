import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Any

logger = logging.getLogger(__name__)

def compareValue(
    path_source: str | Path, 
    path_target: str | Path, 
    key_source: str, 
    key_compare: list, 
    operators: list, 
    result_compare: list, 
    default_value: Any,
    key_result: str
) -> bool:
    
    if not path_source or not path_target:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not key_source or not key_result:
        logger.error("KeySource atau KeyResult tidak boleh kosong.")
        return False

    if not (len(key_compare) == len(operators) == len(result_compare)):
        logger.error("Gagal: Jumlah KeyComparation, Operator, dan ResultComparation harus sama persis!")
        return False

    source_obj = Path(path_source)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk proses komparasi nilai: {source_obj.name}")
        df = pd.read_json(source_obj)

        if key_source not in df.columns:
            logger.error(f"Kolom KeySource '{key_source}' tidak ditemukan di dataset.")
            return False

        missing_cols = [col for col in key_compare if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom pembanding berikut tidak ditemukan: {missing_cols}")
            return False
        
        conditions = []
        for col, op in zip(key_compare, operators):
            if op == '<':
                conditions.append(df[key_source] < df[col])
            elif op == '<=':
                conditions.append(df[key_source] <= df[col])
            elif op == '>':
                conditions.append(df[key_source] > df[col])
            elif op == '>=':
                conditions.append(df[key_source] >= df[col])
            elif op == '==':
                conditions.append(df[key_source] == df[col])
            elif op == '!=':
                conditions.append(df[key_source] != df[col])
            else:
                logger.error(f"Operator tidak valid: {op}")
                return False
            
        df[key_result] = np.select(conditions, result_compare, default=default_value)
            
        logger.info(f"Berhasil membandingkan {key_source} dengan {key_compare} dan membuat kolom baru '{key_result}'")

        target_obj = Path(path_target)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil komparasi disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses komparasi JSON: {e}")
        return False