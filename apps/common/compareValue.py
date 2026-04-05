import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Any

logger = logging.getLogger(__name__)

def compareValue(
    pathSource: str | Path, 
    pathTarget: str | Path, 
    keySource: str, 
    keyCompare: list, 
    operators: list, 
    resultCompare: list, 
    defaultValue: Any,
    keyResult: str
) -> bool:
    
    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    if not keySource or not keyResult:
        logger.error("KeySource atau KeyResult tidak boleh kosong.")
        return False

    if not (len(keyCompare) == len(operators) == len(resultCompare)):
        logger.error("Gagal: Jumlah KeyComparation, Operator, dan ResultComparation harus sama persis!")
        return False

    source_obj = Path(pathSource)
    
    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    try:
        logger.info(f"Membaca file untuk proses komparasi nilai: {source_obj.name}")
        df = pd.read_json(source_obj)

        if keySource not in df.columns:
            logger.error(f"Kolom KeySource '{keySource}' tidak ditemukan di dataset.")
            return False

        missing_cols = [col for col in keyCompare if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom pembanding berikut tidak ditemukan: {missing_cols}")
            return False
        
        conditions = []
        for col, op in zip(keyCompare, operators):
            if op == '<':
                conditions.append(df[keySource] < df[col])
            elif op == '<=':
                conditions.append(df[keySource] <= df[col])
            elif op == '>':
                conditions.append(df[keySource] > df[col])
            elif op == '>=':
                conditions.append(df[keySource] >= df[col])
            elif op == '==':
                conditions.append(df[keySource] == df[col])
            elif op == '!=':
                conditions.append(df[keySource] != df[col])
            else:
                logger.error(f"Operator tidak valid: {op}")
                return False
            
        df[keyResult] = np.select(conditions, resultCompare, default=defaultValue)
            
        logger.info(f"Berhasil membandingkan {keySource} dengan {keyCompare} dan membuat kolom baru '{keyResult}'")

        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(target_obj, orient="records", indent=4)
        logger.info(f"SUKSES! File hasil komparasi disimpan di: {target_obj}")
        
        return True
        
    except Exception as e:
        logger.error(f"Gagal memproses komparasi JSON: {e}")
        return False