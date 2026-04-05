import pandas as pd
from pathlib import Path
from apps.common.normalization import apply_minmax_normalization

def main():
    pathSource = "apps/data/result/outlierHandled.json" 
    pathTarget = "apps/data/result/normalizedData.json"
    pathPreset = "apps/preset/preset.json"

    valueCol = "nvalue"
    paramCol = "param"
    
    try:
        df = pd.read_json(pathSource)
    except Exception as e:
        print(f"Gagal membaca file JSON: {e}")
        return

    df_normalized = apply_minmax_normalization(
        df=df,
        valueCol=valueCol,
        paramCol=paramCol,
        pathPreset=pathPreset
    )

    target_obj = Path(pathTarget)
    target_obj.parent.mkdir(parents=True, exist_ok=True)
    df_normalized.to_json(target_obj, orient="records", indent=4)
    
    print(f"Selesai! Data hasil normalisasi disimpan di: {pathTarget}")

if __name__ == "__main__":
    main()