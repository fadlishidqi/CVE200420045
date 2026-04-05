import pandas as pd
from pathlib import Path
from apps.common.outlier import apply_iqr_outlier

def main():
    pathSource = "apps/data/result/filteredRange.json"
    pathTarget = "apps/data/result/outlierHandled.json"
    pathPreset = "apps/preset/preset.json"

    groupCols = ["param"]
    valueCol = "nvalue"
    paramCol = "param"
    
    try:
        df = pd.read_json(pathSource)
    except Exception as e:
        print(f"Gagal membaca file JSON: {e}")
        return

    df_clean = apply_iqr_outlier(
        df=df,
        groupCols=groupCols,
        valueCol=valueCol,
        paramCol=paramCol,
        pathPreset=pathPreset
    )

    target_obj = Path(pathTarget)
    target_obj.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_json(target_obj, orient="records", indent=4)
    
    print(f"Selesai! Data bebas outlier disimpan di: {pathTarget}")

if __name__ == "__main__":
    main()