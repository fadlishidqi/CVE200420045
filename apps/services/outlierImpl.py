from apps.common.outlier import apply_iqr_outlier

def main():
    pathSource = "apps/data/result/filteredRange.json" 
    
    pathTarget = "apps/data/result/outlierHandled.json"
    
    pathPreset = "apps/preset/preset.json"
    groupCols = ["technum", "param"]
    valueCol = "nvalue"
    paramCol = "param"

    print("Memulai proses IQR Outlier JSON...")
    hasil = apply_iqr_outlier(
        pathSource=pathSource,
        pathTarget=pathTarget,
        groupCols=groupCols,
        valueCol=valueCol,
        paramCol=paramCol,
        pathPreset=pathPreset
    )

    if hasil is True:
        print(f"Selesai! File hasil pembersihan outlier berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan IQR Outlier JSON.\n")

if __name__ == "__main__":
    main()