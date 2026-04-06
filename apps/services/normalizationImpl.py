from apps.common.normalization import apply_minmax_normalization

def main():
    pathSource = "apps/data/result/outlierHandled.json" 
    
    pathTarget = "apps/data/result/normalizedData.json"
    
    pathPreset = "apps/preset/preset.json"
    valueCol = "nvalue"
    paramCol = "param"

    print("Memulai proses Min-Max Normalization JSON...")
    hasil = apply_minmax_normalization(
        pathSource=pathSource,
        pathTarget=pathTarget,
        valueCol=valueCol,
        paramCol=paramCol,
        pathPreset=pathPreset
    )

    if hasil is True:
        print(f"Selesai! File hasil normalisasi berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan normalisasi JSON.\n")

if __name__ == "__main__":
    main()