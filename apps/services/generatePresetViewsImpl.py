from apps.common.generatePresetViews import generatePresetViews

def main():
    pathSourceValue = "apps/data/result/forwardFilledData.json"
    pathSourceStatus = "apps/data/result/filteredRange.json"
    pathPreset = "apps/preset/preset.json"
    
    pathTarget = "apps/data/result/presetViewsData.json"

    print("Memulai pembuatan view...")

    hasil = generatePresetViews(
        pathSourceValue=pathSourceValue,
        pathSourceStatus=pathSourceStatus,
        pathPreset=pathPreset,
        pathTarget=pathTarget,
        indexCol="t"
    )

    if hasil is True:
        print(f"Selesai! Tabel view berhasil di-generate dan disimpan di file: {pathTarget}")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:300]}...\n")
    else:
        print("Proses gagal. Silakan cek log untuk detail error.")

if __name__ == "__main__":
    main()