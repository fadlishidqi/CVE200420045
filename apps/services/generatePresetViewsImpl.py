from apps.common.generatePresetViews import generatePresetViews

def main():
    pathSourceValue = "apps/data/result/filledData.json"
    pathSourceStatus = "apps/data/result/filteredRange.json"
    pathPreset = "apps/preset/preset.json"
    pathTarget = "apps/data/result/presetViewsData.json"

    print("Memulai pembuatan view...")

    sukses = generatePresetViews(
        pathSourceValue=pathSourceValue,
        pathSourceStatus=pathSourceStatus,
        pathPreset=pathPreset,
        pathTarget=pathTarget,
        indexCol="t"
    )

    if sukses:
        print("Selesai! Tabel view berhasil di-generate dan diurutkan.")
    else:
        print("Proses gagal. Silakan cek log untuk detail error.")

if __name__ == "__main__":
    main()