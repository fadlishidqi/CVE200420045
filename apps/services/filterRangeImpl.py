from apps.common.filterRange import filterRange

def main():
    # Lokasi file-file Anda
    PathSource = "apps/data/result/aggregatedTime.json"
    PathTarget = "apps/data/result/filteredRange.json"
    
    # Lokasi file preset JSON Anda
    PathPreset = "apps/preset/preset.json"

    KeyValue = "nvalue"
    KeyParam = "param"

    print("Memulai proses filter range...")

    sukses = filterRange(
        path_source=PathSource,
        path_target=PathTarget,
        path_preset=PathPreset,
        key_value=KeyValue,
        key_param=KeyParam
    )

    if sukses:
        print("Proses filter berhasil disimpan!")
    else:
        print("Proses filter gagal.")

if __name__ == "__main__":
    main()