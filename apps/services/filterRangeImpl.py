from apps.common.filterRange import filterRange

def main():
    pathSource = "apps/data/result/comparedValue.json" 
    pathTarget = "apps/data/result/filteredRange.json"
    pathPreset = "apps/preset/preset.json"
    keyValue = "nvalue"
    keyParam = "param"

    print("Memulai proses filter range data JSON...")
    hasil = filterRange(
        pathSource=pathSource,
        pathTarget=pathTarget,
        pathPreset=pathPreset,
        keyValue=keyValue,
        keyParam=keyParam
    )

    if hasil is True:
        print(f"Selesai! File hasil filter range berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data tidak dibuat file, melainkan masuk ke memori/variabel.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan filter range JSON.\n")

if __name__ == "__main__":
    main()