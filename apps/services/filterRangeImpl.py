from apps.common.filterRange import filterRange

def main():
    pathSource = "apps/data/result/aggregatedTime.json"
    pathTarget = "apps/data/result/filteredRange.json"
    pathPreset = "apps/preset/preset.json"

    keyValue = "nvalue"
    keyParam = "param"

    print("Memulai proses filter range...")

    sukses = filterRange(
        pathSource=pathSource,
        pathTarget=pathTarget,
        pathPreset=pathPreset,
        keyValue=keyValue,
        keyParam=keyParam
    )

    if sukses:
        print("Proses filter berhasil disimpan!")
    else:
        print("Proses filter gagal.")

if __name__ == "__main__":
    main()