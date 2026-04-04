from apps.common.filterTime import filterTime

def main():
    PathSource = "apps/data/result/convertedTime.json"
    PathTarget = "apps/data/result/filteredTime.json"

    KeyStart = "t"
    KeyEnd = "dcrea"
    MaxDelta = 1
    UnitDelta = "hours"

    print("Memulai proses filter time...")

    sukses = filterTime(
        path_source=PathSource,
        path_target=PathTarget,
        key_start=KeyStart,
        key_end=KeyEnd,
        max_delta=MaxDelta,
        unit_delta=UnitDelta
    )

    if sukses:
        print("Proses filter berhasil disimpan.")
    else:
        print("Proses filter gagal.")

if __name__ == "__main__":
    main()