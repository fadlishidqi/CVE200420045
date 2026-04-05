from apps.common.mergeJson import mergeJson

def main():
    pathSource = [
        "apps/data/raw/POLIMER_CUSHION-POSITION_3M2026.json",
        "apps/data/raw/POLIMER_INJ-MAX-PRESSURE_3M2026.json",
        "apps/data/raw/POLIMER_NH1-TEMP_3M2026.json",
        "apps/data/raw/POLIMER_NH2-TEMP_3M2026.json",
    ]

    pathTarget = "apps/data/result/rawMerge.json"

    keySource = [
        "time", "dcrea", "vwctid", "vmachineid", 
        "vparam", "nvalue", "vvalue", "nhhigh", "nhigh", "nllow", "nlow", "catg"
    ]

    print("Memulai proses merge data...")

    sukses = mergeJson(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource
    )

    if sukses:
        print("Proses merge berhasil disimpan.")
    else:
        print("Proses merge data gagal.")

if __name__ == "__main__":
    main()