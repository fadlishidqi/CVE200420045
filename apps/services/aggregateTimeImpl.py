from apps.common.aggregateTime import aggregateTime

def main():
    PathSource = "apps/data/result/convertedTime.json" 
    PathTarget = "apps/data/result/aggregatedTime.json"

    KeyTime = "t" 
    KeyValues = ["nvalue"]

    ExtraGroupKeys = ["wct", "technum", "param"] 

    print("Memulai proses agregasi waktu...")

    sukses = aggregateTime(
        path_source=PathSource,
        path_target=PathTarget,
        key_time=KeyTime,
        key_values=KeyValues,
        extra_group_keys=ExtraGroupKeys
    )

    if sukses:
        print("Proses agregasi waktu berhasil disimpan.")
    else:
        print("Proses agregasi waktu gagal.")

if __name__ == "__main__":
    main()