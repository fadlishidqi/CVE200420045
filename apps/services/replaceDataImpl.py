from apps.common.replaceData import replaceData

def main():
    PathSource = "apps/data/result/filteredTime.json"
    PathTarget = "apps/data/result/replacedData.json"

    KeySource = ["category"] 
    FromData = ["C", "NC"]
    ToData = [1, 0]

    print("Memulai proses replace data...")

    sukses = replaceData(
        path_source=PathSource,
        path_target=PathTarget,
        key_source=KeySource,
        from_val=FromData,
        to_val=ToData
    )

    if sukses:
        print("Proses replace data berhasil disimpan.")
    else:
        print("Proses replace data gagal.")

if __name__ == "__main__":
    main()