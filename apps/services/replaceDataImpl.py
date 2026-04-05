from apps.common.replaceData import replaceData

def main():
    pathSource = "apps/data/result/filteredTime.json"
    pathTarget = "apps/data/result/replacedData.json"

    keySource = ["category"] 
    fromData = ["C", "NC"]
    toData = [1, 0]

    print("Memulai proses replace data...")

    sukses = replaceData(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource,
        fromData=fromData,
        toData=toData
    )

    if sukses:
        print("Proses replace data berhasil disimpan.")
    else:
        print("Proses replace data gagal.")

if __name__ == "__main__":
    main()