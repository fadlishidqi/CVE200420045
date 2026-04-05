from apps.common.compareValue import compareValue

def main():
    pathSource = "apps/data/result/replacedData.json"
    pathTarget = "apps/data/result/comparedValue.json"

    keySource = "nvalue"
    keyResult = "threshold"
    defaultValue = 0

    keyCompare = ["llow", "hhigh", "low", "high"] 
    operators = ["<", ">", "<", ">"]
    resultCompare = [-2, 2, -1, 1]

    print("Memulai proses komparasi nilai...")

    sukses = compareValue(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource,
        keyCompare=keyCompare,
        operators=operators,
        resultCompare=resultCompare,
        defaultValue=defaultValue,
        keyResult=keyResult
    )

    if sukses:
        print("Proses komparasi berhasil disimpan.")
    else:
        print("Proses komparasi gagal.")

if __name__ == "__main__":
    main()