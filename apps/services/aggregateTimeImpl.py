from apps.common.aggregateTime import aggregateTime

def main():
    pathSource = "apps/data/result/convertedTime.json" 
    pathTarget = "apps/data/result/aggregatedTime.json"
    keyTime = "t" 
    keyValues = ["nvalue"]
    groupKeys = ["wct", "technum", "param"] 

    print("Memulai proses agregasi waktu...")

    sukses = aggregateTime(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keyTime=keyTime,
        keyValues=keyValues,
        groupKeys=groupKeys
    )

    if sukses:
        print("Proses agregasi waktu berhasil disimpan.")
    else:
        print("Proses agregasi waktu gagal.")

if __name__ == "__main__":
    main()