from apps.common.filterTime import filterTime

def main():
    pathSource = "apps/data/result/convertedTime.json"
    pathTarget = "apps/data/result/filteredTime.json"

    keyStart = "t"
    keyEnd = "dcrea"
    maxDelta = 1
    unitDelta = "hours"

    print("Memulai proses filter time...")

    sukses = filterTime(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keyStart=keyStart,
        keyEnd=keyEnd,
        maxDelta=maxDelta,
        unitDelta=unitDelta
    )

    if sukses:
        print("Proses filter berhasil disimpan.")
    else:
        print("Proses filter gagal.")

if __name__ == "__main__":
    main()