from apps.common.forwardFill import forwardFill

def main():
    pathSource = "apps/data/result/pivotedData.json"
    pathTarget = "apps/data/result/filledData.json"
    timeCol = "t"
    groupCols = ["wct", "technum"] 

    print("Memulai proses Forward Fill untuk data yang kosong (null)...")

    sukses = forwardFill(
        pathSource=pathSource,
        pathTarget=pathTarget,
        groupCols=groupCols,
        timeCol=timeCol
    )

    if sukses:
        print("Data kosong berhasil diisi dengan data sebelumnya!")
    else:
        print("Proses forward fill gagal.")

if __name__ == "__main__":
    main()