from apps.common.pivotTable import pivotTable

def main():
    pathSource = "apps/data/result/aggregatedTime.json"
    pathTarget = "apps/data/result/pivotedData.json"

    indexCols = ["t", "wct", "technum"] 
    pivotCol = "param"
    valueCol = "nvalue" 
    timeCol = "t"

    print("Memulai proses Pivot Table data...")

    sukses = pivotTable(
        pathSource=pathSource,
        pathTarget=pathTarget,
        indexCols=indexCols,
        pivotCol=pivotCol,
        valueCol=valueCol,
        timeCol=timeCol
    )

    if sukses:
        print("Proses pivot data berhasil disimpan!")
    else:
        print("Proses pivot data gagal.")

if __name__ == "__main__":
    main()