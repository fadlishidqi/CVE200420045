from apps.common.pivotTable import pivotTable

def main():
    pathSource = "apps/data/result/filteredRange.json" 
    pathTarget = "apps/data/result/pivotedData.json"
    
    indexCols = ["t", "wct", "technum"]
    pivotCol = "param"
    valueCol = "nvalue"
    timeCol = "t"

    print("Memulai proses pivot table JSON...")
    hasil = pivotTable(
        pathSource=pathSource,
        pathTarget=pathTarget,
        indexCols=indexCols,
        pivotCol=pivotCol,
        valueCol=valueCol,
        timeCol=timeCol
    )

    if hasil is True:
        print(f"Selesai! File hasil pivot berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:500]}...\n")
    else:
        print("Gagal menjalankan pivot table JSON.\n")

if __name__ == "__main__":
    main()