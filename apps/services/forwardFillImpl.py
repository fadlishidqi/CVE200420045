from apps.common.forwardFill import forwardFill

def main():
    pathSource = "apps/data/result/pivotedData.json" 
    
    pathTarget = "apps/data/result/forwardFilledData.json"
    groupCols = ["wct", "technum"] 
    timeCol = "t"

    print("Memulai proses forward fill JSON...")
    hasil = forwardFill(
        pathSource=pathSource,
        pathTarget=pathTarget,
        groupCols=groupCols,
        timeCol=timeCol
    )

    if hasil is True:
        print(f"Selesai! File hasil pengisian berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:300]}...\n")
    else:
        print("Gagal menjalankan forward fill JSON.\n")

if __name__ == "__main__":
    main()