from apps.common.aggregateTime import aggregateTime

def main():
    pathSource = "apps/data/result/convertedTime.json" 

    pathTarget = "apps/data/result/aggregatedTime.json"
    
    keyTime = "t" 
    keyValues = ["nvalue"]
    groupKeys = ["wct", "technum", "param"]

    print("Memulai proses agregasi waktu JSON...")
    hasil = aggregateTime(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keyTime=keyTime,
        keyValues=keyValues,
        groupKeys=groupKeys
    )

    if hasil is True:
        print(f"Selesai! File hasil agregasi berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:100]}...\n")
    else:
        print("Gagal menjalankan agregasi waktu JSON.\n")

if __name__ == "__main__":
    main()