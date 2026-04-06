from apps.common.mergeJson import mergeJson

def main():
    pathSource = [
        "apps/data/raw/POLIMER_CUSHION-POSITION_3M2026.json",
        "apps/data/raw/POLIMER_INJ-MAX-PRESSURE_3M2026.json",
        "apps/data/raw/POLIMER_NH1-TEMP_3M2026.json",
        "apps/data/raw/POLIMER_NH2-TEMP_3M2026.json",
    ]
    
    pathTarget = "apps/data/result/rawMerge.json" 
    
    keySource = [
        "time", "dcrea", "vwctid", "vmachineid", "vparam", 
        "nvalue", "vvalue", "nhhigh", "nhigh", "nllow", "nlow", "catg"
    ]
    
    print("Memulai proses merge JSON dengan membuang kolom yang tidak perlu...")
    hasil = mergeJson(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource
    )
    
    if hasil is True:
        print(f"Selesai! File gabungan berhasil dibuat di: {pathTarget}\n")
    elif isinstance(hasil, str):
        print("Selesai! Data disimpan dalam JsonString.")
        print(f"Bentuk datanya: {hasil[:200]}...\n")
    else:
        print("Gagal menjalankan merge JSON.\n")

if __name__ == "__main__":
    main()