from apps.common.mergeJson import mergeJson

def main():
    pathSource = [
        "apps/data/raw/CUSHIONPOS.json",
        "apps/data/raw/MAXPRESSURE.json",
        "apps/data/raw/NH1.json",
        "apps/data/raw/NH2.json",
    ]
    
    print("Mencoba jalankan merge dan simpan sebagai file...")
    sukses_file = mergeJson(
        pathSource=pathSource,
        pathTarget="apps/data/result/rawMerge.json"
    )
    if sukses_file == True:
        print("Selesai! File gabungan berhasil dibuat.\n")

    print("Mencoba jalankan merge tanpa pathTarget...")
    hasil_string = mergeJson(
        pathSource=pathSource
    )
    
    if type(hasil_string) == str:
        print("Selesai! Data tidak dibuat file, melainkan masuk ke memori/variabel.")
        print(f"Bentuk datanya: {hasil_string[:100]}...")

if __name__ == "__main__":
    main()