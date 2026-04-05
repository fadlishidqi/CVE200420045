from apps.common.convertTime import convertTime

def main():
    pathSource = "apps/data/result/renamed.json" 
    pathTarget = "apps/data/result/convertedTime.json"
    keySource = ["t", "dcrea"] 
    formatTime = "%d-%m-%Y %H:%M:%S"

    print("Memulai proses konversi format waktu...")

    sukses = convertTime(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource,
        formatTime=formatTime
    )

    if sukses:
        print("Proses konversi waktu berhasil disimpan.")
    else:
        print("Proses konversi waktu gagal.")

if __name__ == "__main__":
    main()