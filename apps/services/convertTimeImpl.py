from apps.common.convertTime import convertTime

def main():
    PathSource = "apps/data/result/renamed.json" 
    PathTarget = "apps/data/result/convertedTime.json"
    KeySource = ["t", "dcrea"] 
    FormatTime = "%d-%m-%Y %H:%M:%S"

    print("Memulai proses konversi format waktu...")

    sukses = convertTime(
        pathSource=PathSource,
        pathTarget=PathTarget,
        keySource=KeySource,
        formatTime=FormatTime
    )

    if sukses:
        print("Proses konversi waktu berhasil disimpan.")
    else:
        print("Proses konversi waktu gagal.")

if __name__ == "__main__":
    main()