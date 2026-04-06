from apps.common.combineColumn import combineColumn

def main():
    pathSource = "apps/data/result/renamed.json"
    pathTarget = "apps/data/result/combinedParam.json"

    colsToCombine = ['wct', 'technum', 'param']
    targetCol = 'param'
    separator = '_'

    print("Memulai proses penggabungan kolom...")

    sukses = combineColumn(
        pathSource=pathSource,
        pathTarget=pathTarget,
        colsToCombine=colsToCombine,
        targetCol=targetCol,
        separator=separator
    )

    if sukses:
        print(f"Selesai! Kolom berhasil digabungkan menjadi kolom '{targetCol}'.")
    else:
        print("Proses gagal. Silakan cek log.")

if __name__ == "__main__":
    main()