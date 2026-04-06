from apps.common.renameJsonParam import renameJsonParam

def main():
    print("=== Memulai Uji Coba Rename JSON Parameter ===")

    # =====================================================================
    # SKENARIO 1: Input berupa STRING JSON MURNI (In-Memory Processing)
    # =====================================================================
    print("\n[Skenario 1] Membaca dari String JSON (Tanpa File)")
    
    # Ini adalah contoh string JSON langsung (bisa dari hasil return fungsi lain)
    string_json_input = '[{"time":"2026-05-01 08:28:59", "vmachineid":"IMM04", "vparam":"CUSHION-POSITION", "value": 17.5}]'

    # Kita ubah time -> t, dan vmachineid -> technum
    keySource = ['time', 'vmachineid']
    keyTarget = ['t', 'technum']

    # Panggil fungsi tanpa pathTarget, sehingga outputnya juga string
    hasil_string = renameJsonParam(
        pathSource=string_json_input, 
        keySource=keySource,
        keyTarget=keyTarget
    )

    if isinstance(hasil_string, str):
        print("-> SUKSES! Data berhasil di-rename di dalam memori.")
        print(f"-> Bentuk datanya sekarang:\n{hasil_string}")
    else:
        print("-> GAGAL pada Skenario 1.")


    # =====================================================================
    # SKENARIO 2: Input berupa ALAMAT FILE (Disk I/O)
    # =====================================================================
    print("\n[Skenario 2] Membaca dari Alamat File dan Menyimpan ke File Baru")
    
    # Pastikan Anda memiliki file ini atau ubah path-nya ke file yang ada
    path_file_sumber = "apps/data/result/rawMerge.json" 
    path_file_target = "apps/data/result/renamed.json"

    hasil_file = renameJsonParam(
        pathSource=path_file_sumber, # Input berupa String Alamat File
        pathTarget=path_file_target, # Output disuruh simpan ke File
        keySource=keySource,
        keyTarget=keyTarget
    )

    if hasil_file == True:
        print(f"-> SUKSES! File berhasil dibaca, di-rename, dan disimpan ke: {path_file_target}")
    else:
        print(f"-> Skenario 2 di-skip/gagal (mungkin karena file {path_file_sumber} belum ada).")


if __name__ == "__main__":
    main()