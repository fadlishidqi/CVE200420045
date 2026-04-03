from apps.common.renameJsonParam import renameJsonParam

def main():
    PathSource = "apps/data/result/rawMerge.json"
    PathTarget = "apps/data/result/renamed.json"
    
    KeySource = ['time', 'vmachineid', 'vwctid', 'vparam', 'catg', 'nllow', 'nlow', 'nhigh', 'nhhigh']
    KeyTarget = ['t', 'technum', 'wct', 'param', 'category', 'llow', 'low', 'high', 'hhigh']

    print("Memulai ubah nama parameter...")

    sukses = renameJsonParam(
        path_source=PathSource,
        path_target=PathTarget,
        key_source=KeySource,
        key_target=KeyTarget
    )

    if sukses:
        print("Selesai! Parameter JSON berhasil diubah namanya.")
    else:
        print("Proses gagal. Silakan cek log.")

if __name__ == "__main__":
    main()