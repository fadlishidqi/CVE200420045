from apps.common.renameJsonParam import renameJsonParam

def main():
    pathSource = "apps/data/result/rawMerge.json"
    pathTarget = "apps/data/result/renamed.json"

    keySource = ['time', 'vmachineid', 'vwctid', 'vparam', 'catg', 'nllow', 'nlow', 'nhigh', 'nhhigh']
    keyTarget = ['t', 'technum', 'wct', 'param', 'category', 'llow', 'low', 'high', 'hhigh']

    print("Memulai ubah nama parameter...")

    sukses = renameJsonParam(
        pathSource=pathSource,
        pathTarget=pathTarget,
        keySource=keySource,
        keyTarget=keyTarget
    )

    if sukses:
        print("Selesai! Parameter JSON berhasil diubah namanya.")
    else:
        print("Proses gagal. Silakan cek log.")

if __name__ == "__main__":
    main()