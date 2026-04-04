from apps.common.compareValue import compareValue

def main():
    PathSource = "apps/data/result/replacedData.json"
    PathTarget = "apps/data/result/comparedValue.json"

    KeySource = "nvalue"
    KeyResult = "threshold"
    DefaultValue = 0

    KeyComparation = ["llow", "hhigh", "low", "high"] 
    Operators = ["<", ">", "<", ">"]
    ResultComparation = [-2, 2, -1, 1]

    print("Memulai proses komparasi nilai...")

    sukses = compareValue(
        path_source=PathSource,
        path_target=PathTarget,
        key_source=KeySource,
        key_compare=KeyComparation,
        operators=Operators,
        result_compare=ResultComparation,
        default_value=DefaultValue,
        key_result=KeyResult
    )

    if sukses:
        print("Proses komparasi berhasil disimpan.")
    else:
        print("Proses komparasi gagal.")

if __name__ == "__main__":
    main()