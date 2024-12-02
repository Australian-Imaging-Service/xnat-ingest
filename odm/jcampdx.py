from brukerapi.dataset import Dataset, JCAMPDX


def load_jcampdx(_file):
    result = {}
    for k, v in JCAMPDX(_file).to_dict().items():
        try:
            result[k] = v["value"]
        except Exception as e:
            print(f"Error loading JCAMPDX file {_file}: {e}")
            continue
    return result
