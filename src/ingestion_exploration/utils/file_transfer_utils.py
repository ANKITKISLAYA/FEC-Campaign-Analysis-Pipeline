import gcsfs
import zipfile
import io
import os


def output_path(input_path):
    """
    Converts an input path from the 'ingested' directory to the 'raw' directory.
    """
    return input_path.replace("/ingested/", "/raw/")


def unzip_file(file_path, target_path, fs):
    """
    Unzips the zip file at 'file_path' and writes the extracted files to 'target_path' using gcsfs.
    """
    with fs.open(file_path, "rb") as f:
        zip_bytes = io.BytesIO(f.read())
        with zipfile.ZipFile(zip_bytes, "r") as zip_ref:
            for zip_info in zip_ref.infolist():
                if zip_info.is_dir():
                    continue  # Skip folders

                extracted_file = zip_ref.open(zip_info)
                file_name = os.path.basename(zip_info.filename)

                full_target_path = os.path.join(target_path, file_name).replace(
                    "\\", "/"
                )
                directory = os.path.dirname(full_target_path)

                if not fs.exists(directory):
                    fs.mkdirs(directory)

                with fs.open(full_target_path, "wb") as out_file:
                    print(f"Copying {file_path} to {full_target_path}...")
                    out_file.write(extracted_file.read())


def copy_to_raw(ingested_parent_path):
    """
    Copies all files from 'ingested_parent_path' to a corresponding 'raw' directory.
    If a file is a ZIP, it gets unzipped in the target location.
    """
    fs = gcsfs.GCSFileSystem()
    child_dirs = fs.ls(ingested_parent_path)[1:]  # Skip parent dir

    for directory in child_dirs:
        target_path = output_path(directory)
        files = fs.ls(directory)

        for file_path in files:
            if file_path.endswith(".zip"):
                unzip_file(file_path, target_path, fs)
            else:
                if not fs.exists(target_path):
                    fs.mkdirs(target_path)
                print(f"Copying {file_path} to {target_path}...")
                fs.copy(file_path, target_path)


if __name__ == "__main__":
    gcs_path = "gs://dataproc-staging-us-central1-784600309852-sdhxiysx/notebooks/jupyter/FEC Project/data/ingested/2019-2020/"
    copy_to_raw(gcs_path)
