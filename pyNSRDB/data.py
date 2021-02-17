from typing import Union
import pandas as pd
import zipfile
import io


def create_df(input: Union[io.BytesIO, io.StringIO]):
    header = pd.read_csv(input, nrows=1).iloc[0].to_dict()
    input.seek(0)
    df = pd.read_csv(input, skiprows=[0, 1])
    df.attrs = header
    return df

def process_zip_file(zf: zipfile.ZipFile):
    dfs = []
    attrs = {}
    for file in zf.filelist:
        file_io = io.BytesIO(zf.read(file))
        df = create_df(file_io)
        df.attrs["filename"] = file
        attrs[file.filename] = df.attrs
        dfs.append(df)
    combined = pd.concat(dfs, ignore_index=True)
    combined.attrs = attrs
    return combined   

