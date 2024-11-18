import pytest
import pandas as pd
import subprocess
import shlex
import time
import platform
from pandas._testing import assert_frame_equal
from nempy.historical_inputs import mms_db


def test_download_to_df():
    if platform.system() == 'Windows':
        server = subprocess.Popen(shlex.split('python -m http.server 8888 --bind 127.0.0.1'))
    else:
        server = subprocess.Popen(shlex.split('python3 -m http.server 8888 --bind 127.0.0.1'))
    try:
        time.sleep(1)
        output_1 = mms_db._download_to_df(
            url='http://127.0.0.1:8888/tests/test_files/{table}_{year}{month}01.zip',
            table_name='table_one', year=2020, month=1)
        expected_1 = pd.DataFrame({
            'a': [1, 2],
            'b': [4, 5]
        })

        output_2 = mms_db._download_to_df(
            url='http://127.0.0.1:8888/tests/test_files/{table}_{year}{month}01.zip',
            table_name='table_two', year=2019, month=2)
        expected_2 = pd.DataFrame({
            'c': [1, 2],
            'd': [4, 5]
        })
    finally:
        server.terminate()
    assert_frame_equal(output_1, expected_1)
    assert_frame_equal(output_2, expected_2)


def test_download_to_df_raises_on_missing_data():
    if platform.system() == 'Windows':
        server = subprocess.Popen(shlex.split('python -m http.server 8888 --bind 127.0.0.1'))
    else:
        server = subprocess.Popen(shlex.split('python3 -m http.server 8888 --bind 127.0.0.1'))
    time.sleep(1)
    try:
        with pytest.raises(mms_db._MissingData) as exc_info:
            mms_db._download_to_df(
                url='http://127.0.0.1:8888/tests/test_files/{table}_{year}{month}01.zip',
                table_name='table_two', year=2019, month=3)
    finally:
        server.terminate()


def test_download_to_df_raises_on_data_not_on_nemweb():
    with pytest.raises(mms_db._MissingData):
        url = ('http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/' +
               'MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip')
        mms_db._download_to_df(
            url=url, table_name='DISPATCHREGIONSUM', year=2050, month=3)


