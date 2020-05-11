import pytest
import pandas as pd
import subprocess
from pandas._testing import assert_frame_equal
from nempy import historical_inputs


def test_download_to_df():
    server = subprocess.Popen('python -m http.server 8080 --bind 127.0.0.1')
    output = historical_inputs.download_to_df(url='http://127.0.0.1:8080/test_files/{table}_{year}{month}01.zip',
                                              table_name='table_one', year=2020, month=1)
    expected = pd.DataFrame({
        'a': [1, 2, 3],
        'b': [4, 5, 6]
    })
    assert_frame_equal(output, expected)
    output = historical_inputs.download_to_df(url='http://127.0.0.1:8080/test_files/{table}_{year}{month}01.zip',
                                              table_name='table_two', year=2019, month=2)
    expected = pd.DataFrame({
        'c': [1, 2, 3],
        'd': [4, 5, 6]
    })
    assert_frame_equal(output, expected)
    server.terminate()


def test_download_to_df_raises_on_missing_data():
    server = subprocess.Popen('python -m http.server 8080 --bind 127.0.0.1')
    with pytest.raises(historical_inputs.MissingData):
        output = historical_inputs.download_to_df(url='http://127.0.0.1:8080/test_files/{table}_{year}{month}01.zip',
                                                  table_name='table_one', year=2020, month=3)
    server.terminate()


def test_download_to_df_raises_on_url_down():
    with pytest.raises(historical_inputs.MissingData):
        output = historical_inputs.download_to_df(url='http://127.0.0.1:8080/test_files/{table}_{year}{month}01.zip',
                                                  table_name='table_one', year=2020, month=3)

