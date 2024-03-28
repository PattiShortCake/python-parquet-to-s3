# This is a sample Python script.
import os
import string

import pandas as pd


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def to_parquet(s3_uri: string):
    # create dictionary
    data = [['Homer', 'Simpson', 34], ['Bart', 'Simpson', 10]]

    # create a DataFrame
    df = pd.DataFrame(data, columns=['first_name', 'last_name', 'age'])

    # write the DataFrame to a Parquet file
    print(f'Writing to {s3_uri}')
    df.to_parquet(s3_uri, compression='gzip')


def from_parquet(s3_uri: string):
    # Load DataFrame from S3
    print(f'Reading from {s3_uri}')
    df = pd.read_parquet(s3_uri)

    # print DataFrame
    print(df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    _s3_uri = os.environ['S3_PREFIX'] + 'test.parquet.gzip'
    to_parquet(_s3_uri)
    from_parquet(_s3_uri)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
