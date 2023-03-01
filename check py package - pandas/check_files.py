import pandas as pd
import boto3
from io import StringIO

class S3Client():
    """Class to create boto3 s3 client object to get and put files
    """

    def __init__(self, s3_bucket_name):
        """ S3Client __init__ method

        Args:
            s3_bucket_name (string): s3 bucket name to create boto3 s3 client object
        """
        self.bucket_name = s3_bucket_name
        self.s3 = boto3.client('s3')

    def get_file(self, filepath):
        """method to get file from s3 bucket

        Args:
            filepath (string): filepath of a s3 file

        Returns:
            obj: Returns file object(body)
        """
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=filepath)
        return obj["Body"]

    def get_csv_file(self, filepath, separator=",", cols=None):
        """method to get csv file from s3 bucket

        Args:
            filepath (string): csv filepath in s3 bucket
            separator (str, optional): string to separate each column . Defaults to ",".
            cols (list, optional): list of columns to get from csv file from s3. Defaults to None.

        Returns:
            DataFrame: Returns the fetched Dataframe
        """
        if filepath.split('.')[-1] == 'csv':
            obj = self.s3.get_object(Bucket=self.bucket_name, Key=filepath)
            if cols:
                df = pd.read_csv(obj["Body"], sep=separator, usecols=cols)
            else:
                df = pd.read_csv(obj["Body"], sep=separator)
            return df
        return "not a csv file"

    def put_csv_file(self, df_data, out_filepath):
        """upload csv file to s3

        Args:
            df_data (DataFrame): Pandas DataFrame
            out_filepath (string): filepath of the csv file to save in s3 bucket
        """
        csv_buf = StringIO()
        df_data.to_csv(csv_buf, index=False)
        csv_buf.seek(0)
        self.s3.put_object(Bucket=self.bucket_name, Body=csv_buf.getvalue(), Key=out_filepath)

    def upload_file(self, file_data, out_filepath):
        """Method to upload file to s3

        Args:
            file_data (obj): file obj
            out_filepath (string): filepath to save in s3 bucket
        """
        self.s3.put_object(Bucket=self.bucket_name, Body=file_data, Key=out_filepath)

    def get_dir_filenames(self, dirname):
        """Return list of file in a s3 Directory

        Args:
            dirname (string): name of the directory

        Returns:
            list: list of dict that contains filename and filesize

            eg: [
                    {'filename': 'test/', 'filesize': 0},
                    {'filename': 'test/ff.txt', 'filesize': 0},
                    {'filename': 'test/test.csv', 'filesize': 6}
                ]

        """
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=dirname)
        files = response.get("Contents")
        file_list = []
        for file in files:
            file_list.append({"filename":file['Key'], "filesize": file['Size']})
        return file_list

source_S3Client = S3Client("aws-glue-ravi-demo1")
print(source_S3Client)
a = source_S3Client.get_dir_filenames("")
print("printing a >>>>")
print(a)


