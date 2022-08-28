#
# MIT License
#
# Copyright (c) 2022 Graz University of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
import boto3
from werkzeug.utils import secure_filename
from .utils import convert_to_snake_case
from . import AbstractStorage, StorageExists, StorageNotExists, StorageNotAllowed

#Enhancement: Upload needs to have proper permissons public/private
#Enhancement: The get_existing_files method needs to handle subfolders

class S3Storage(AbstractStorage):
    def __init__(self, app):
        super(S3Storage, self).__init__(app)
        self.bucket_name = app.config.get("FILEUPLOAD_S3_BUCKET", "flask_fileupload")
        self.acl = app.config.get("FILEUPLOAD_S3_ACL", "public-read")
        self.s3 = boto3.client('s3')
        self.s3_res = boto3.resource('s3')
        response = self.s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        #self.abs_img_path = '{}/{}'.format(self.s3.meta.endpoint_url, self.bucket_name)
        if self.bucket_name not in buckets:
            self.s3.create_bucket(Bucket=self.bucket_name)
        self.bucket = self.s3_res.Bucket(self.bucket_name)

    def get_existing_files(self):
        return [f.key for f in self.bucket.objects.all()]

    def store(self, filename, file_data):
        filename = secure_filename(filename)
        if self.snake_case:
            filename = convert_to_snake_case(filename)
        if self._exists(filename):
            raise StorageExists()
        if self.all_allowed or any(filename.endswith('.' + x) for x in self.allowed):
            self.s3.put_object(Bucket=self.bucket_name,
                               Key=filename,
                               Body=file_data,
                               ACL=self.acl)
        else:
            raise StorageNotAllowed()
        return filename

    def get_base_path(self):
        return '{}/{}/'.format(self.s3.meta.endpoint_url, self.bucket_name)

    def delete(self, filename):
        if not self._exists(filename):
            raise StorageNotExists()
        else:
            self.s3.delete_object(Bucket=self.bucket_name,
                                  Key=filename)

    def _exists(self, filename):
        objs = list(self.bucket.objects.filter(Prefix=filename))
        if len(objs) > 0 and objs[0].key == filename:
            return True
        else:
            return False
