"""
Run in env py-gcs:
conda create --name py-gcs python=3.11 google-cloud-storage ipykernel

References:
https://cloud.google.com/storage/docs/copying-renaming-moving-objects#storage-move-object-python
https://cloud.google.com/storage/docs/listing-objects#storage-list-objects-python
"""


from google.cloud import storage


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None, file_substr=None):
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you specify prefix ='a/', without a delimiter, you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':

        a/1.txt

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:

        a/b/
    """

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    # Note: The call returns a response only when the iterator is consumed.
    # print("Blobs:")
    file_list = []
    for blob in blobs:
        # print(blob.name)
        if file_substr in blob.name:
            file_list.append(blob.name)

    # if delimiter:
    #     print("Prefixes:")
    #     for prefix in blobs.prefixes:
    #         print(prefix)
    
    return file_list
            
    


def move_blob(storage_client, bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    """
    Moves a blob from one bucket to another with a new name.
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The ID of your GCS object
    # blob_name = "your-object-name"
    # The ID of the bucket to move the object to
    # destination_bucket_name = "destination-bucket-name"
    # The ID of your new GCS object (optional)
    # destination_blob_name = "destination-object-name"
    
    """
    # SMW: moved storage_client to an argument, so it isn't generated each time=
    # storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # There is also an `if_source_generation_match` parameter, which is not used in this example.
    destination_generation_match_precondition = 0

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name, if_generation_match=destination_generation_match_precondition,
    )
    source_bucket.delete_blob(blob_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )
    

def move_blob_wrapper(file_old):
    storage_client = storage.Client()
    bucket_name = "amlr-imagery-proc-dev"
    file_new = file_old.replace("/images/", "/images-ffPCG/").replace("/output/", "/")

    # return [bucket_name, file_old, bucket_name, file_new]
    move_blob(storage_client, bucket_name, file_old, bucket_name, file_new)

    


if __name__ == '__main__':
                    # freeze_support()
    ### Code
    storage_client = storage.Client()
    bucket_name    = "amlr-imagery-proc-dev"
    file_prefix    = "gliders/2022/amlr07-20221204/shadowgraph/images/Dir0003"
    file_substr    = "-ffPCG"
    # file_prefix_new = "amlr04 "
    # prefix="sam-tmp/img/2017_*"
    # delimiter=None

    file_list_orig = list_blobs_with_prefix(
        bucket_name, file_prefix, delimiter=None, file_substr=file_substr
    )    
    print(f"there are {len(file_list_orig)} files with {file_substr} " +
          f"in {bucket_name}/{file_prefix}")
    
    # Remove Dir016* paths
    file_list = [i for i in file_list_orig if not ('Dir016' in i)]
    
    print(f"there are {len(file_list)} files with {file_substr} " +
          f"in {bucket_name}/{file_prefix}, after removing 'Dir016'")
 

    print("Moving:")
    # for file_old in file_list:
    #     file_new = file_old.replace("/images/", f"/images{file_substr}/").replace("/output/", "/")
    #     # print(f"{file_old} to {file_new}")
    #     move_blob(storage_client, bucket_name, file_old, bucket_name, file_new)
           
    # file_list_new = []
    # for file_old in file_list:
    #     file_list_new.append(
    #         (bucket_name, file_old, file_old.replace("/images/", f"/images{file_substr}/").replace("/output/", "/"))
    #     )
               
    from datetime import datetime
    print(datetime.now())

    import multiprocessing as mp
    numcores = mp.cpu_count() - 1
    print(f"numcores: {numcores}")
    with mp.Pool(numcores) as pool:
        y = pool.map(move_blob_wrapper, file_list)

    print(datetime.now())
    # print(y)
