import os
import click
import boto3

@click.command()
@click.option(
    '--bucket',
    type=str, 
    help='The bucket to destroy (delete all objects, delete all object versions, delete bucket).'
)
@click.option(
    '--objects-file', 
    type=str, 
    default=None,
    help='Path to a text-file containing a list of objects to delete (one per line).'
)
@click.option(
    '--destroy-bucket',
    is_flag=True,
    help='Should the script attempt to destroy the bucket after deleting the requested objects?'
)
@click.option(
    '--quiet',
    '--q',
    is_flag=True,
    help='Should the script run quietly or display output? (Default is to display output)'
)
@click.option(
    '--no-dryrun',
    is_flag=True,
    help='Should the script do a dryrun or a normal run? (print objects to be deleted but skip actual deletion for QC) \
      A dryrun will never wipe a bucket.'
)


def destroy_bucket_contents(bucket, objects_file, destroy_bucket, quiet, no_dryrun):
    """
    Cleans the S3 bucket by deleting all objects in the objects_file from the provided S3 bucket

    Arguments
    ----------
    bucket : str
        The name of the S3 bucket
    
    objects_file : str/None
        The path to the file containing the objects we want to delete (one per line, bucket not included in key).
        If this is None, all objects and object versions will be deleted from the bucket.

    destroy_bucket : bool
        Should the script attempt to destroy the bucket after deleting the requested objects?'
    
    quiet : bool
        Should the script run quietly or display output?
    
    no_dryrun : bool
        Should the script do a dryrun? (print objects to be deleted but skip actual deletion for QC).
    """
    verbose            = (not quiet)
    dryrun             = (not no_dryrun)
    objects_to_destroy = parse_objects_file(objects_file)

    if verbose:
        if objects_to_destroy is None or len(objects_to_destroy) == 0:
            print(f'No objects list found at provided filepath "{objects_file}", selecting ALL objects in bucket.')
        else:
            print(f'Running script on objects provided in objects file at "{objects_file}".')

    delete_objects(bucket, objects_to_destroy, dryrun=dryrun, verbose=verbose)
    delete_versions(bucket, objects_to_destroy, dryrun=dryrun, verbose=verbose)

    if destroy_bucket:
        result = destroy_bucket_if_empty(bucket, dryrun=dryrun, verbose=verbose)


def parse_objects_file(filepath):
    """
    Fetches the objects list from the provided filepath. Returns None if filepath can't be found.
    """
    objects = None
    if filepath is not None and os.path.exists(filepath) and os.path.isfile(filepath):
        with open(filepath, 'r+') as in_file:
            objects = [line.rstrip() for line in in_file.readlines()]

    return objects


def delete_objects(bucket_name, objects=None, dryrun=True, verbose=True):
    """
    Delete objects in the provided bucket. 

    Arguments
    ----------
    bucket_name : str
        The name of the S3 bucket
    
    objects : None/list(str)
        The objects we want to delete. If this argument is None or an empty list,
        then ALL objects in the bucket will be deleted.
    
    verbose : bool
        Should the script display output?
    """
    session = boto3.Session(profile_name='default')
    s3      = session.resource('s3')
    bucket  = s3.Bucket(bucket_name)
    if objects:
        for obj_summary in bucket.objects.all():
            key = obj_summary.key
            if key in objects:
                if verbose:
                    print(f'(dryrun) s3://{bucket_name}/{key}' if dryrun else f's3://{bucket_name}/{key}')
                if not dryrun:
                    bucket.Object(key).delete()
    else:
        for obj_summary in bucket.objects.all():
            key = obj_summary.key
            if verbose:
                print(f'(dryrun) s3://{bucket_name}/{key}' if dryrun else f's3://{bucket_name}/{key}')
            if not dryrun:
                bucket.Object(key).delete()



def delete_versions(bucket_name, objects=None, dryrun=True, verbose=True):
    """
    Delete all versions of the objects in the provided bucket. 

    Arguments
    ----------
    bucket_name : str
        The name of the S3 bucket
    
    objects : None/list(str)
        The objects whose versions we want to delete. If this argument is None or an empty list,
        then ALL object versions in the bucket will be deleted.
    
    verbose : bool
        Should the script display output?
    """
    session = boto3.Session(profile_name='default')
    bucket = session.resource('s3').Bucket(bucket_name)
    for version in bucket.object_versions.all():
        key = version.object_key
        should_delete_key = (key in objects) if objects else True
        if should_delete_key:
            if verbose:
                s = f' s3://{bucket_name}/{key}'
                print(s if not dryrun else f' (dryrun){s}')
            if not dryrun:
                version.delete()


def bucket_is_empty(bucket_name):
    """
    Is the provided S3 bucket empty?
    """
    return len(list(boto3.resource('s3').Bucket(bucket_name).objects.all())) == 0


def destroy_bucket_if_empty(bucket_name, dryrun=True, verbose=True):
    """
    Delete the provided S3 bucket if it's empty, otherwise do nothing.
    """
    if dryrun and verbose:
        print('Script performing dryrun, bucket deletion is skipped.')

    if (not dryrun) and verbose:
        print(f'Script will now attempt to destroy S3 bucket s3://{bucket_name}...')

    if bucket_is_empty(bucket_name):
        if verbose:
            if dryrun:
                print(f'Bucket s3://{bucket_name} is empty but dryrun = True, skipping bucket deletion.')
            else:
                print(f'Bucket s3://{bucket_name} is empty, deleting bucket...')
        if not dryrun:
            boto3.resource('s3').Bucket(bucket_name).delete()
        return 0
    else:
        if verbose and not dryrun:
            print(f'Bucket s3://{bucket_name} is NOT empty, skipping bucket deletion...')
        return 1


if __name__ == '__main__':
    clean_bucket()
