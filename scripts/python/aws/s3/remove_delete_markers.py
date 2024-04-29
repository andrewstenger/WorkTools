import click
import ast
import subprocess


@click.command()
@click.option('--bucket', '--b', type=str,     multiple=False)
@click.option('--prefix', '--p', type=str,     multiple=True)
@click.option('--quiet',  '--q', is_flag=True, multiple=False)
@click.option('--no-dryrun',     is_flag=True, multiple=False)

def remove_delete_markers(bucket, prefix, quiet, no_dryrun):
    """
    Removes all the delete markers for the objects in the provided bucket

    Arguments
    ----------
    objects_to_delete : list
        A list of object dictionaries, each containing the object key and version ID.
    
    Returns
    ----------
    all_output : list
        A list of all the output messages displayed by the s3api as each "delete-object" command is run.
    """
    all_output        = list()
    prefixes          = prefix if type(prefix) == list else [prefix]
    verbose           = (not quiet)
    dryrun            = (not no_dryrun)
    objects_to_delete = find_objects_with_delete_markers(bucket, prefixes)

    for obj_dict in objects_to_delete:
        key = obj_dict['Key']
        version_id = obj_dict['VersionId']

        if verbose:
            if not dryrun:
                msg = f' s3://{bucket}/{key},  version = {version_id} will be deleted'
            else:
                msg = f' (dryrun) s3://{bucket}/{key},  version = {version_id} will be deleted'
            print(msg)

        if not dryrun:
            cmd = [f'aws s3api delete-object --bucket {bucket} --key {key} --version-id {version_id}']
            cmd = cmd_str.split(' ')
            output = subprocess.run(cmd, capture_output=True).stdout.decode('utf-8')
            all_output.append(output)

    return all_output


def find_objects_with_delete_markers(bucket, prefix):
    """
    Search the provided bucket for objects with Delete Markers that are not non-file objects (i.e. s3://bucket/path/).
    Uses the AWS s3api.

    Arguments
    ----------
    bucket : str
        The name of the S3 bucket containing the objects with delete markers.

    prefix : list/str
        A partial prefix to search for objects with Delete Markers. Can be list or str.
    
    Returns
    ----------
    filtered_objects : list
        A list of all object dictionaries, each corresponds to an object with a Delete Marker.
    """
    filtered_objects = list()
    prefixes = prefix if type(prefix) == list else [prefix]
    for prefix in prefixes:
        cmd_str = f'aws s3api list-object-versions --bucket {bucket} --prefix {prefix} --query DeleteMarkers[?IsLatest==`true`]'
        cmd = cmd_str.split(' ')

        output = subprocess.run(cmd, capture_output=True).stdout.decode('utf-8')

        # make the output parsable by Python
        parsable_output = output.replace('true', 'True')

        # literal_eval is okay since we trust the "parsable_output" string that comes from s3api
        objects = ast.literal_eval(parsable_output)

        # filter out objects with non-file prefixes
        for obj_dict in objects:
            key = obj_dict['Key']
            if not key.endswith('/'):
                filtered_objects.append(obj_dict)

    return filtered_objects


if __name__ == '__main__':
    output = remove_delete_markers()
