import os
import json
import click


@click.command()
@click.option('--string', help='The strings to search for')
@click.option('--path',   help='The directory path to search.')
@click.option('--ext',    help='Search through all files ending with the provided extensions.')

def get_filepaths_containing_string(string, path, ext, case_sensitive=False):
    """
    Given the provided strings, search the files in the directory path for instances
    of those strings. Can filter based on file extension/suffix, and can run case-sensitive
    or insensitive searches.

    Args
    ----------
        string : str/iterable(str)
            A string or iterable object containing strings

        path : str
            The directory to search within

        ext : str/iterable(str)
            Only search through files with this extension
        
        case_sensitive : bool
            Is the search case-sensitive?

    Returns
    ----------
        map_string_to_filepaths : dict(str --> list)
            A dictionary mapping each search string to a list of files in which
            it found a match.
    """
    # if only one string/extension was provided, squeeze it into a tuple
    if type(string) is str:
        string = (string, )
    if type(ext) is str:
        ext = (ext, )

    map_string_to_filepaths = {s:list() for s in string}

    for root, _, files in os.walk(path):
        for file in files:
            filepath = os.path.join(root, file)
            # match filepaths
            if any(filepath.endswith(x) for x in ext):
                with open(filepath, 'r+') as in_file:
                    lines = in_file.readlines()
                # search through file contents
                for line in lines:
                    for s in string:
                        if case_sensitive:
                            parsed_line = line
                            parsed_s    = s
                        else:
                            parsed_line = line.lower()
                            parsed_s    = s.lower()

                        if parsed_s in parsed_line and filepath not in map_string_to_filepaths:
                            map_string_to_filepaths[s].append(filepath)

    return map_string_to_filepaths


if __name__ == '__main__':
    to_find        = ['word']
    path           = '/mnt/c/Users/astenger/Documents/Projects/'
    ext            = ['.js', '.py']
    case_sensitive = False

    string_to_filepaths = get_filepaths_containing_string(to_find, path, ext, case_sensitive=case_sensitive)
    print(json.dumps(string_to_filepaths, indent=4, sort_keys=True))
