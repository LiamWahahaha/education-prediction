import os
import sys
'''
    For the given path, get the List of all json files in the directory tree
'''


def get_list_of_files(dir_name):
    # create a list of file and sub directories
    # names in the given directory
    entries = os.listdir(dir_name)
    files = list()

    # Iterate over all the entries
    for entry in entries:
        full_path = os.path.join(dir_name, entry)
        if os.path.isdir(full_path):
            files = files + getListOfFiles(full_path)
        else:
            files.append(full_path)

    return files


def main():
    dir_name = None
    file_name = 'twitter_file_list.txt'

    try:
        folder = 'twitter/'
        dir_name = sys.argv[1]
        file_name = sys.argv[2]
    except:
        print('Write files to default directory')

    # Get the list of all files in directory tree at given path
    files = list()
    for (dir_path, dir_names, file_names) in os.walk(dir_name):
        files += [
            os.path.join(dir_path, file) for file in file_names
            if file[-5:] == '.json'
        ]

    # Write the list of json files to twitter_file_list.txt
    f = open(folder + file_name, "w")
    f.write(','.join(files))
    f.close()


if __name__ == '__main__':
    main()
