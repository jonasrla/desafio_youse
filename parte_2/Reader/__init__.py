from json import loads

def read_file(file_path):
    with open(file_path, 'r') as file:
        data = [loads(line) for line in file]
    return data
