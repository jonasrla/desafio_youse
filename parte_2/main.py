from argparse import ArgumentParser

from Context import get_context
from Reader import read_file

if __name__ == '__main__':
    parser = ArgumentParser(description='Process events')
    parser.add_argument('file_path', type=str)
    file_path = parser.parse_args().file_path

    context = get_context(file_path)
    raw_data = read_file(file_path)
    new_data = context.transformation(raw_data)
    context.save(new_data)
