from Context import get_context
from Reader import read_file

if __name__ == '__main__':
    context = get_context(file_name)
    raw_data = read_file(file_name)
    new_data = context.transformation(raw_data)
    context.save(new_data)
