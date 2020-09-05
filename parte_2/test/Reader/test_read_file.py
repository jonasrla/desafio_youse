from Reader import read_file

def test_read_file():
    object = read_file('test/Reader/sample.json')
    assert len(object) == 2
    assert object[0]['a'] == 1
    assert object[1]['a'] == 2

    assert object[0]['b'] == "Jonas"
    assert object[1]['b'] == "Amaro"

    assert object[0]['c']['bool'] == True
    assert object[1]['c']['bool'] == False
