def filter_data(data, criteria):
    return [item for item in data if all(item[key] == value for key, value in criteria.items())]

def test_filter_data():
    data = [
        {'name': 'Alice', 'age': 30},
        {'name': 'Bob', 'age': 25},
        {'name': 'Charlie', 'age': 30}
    ]
    criteria = {'age': 30}
    filtered = filter_data(data, criteria)
    assert len(filtered) == 2
    assert filtered[0]['name'] == 'Alice'
    assert filtered[1]['name'] == 'Charlie'