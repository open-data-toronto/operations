'''Functions yielding rows (as dicts) for custom read jobs'''

def test_reader():
    data = [
        {
            "row1": 1,
            "row2": "text",
            "row3": 2.11
        },
        {
            "row1": 1,
            "row2": "text",
            "row3": 3.1
        },
        {
            "row1": 2,
            "row2": "text",
            "row3": 3.1
        },
        {
            "row1": 1,
            "row2": "text",
            "row3": 3.1
        },
    ]

    for i in data:
        yield i

