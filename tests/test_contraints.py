from aqneodriver.constraints import iter_constraints


def test_iter_constraints():
    for c in iter_constraints():
        print(c)
