from pytest import fixture


@fixture
def my_fruit():
    return "apple"


@fixture
def fruit_basket(my_fruit):
    return ["banana", my_fruit]


def test_my_fruit_in_basket(my_fruit, fruit_basket):
    assert my_fruit in fruit_basket
