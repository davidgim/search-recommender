from db.postgres_client import calculate_score
from api.main import blend_scores

def test_score_all_ones():
    score = calculate_score(1, 1, 1)
    assert score == 14

def test_score_all_zeros():
    score = calculate_score(0, 0, 0)
    assert score == 0

def test_score_only_purchases():
    score = calculate_score(0, 0, 10)
    assert score == 100

def test_blend_equal():
    blended = blend_scores(10, 10)
    assert blended == 10

def test_blend_no_personal():
    blended = blend_scores(10, 0)
    assert blended == 7.0