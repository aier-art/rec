#!/usr/bin/env python

SCORE_BASE = 50
平均分 = 20000


def calculate_score(SCORE_BASE, click_fav, seen_click, seen, click, fav):
  score = ((((100 * click + fav * click_fav) * seen_click)) +
           SCORE_BASE * 平均分) / (seen + SCORE_BASE)
  return score


CLICK_FAV = 550  # 3 * 100
SEEN_CLICK = 2148  # 20 * 100

seen = 1
click = 0
fav = 0
test_score = calculate_score(SCORE_BASE, CLICK_FAV, SEEN_CLICK, seen, click,
                             fav)

print(test_score)
