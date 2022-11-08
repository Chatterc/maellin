import unittest

import pandas as pd
from maellin.common.workflows import Pipeline
from maellin.common.tasks import Task
from pandas.testing import assert_frame_equal


def load_ssm(zone: str) -> pd.DataFrame:
    fn = f"maellin/data/example_1/solar_{zone}.csv"
    df = pd.read_csv(fn, index_col=0)
    return df


def prep_ssm(df: pd.DataFrame) -> pd.DataFrame:
    df['date'] = pd.to_datetime(df.date.astype(str).str[:-6])
    # idebug()
    return df


def load_load(zone: str, loadtype: str) -> pd.DataFrame:
    zone_loadtype = f"{zone}_{loadtype}"
    fn = "maellin/data/example_1/inputdata.csv"
    df = pd.read_csv(fn, index_col=0)
    df = df[df.zone_loadtype == zone_loadtype]
    assert len(df) > 0
    return df


def prep_load(df: pd.DataFrame) -> pd.DataFrame:
    dfp = df.copy()
    dfp['date'] = pd.to_datetime(dfp.hourBeginning.astype(str).str[:-6])
    return dfp


def merge(load: pd.DataFrame, ssm: pd.DataFrame) -> pd.DataFrame:
    df3 = pd.merge(load, ssm, on='date', how='inner')
    return df3


def head(df: pd.DataFrame, num: int) -> pd.DataFrame:
    return df.head(num)


def tail(df: pd.DataFrame, num: int) -> pd.DataFrame:
    return df.tail(num)


def scenario():

    loadssm = Task(load_ssm)
    prepssm = Task(prep_ssm)
    loadload = Task(load_load)
    prepload = Task(prep_load)
    merget = Task(merge)
    headt = Task(head)
    tailt = Task(tail)

    steps = [
        (loadssm, {'zone': 'CT'}, None),
        (prepssm, {}, [loadssm]),
        (loadload, {'zone': 'CT', 'loadtype': 'CI'}, None),
        (prepload, {}, [loadload]),
        (merget, {}, [prepload, prepssm]),
        (headt, {'num': 10}, [merget]),
        (tailt, {'num': 5}, [merget]),
    ]

    pipeline = Pipeline(steps)
    pipeline.run()
    return pipeline


class TestTuringSceanrio(unittest.TestCase):

    def setUp(self):
        self.results_head = pd.read_csv('maellin/data/example_1/head.csv', index_col=0)
        self.results_head['date'] = pd.to_datetime(self.results_head['date'])

        self.results_tail = pd.read_csv('maellin/data/example_1/tail.csv', index_col=0)
        self.results_tail['date'] = pd.to_datetime(self.results_tail['date'])

    def test_scenario_1(self):
        try:
            results = scenario()
            status = "SUCCESS"
        except Exception as e:
            status = "FAILED"
            raise e

        self.assertTrue(status == "SUCCESS")
        assert_frame_equal(results.steps[-1].result, self.results_tail)
        assert_frame_equal(results.steps[-2].result, self.results_head)


if __name__ == '__main__':
    unittest.main()
