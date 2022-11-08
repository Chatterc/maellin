from typing import List, Union
import unittest

import pandas as pd
from cne.pipelines.pipeline import Activity, Pipeline
from cne.pipelines.tasks import Task
from pandas.testing import assert_frame_equal


# ====================== PARAMETERS ====================== #
dimA = 'cne/data/example_0/dim_a.csv'
dimB = 'cne/data/example_0/dim_b.csv'
dimC = 'cne/data/example_0/dim_c.csv'
factTbl = 'cne/data/example_0/fact_tbl.csv'


# ====================== FUNCTIONS ====================== #
def select_target_cols(df: pd.DataFrame, target_schema: List) -> pd.DataFrame:
    df = df[target_schema]
    return df


def create_composite_key(df: pd.DataFrame) -> pd.DataFrame:
    df['CompositeKey'] = df.CNo.map(str) + "-" \
        + df.BNo.map(str) + "-" \
        + df.ANo.map(str)
    return df


def read_table(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


def join_dataframes(A: pd.DataFrame, B: pd.DataFrame, on: Union[str, List], how: str) -> pd.DataFrame:
    df = A.merge(B, on=on, how=how)
    return df


def sink_output(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path)
    return


def drop_duplicates(df: pd.DataFrame, columns=List[str]) -> pd.DataFrame:
    df = df.drop_duplicates(subset=columns)
    return df


# ====================== TASKS ====================== #
read = Task(read_table)
select = Task(select_target_cols)
join = Task(join_dataframes)
joinCust = Task(join_dataframes)
joinBG = Task(join_dataframes)
composite = Task(create_composite_key)
sink = Task(sink_output)


def scenario():
    # Subpipeline to to join Cust Features
    custPipe = Pipeline(
        steps=[
            Activity(
                task=(read_table),
                kwargs={
                    'path': dimC},
                name='read_cust'),
            Activity(
                task=(select_target_cols),
                kwargs={
                    'target_schema': [
                        'CId',
                        'CNo']},
                depends_on=['read_cust'],
                name='select_cust_cols'),
        ])

    # Subpipeline to to join BillGroup Features
    bgPipe = Pipeline(
        steps=[
            Activity(
                task=(read_table), kwargs={
                    'path': dimB}, name='read_bg'),
            Activity(
                task=(select_target_cols), kwargs={
                    'target_schema': [
                        'BId', 'BNo', 'CId']},
                depends_on=['read_bg'],
                name='select_bg_cols'), (custPipe, {}, None),
            (joinCust, {
                'on': 'CId', 'how': 'inner'}, [
                'select_cust_cols', 'select_bg_cols'])])

    # Subpipeline to to join Account Features
    acctPipe = Pipeline(
        steps=[
            Activity(
                task=(read_table), kwargs={
                    'path': dimA}, name="read_acct"),
            Activity(
                task=(select_target_cols), kwargs={
                    'target_schema': [
                        'AId', 'ANo', 'BId']},
                depends_on=['read_acct'],
                name="select_acct_cols"),
            (bgPipe, {}, None),
            (joinBG, {
                'on': 'BId', 'how': 'inner'}, [
                "select_acct_cols", bgPipe]), (composite, {}, [joinBG])])

    exceptPipe = Pipeline(
        steps=[
            Activity(task=drop_duplicates,
                     name='dropDups',
                     depends_on=['read_aging'],
                     kwargs={'columns': 'AId'})
        ]
    )

    # Main Pipeline for Curating Dimension table and writes data to a sink
    pipeline = Pipeline(
        steps=[
            Activity(task=(read_table), kwargs={'path': factTbl}, name='read_aging'),
            (acctPipe, {}, None),
            (exceptPipe, {}, ['read_aging']),
            (join, {'on': 'AId', 'how': 'inner'}, [acctPipe, 'read_aging']),
            # (sink, {'path':'.workingfiles/output2.csv'}, [join])
        ],
        gc_enabled=False
    )
    pipeline.run()
    return pipeline


class TestTuringSceanrio(unittest.TestCase):

    def setUp(self):
        self.results = pd.read_csv('cne/data/example_0/correct_result.csv')

    def test_scenario_1(self):
        try:
            results = scenario()
            status = "SUCCESS"
        except Exception as e:
            status = "FAILED"
            raise e

        self.assertTrue(status == "SUCCESS")
        assert_frame_equal(results.steps[-1].result, self.results)


if __name__ == '__main__':
    unittest.main()
