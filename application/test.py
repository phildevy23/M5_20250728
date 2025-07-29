import unittest
import pandas as pd
import pandas.testing as pdt
from pandas import Timestamp
from input import Convert_to_Date
from input import Convert_to_Int
from input import Capture_Invalid_Dates
from input import TitleCleaner

class TestCalculations(unittest.TestCase):

    def test_int_convert(self):
        data = {'id':[1.0,2.0,3.1],'date':["20/04/2023","25/04/2023","20/01/2023"]}
        source = pd.DataFrame(data)
        Convert_to_Int(source,'id')

        answer = [1,2,3]
        for i, expected in enumerate(answer):
            self.assertEqual(source['id'][i],expected,'Convert to INT failed')

    def test_date_convert(self):
        data = {'id':[1.0,2.0,3.1],'date':["20/04/2023","25/04/2023","20/01/2023"]}
        source = pd.DataFrame(data)
        Convert_to_Date(source,'date')
        answer = [pd.Timestamp('2023-04-20 00:00:00'),pd.Timestamp('2023-04-25 00:00:00'),pd.Timestamp('2023-01-20 00:00:00')]
        for i, expected in enumerate(answer):
            self.assertEqual (source['date'][i],expected,' Convert to Date Failed')

    def test_openai_title(self):
        data = 'Hari Potter and the half blud prince'
        cleaner = TitleCleaner()
        answer = cleaner.clean_book_title(data)
        self.assertEqual(answer,'Harry Potter and the Half-Blood Prince')

    def test_capture_invalid_date(self):
        data = {'id':[1,2,3],'date':[pd.Timestamp("20/04/2028"),None,pd.Timestamp("20/01/2023")]}
        source = pd.DataFrame(data) 
        answer = Capture_Invalid_Dates(source,'date')
        correct_data = {'id':[1,2],'date':[pd.Timestamp("20/04/2028"),pd.NaT]}
        correct = pd.DataFrame(correct_data)
        pdt.assert_frame_equal(answer,correct,'Captured incorrect dates')

if __name__ == '__main__':
    unittest.main()
