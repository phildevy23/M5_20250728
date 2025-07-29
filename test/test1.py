import unittest
from calculator import Calculator

class TestOperations(unittest.TestCase):
    def test_sum(self):
        calculation = Calculator(5,12)
        answer = calculation.get_sum()
        self.assertEqual(answer,17,'The sum is wrong.')
    def test_diff(self):
        calculation = Calculator(9,1)
        answer = calculation.get_diff()
        self.assertEqual(answer,8,'The diff is wrong.')
    def test_product(self):
        calculation = Calculator(3,6)
        answer = calculation.get_product()
        self.assertEqual(answer,18,'The product is wrong.')
    def test_quotient(self):
        calculation = Calculator(8,2)
        answer = calculation.get_quotient()
        self.assertEqual(answer,4,'The quotient is wrong.')
    def test_sqrt(self):
        calculation = Calculator(8,2)
        answer = calculation.get_sqrt()
        self.assertEqual(round(answer,1),2.8,'The Square root is wrong.')                  
if __name__ == '__main__':
    unittest.main()