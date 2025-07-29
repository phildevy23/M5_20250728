import math
class Calculator:
    def __init__(self,a,b):
        self.a = a
        self.b = b
    def get_sum(self):
        return self.a + self.b
    def get_diff(self):
        return abs(self.a-self.b)
    def get_product(self):
        return self.a*self.b
    def get_quotient(self):
        if self.b == 0:
            return 0
        else:
            return self.a/self.b
    def get_sqrt(self):
        if self.a <=0:
            return 0
        else:
            return math.sqrt(self.a)