import time,logging
import numpy as np
logger=logging.getLogger(__name__)
x=1
def module_level_function():
    time.sleep(1)
    print("Module-level function")
    return x,"Module-level function"
    
class Callable():
    def __init__(self):
        self.message="callable object"
    
    def __call__(self):
        time.sleep(1)
        print("Callable object")
        return self.message
    
class GeneralObject():
    def __init__(self):
        self.message="General object"
    
    def doit(self):
        time.sleep(1)
        print("General object")
        return self.message,x

def square_and_add(y):
    return x+y**2
                
def square_and_add_hs(y):
    time.sleep(0.5)
    return x+y**2

def square_and_add_except_1_or_3(y):
    if y in (1,3):
        raise ValueError("I don't like y=1 or y=3")
    time.sleep(1)
    return x+y**2
    
def square_and_add_slow(y):
    time.sleep(5)
    return x+y**2

def square_and_add_logging(y):
    logger.log(logging.INFO,'squaring and adding, y=%g',y)
    time.sleep(1)
    return x+y**2

vector=np.arange(4)
def use_numpy(y):
    return y**vector

def whoops():
    raise ValueError('whoops')

# import cbma
# class CalcWithMemory:
#     def __init__(self,x,memory):
#         self.x=x
#         self.memory=memory
#         self.calc_y=cbma.cache_bound_method_by_attr(self.calc_y,'x',memory)
#
#     def calc_y(self):
#         return self.x+1
#
#     def __getstate__(self):
#         return self.x,self.memory
#
#     def __setstate__(self,state):
#         self.__init__(*state)