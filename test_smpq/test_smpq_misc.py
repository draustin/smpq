import pytest,sys
from collections import Counter
import smpq
from smpq.testing import *
#if sys.platform!='linux':
# With pytest capture, logging causes freeze on Ubuntu. Or maybe just coincidence.
# It freezes nondeterministically.
import logging
logging.basicConfig()
logging.getLogger('smpq').setLevel(2)

def test_Manager():
    with smpq.Manager(2) as manager:
        manager.add_task(0,module_level_function)
        manager.add_task('a',Callable())
        manager.add_task(1,GeneralObject().doit)
        manager.block_until_tasks_done()
        results=list(manager.get_results())
        assert len(results)==3
        for result in (('a','callable object'),(1,('General object',1)),(0,(1,'Module-level function'))):
            assert results.count(result)==1
            
def test_Manager_map():
    ##
    with smpq.Manager(4) as manager:
        results=list(manager.map(square_and_add_hs,range(4)))
    assert results==[1,2,5,10]
    ## TODO Disabled to remove numpy dependency... put it back optionally.
    # with smpq.Manager(4) as manager:
    #     results=list(manager.map(use_numpy,range(4)))
    ##
    with pytest.raises(ValueError) as e:
        with smpq.Manager(2) as manager:
            results=list(manager.map(square_and_add_except_1_or_3,range(4)))
    assert "I don't like" in str(e)

def test_Manager_retire():
    ##
    n=range(20)
    with smpq.Manager(2,retire_task_num=2) as manager:
        results=list(manager.map(square_and_add_hs,n))
    assert results==list(map(square_and_add,n))

# Don;t use joblib anymore - but don't remove until this has been exported somehow.
# def test_Manager_joblib():
#     import tempfile,joblib
#     with tempfile.TemporaryDirectory() as cachedir:
#         memory=joblib.Memory(cachedir)
#         calcs=[CalcWithMemory(x,memory) for x in range(4)]
#         def eval_call_y(calc):
#             return calc.calc_y()
#         with smpq.Manager(4) as manager:
#            y0=list(manager.map(eval_call_y,calcs))
#         y1=list(map(eval_call_y,calcs))
#     assert y0==y1

if __name__=="__main__":
    test_Manager()    
    test_Manager_map()
    #test_Manager_joblib()
