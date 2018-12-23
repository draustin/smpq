import logging
from smpq import Manager
from smpq.testing import *
logging.basicConfig()
logging.getLogger('smpq.smpq').setLevel(logging.DEBUG)
##
if __name__=="__main__":
    data=range(10)
    logging.getLogger('smpq.testing').setLevel(logging.DEBUG)
    with Manager(4) as manager:
        results=manager.map(square_and_add_logging,data)
    assert results==[x**2+1 for x in data]
    
##
try:
    try:
        raise ValueError('cow')
    except ValueError as e:
        logger.exception(e)
        print('here')
except:
    print('now here')


