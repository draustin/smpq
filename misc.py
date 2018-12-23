"""For using dill with multiprocessing.Pool"""
def unpack_and_call_function(packed_function_arg):
    packed_function,arg=packed_function_arg
    function=dill.loads(packed_function)
    result=function(arg)    
    return result

def map_pool_dill(pool,function,args,no_pool=False):
    logger.debug('packing function')
    packed_function=dill.dumps(function)
    logger.debug('packing args')
    packed_args=[(packed_function,arg) for arg in args]
    logger.debug('done')
    if no_pool:
      return list(map(unpack_and_call_function,packed_args))
    else:
      return list(pool.map(unpack_and_call_function,packed_args))