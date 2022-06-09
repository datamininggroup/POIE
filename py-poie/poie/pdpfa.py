 
import numpy as np
import pandas as pd
import poie.prettypfa
from poie.genpy import PFAEngine
from poie.errors import PFAInitializationException, PFAUserException
  
 
class SubclassedSeries(pd.Series):
    @property
    def _constructor(self):
        return SubclassedSeries

    @property
    def _constructor_expanddim(self):
        return SubclassedDataFrame
        
    def pfa_apply(self, *args, **kwargs):
        inputs = args[0]
        pfa_eng = args[1]
        i_len = len(self.__session)
        self.__session[-1]['pfa'] = pfa_eng[0].config
        self.__session[-1]['pfa'] = inout
    
        if type(inputs) == list:
            self.apply(lambda x : pfa_eng[0].action({'x': getattr(x, inputs[0])  , 'y':getattr(x, inputs[1])  }), axis=1 )
        else:
             self[inputs].apply(lambda x: pfa_eng[0].action(x))
  
class PoieDataFrame(pd.DataFrame):

    #_metadata = ['__sessions', '']
    
    @property
    def _constructor(self):
        return SubclassedDataFrame

    @property
    def _constructor_sliced(self):
        return SubclassedSeries
    
    def __init__(self,  *args, **kwargs):

        super(PoieDataFrame, self).__init__( *args, **kwargs)
        self.__session = []
        self.__b_parsing = False
        return
       # pd.DataFrame.__init__( *args, **kwargs)
     
        
        
    def package(self, environment):
        return list(self.__session)
    
    def run_pfa(self, inpackage):
       # print(len(inpackage))
          
        for rule in inpackage:
            tmp_eng = PFAEngine.fromAst(rule['pfa'])
            self[rule['output']] = self.pfa_apply(rule['inputs'], tmp_eng)
        return True


    def __setattr__(self, key, value):
    
        if key == '_PoieDataFrame__session':
            self.__dict__[key] = value
            return
        elif key == '_PoieDataFrame__b_parsing':
            self.__dict__[key] = value
            return
        else:
            return super().__setattr__(key, value)
    
    
    
    def __cxgetattr__(self, key):
    
         if key == 'pfa_apply':
             print(key)
         else:
             return super().__getattr__(key)
            
   
    def __setitem__(self, key, value):
    
        if self.__b_parsing:
            self.__session[-1]['output'] = key
            self.__b_parsing = False
    
        return super().__setitem__(key, value)
        
    def __getitem__(self, key):
        return super().__getitem__(key)
     
    def pfa_apply(self, *args, **kwargs):
        inputs = args[0]
        pfa_eng = args[1]
        self.__b_parsing = True
        i_len = len(self.__session)
        self.__session.append({'pfa' : pfa_eng[0].config, 'inputs' : inputs, 'output': None, 'order':i_len})
         
        if type(inputs) == list:
             return self.apply(lambda x : pfa_eng[0].action({'x': getattr(x, inputs[0])  , 'y':getattr(x, inputs[1])  }), axis=1 )
        else:
             return self[inputs].apply(lambda x: pfa_eng[0].action(x))
    
