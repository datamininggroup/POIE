 
import numpy as np
import pandas as pd
import poie.prettypfa
from poie.genpy import PFAEngine
from poie.errors import PFAInitializationException, PFAUserException
 
class FeatureEngineering(object):

      def __intit__(self):
         self.rules ={}
         
      def remove_na(self):
          return poie.prettypfa.engine("""
input: double
output: double
action:
    if (input == null)
        -1.0
    else if (impute.isnum(input))
       input
    else
        -1.0
 """)
           
      def conditional(self, input, output, spython):
      
          if isinstance(input, dict):
              pfa = "input: record("
              for key ,val in input.items():
                  pfa += key + ': ' + val + ','
              pfa = pfa[:-1]
              pfa += ")\n"
               
          else:
               pfa = "input:" + input  + "\n"

               test = exec("input = 'test'" + spython)
               
          pfa += "output: double\n"
          pfa += "action:\n"
          spacer = "   "
          for code in spython.split('\n'):
          #  print(code)
            if code.startswith('elif'):
               code = 'else if ' + code[4:]

            if code.endswith(':'):
               code =   code[:-1]
            
            pfa += spacer + code + '\n'
         # pfa = "input: string\n"
         # print(pfa)
          return poie.prettypfa.engine(pfa)
           
 
      def custom(self, pfa):
           return  poie.prettypfa.engine(pfa)
 
