'''
Created on Jul 16, 2012

@author: mudassar
'''

from ghmm import *


from UnfairCasino import test_seq
v = m.viterbi(test_seq)
print v


if __name__ == '__main__':
    pass
