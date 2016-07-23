'''
Created on Jul 11, 2012

@author: mudassar
'''
import orange
try:
    from maxent import *
except ImportError:
    import sys
    print >> sys.stderr, 'maxent module not found, get it from homepages.inf.ed.ac.uk/s0450736/maxent_toolkit.html'
    sys.exit(-1)


def test_accuracy():
    correct = 0.0
    for ex in data:
        if classifier(ex) == ex.getclass():
            correct += 1
    print "Classification accuracy:", correct / len(data)
    
from orngMaxent import *

data = orange.ExampleTable("features.tab")

#// build a learner, specify training data and training parameters 
classifier = MaxentLearner(data, iters=10)

#// do prediction on the ith data
#// way 1: return the most possible class label
for i in range(0, 10):
    c = classifier(data[i])
    
    #// way 2: return probability distribution for all class labels
    #// p[0] is the probability of the first class in data.domain.classVar.values
    #// p[1] is the probability of the second class,  and so on
    p = classifier(data[i], orange.GetProbabilities)
    
    #// way 3: return a tuple of the most possible class label and its probability
    r = classifier(data[i], orange.GetBoth)
    
    #// Finally, if you want verbose message during training, just call set_verbose:
    set_verbose(1)
    
test_accuracy()
