'''
Created on Jul 11, 2012

@author: mudassar
'''


import Orange
from Orange.classification import svm
from Orange.evaluation import testing, scoring
from OWWidget import *
import OWGUI
import orange
import sys, os
import orngTree
from mining.OWSaveClassifier import OWSaveClassifier


def test_accuracy():
    correct = 0.0
    for ex in data:
        if classifier(ex) == ex.getclass():
            correct += 1
    print "Classification accuracy:", correct / len(data)
    
data = Orange.data.Table("features.tab")
classifier = svm.LinearSVMLearner()

#a=classifier(data)
app = QApplication([])
w = OWSaveClassifier()
#
##data = orange.ExampleTable("../../doc/datasets/iris.tab")
w.setClassifier(classifier(data))
w.show()
app.exec_()
w.saveSettings()
#test_accuracy()
