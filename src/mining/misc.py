'''
Created on Jun 27, 2012

@author: mudassar
'''
from mining.fingerprint import fingerprint_algo
from mining.levenstienmodule import levenshtein_distance

LEVENSHTEIN = 1

def exactmatch(**kwargs):
    search = kwargs['token']
    match = kwargs['match']
    if str(search).find(str(match)) > -1:
        return True
    else:
        return False


def fingerprint_exactmatch(**kwargs):
    search = kwargs['token']
    match = kwargs['match']
    search, match = fingerprint_algo(search, match)
    if search == match:
        return True
    else:
        return False

def levenshtein_and_fingerprint_mixed(**kwargs):
    search = kwargs['token']
    match = kwargs['match']
    search, match = fingerprint_algo(search, match)
    return levenshtein_exactmatch(token=search, match=match)


def levenshtein_exactmatch_returndistance(**kwargs):
    search = kwargs['token']
    match = kwargs['match']
    distance = levenshtein_distance(search, match)
    return distance
    
def levenshtein_exactmatch(**kwargs):
    search = kwargs['token']
    match = kwargs['match']
    distance = levenshtein_distance(search, match)
    if len(match) < 3:
        if distance == 0:
            return True
        else:
            return False
    if distance <= LEVENSHTEIN:
        return True
    else:
        return False

if __name__ == '__main__':
    print levenshtein_exactmatch(token="leather", match="100% Leather")
    print fingerprint_exactmatch(token="leather", match="100% Leather")
    print levenshtein_and_fingerprint_mixed(token="leather", match="100% Leather")
