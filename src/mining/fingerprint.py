'''
Created on Jun 27, 2012

@author: mudassar
'''
import re, string
import json
import operator
from copy import copy
from copy import deepcopy
from copy import copy
from copy import deepcopy

def to_lower(st):
    return st.lower()

def remove_space(st):
    st = st.strip()
    return st

def remove_punctuation(st):
    
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    st = regex.sub('', st)
    return st


def replace_special_characters_with_space(st):
    special_chars = ["\\", "/"]
    for chr in special_chars:
        
        st = st.replace(chr, " ")
    
    return st
def tokenize(st):
    return st.split()

def sort_token_remove_duplicates(li):
    return sorted(set(li))

def join_tokens(li):
    return " ".join(li)

def western_to_ascii(st):
    li = list(st)
    for i in range(len(li)):
        li[i] = translate(li[i])
    return "".join(li)

def translate(c) :
        if  c == '\u00C0' or    c == '\u00C1' or c == '\u00C2' or c == '\u00C3' or  c == '\u00C4' or c == '\u00C5' or  c == '\u00E0' or  c == '\u00E1' or  c == '\u00E2' or  c == '\u00E3' or c == '\u00E4' or c == '\u00E5' or c == '\u0100' or c == '\u0101' or c == '\u0102' or c == '\u0103' or c == '\u0104' or c == '\u0105' :
                return 'a'
        elif  c == '\u00C7' or c == '\u00E7' or c == '\u0106' or c == '\u0107' or c == '\u0108' or c == '\u0109' or c == '\u010A' or c == '\u010B' or c == '\u010C' or c == '\u010D' :  
            return 'c'
        
        elif c == '\u00D0' or c == '\u00F0' or c == '\u010E' or c == '\u010F' or c == '\u0110' or c == '\u0111' :     
                return 'd'
        elif  c == '\u00C8' or c == '\u00C9' or c == '\u00CA' or c == '\u00CB' or c == '\u00E8' or c == '\u00E9' or c == '\u00EA' or c == '\u00EB' or c == '\u0112' or c == '\u0113' or c == '\u0114' or c == '\u0115' or c == '\u0116' or c == '\u0117' or c == '\u0118' or c == '\u0119' or c == '\u011A' or c == '\u011B' :
            return 'e'
        elif  c == '\u011C' or c == '\u011D' or c == '\u011E' or c == '\u011F' or c == '\u0120' or c == '\u0121' or c == '\u0122' or c == '\u0123' :
            return 'g'
        elif c == '\u0124' or c == '\u0125' or c == '\u0126' or c == '\u0127' :  
            return 'h'
        elif c == '\u00CC' or c == '\u00CD' or c == '\u00CE' or c == '\u00CF' or c == '\u00EC' or c == '\u00ED' or c == '\u00EE' or c == '\u00EF' or c == '\u0128' or c == '\u0129' or c == '\u012A' or c == '\u012B' or c == '\u012C' or c == '\u012D' or c == '\u012E' or c == '\u012F' or c == '\u0130' or c == '\u0131' : 
            return 'i'
        elif c == '\u0134' or c == '\u0135' :    
            return 'j'
        elif c == '\u0136' or c == '\u0137' or c == '\u0138' :
            return 'k'
        elif c == '\u0139' or c == '\u013A' or c == '\u013B' or c == '\u013C' or c == '\u013D' or c == '\u013E' or c == '\u013F' or c == '\u0140' or c == '\u0141' or c == '\u0142' :
            return 'l'
        elif c == '\u00D1' or c == '\u00F1' or c == '\u0143' or c == '\u0144' or c == '\u0145' or c == '\u0146' or c == '\u0147' or c == '\u0148' or c == '\u0149' or c == '\u014A' or c == '\u014B' :
            return 'n'
        elif c == '\u00D2' or c == '\u00D3' or c == '\u00D4' or c == '\u00D5' or c == '\u00D6' or c == '\u00D8' or c == '\u00F2' or c == '\u00F3' or c == '\u00F4' or c == '\u00F5' or c == '\u00F6' or c == '\u00F8' or c == '\u014C' or c == '\u014D' or c == '\u014E' or c == '\u014F' or c == '\u0150' or c == '\u0151' :
            return 'o'
        elif c == '\u0154' or c == '\u0155' or c == '\u0156' or c == '\u0157' or c == '\u0158' or c == '\u0159' : 
            return 'r'
        elif c == '\u015A' or c == '\u015B' or c == '\u015C' or c == '\u015D' or c == '\u015E' or c == '\u015F' or c == '\u0160' or c == '\u0161' or c == '\u017F' :
            return 's'
        elif c == '\u0162' or c == '\u0163' or c == '\u0164' or c == '\u0165' or c == '\u0166' or c == '\u0167' :
            return 't'
        elif c == '\u00D9' or c == '\u00DA' or c == '\u00DB' or c == '\u00DC' or c == '\u00F9' or c == '\u00FA' or c == '\u00FB' or c == '\u00FC' or c == '\u0168' or c == '\u0169' or c == '\u016A' or c == '\u016B' or c == '\u016C' or c == '\u016D' or c == '\u016E' or c == '\u016F' or c == '\u0170' or c == '\u0171' or c == '\u0172' or c == '\u0173' :
            return 'u'
        elif c == '\u0174' or c == '\u0175' :
            return 'w'
        elif c == '\u00DD' or c == '\u00FD' or c == '\u00FF' or c == '\u0176' or c == '\u0177' or c == '\u0178' :
            return 'y'
        elif c == '\u0179' or c == '\u017A' or c == '\u017B' or c == '\u017C' or c == '\u017D' or c == '\u017E' : 
            return 'z'
        
        return c
    
def delete_market_keywords(st):
    try:
        dictionary = ["jeans", "tshirt", "top", "shirt", "casual", "boots", "shoes", "the", "company", "of", "at", "this", "to", "for", "brand", "miss", "traders", "laundry", "and", "by", "for", "a", "in", "conn"]
        st_list = st.split(" ")        
        st_temp_list = deepcopy(st_list)        
        for i in range(len(st_temp_list)):            
            if st_temp_list[i] in dictionary:
                
                while True:
                    try:
                        st_list.remove(st_temp_list[i])
                    except:
                        break
                
        
        return " ".join(st_list)
    except Exception, e:
        print " i got the exception here for st", st
        raise 


def fingerprintfilter(word):
    word = to_lower(word)
    word = remove_space(word)
    word = remove_punctuation(word)
    word = tokenize(word)
    word = sort_token_remove_duplicates(word) 
    word = join_tokens(word)
    word = western_to_ascii(word) 
    return word

def ngramtokenizer(word, ngram):
    tokens = []
    for i in range(0, len(word), ngram):
        tokens.append(word[i:ngram])
    return tokens



def ngramfilter(word, ngram):
    word = to_lower(word)
    word = remove_space(word)
    word = remove_punctuation(word)
    word = ngramtokenizer(word, ngram)
    word = sort_token_remove_duplicates(word) 
    word = join_tokens(word)
    word = western_to_ascii(word) 
    return word

def fingerprint_algo(search, match):    
    search = fingerprintfilter(search)
    match = fingerprintfilter(match) 
    return (search, match)

if __name__ == '__main__':
    print fingerprint_algo(search="leather", match="100% Leather")
