#!/usr/bin/env python
# -*- coding=utf-8 -*-

import codecs

def lev_dist(source, target):
    if source == target:
        return 0

    # Prepare a matrix
    slen, tlen = len(source), len(target)
    dist = [[0 for i in range(tlen+1)] for x in range(slen+1)]
    for i in range(slen+1):
        dist[i][0] = i
    for j in range(tlen+1):
        dist[0][j] = j

    # Counting distance, here is my function
    for i in range(slen):
        for j in range(tlen):
            cost = 0 if source[i] == target[j] else 1
            dist[i+1][j+1] = min(
                            dist[i][j+1] + 1,   # deletion
                            dist[i+1][j] + 1,   # insertion
                            dist[i][j] + cost   # substitution
                        )
    return dist[-1][-1]/slen


# load words from a file into a list
def loadWords(file):
    list = [] # create an empty list to hold the file contents
    file_contents = codecs.open(file, "r", "utf-8") # open the file
    for line in file_contents: # loop over the lines in the file
        line = line.strip() # strip the line breaks and any extra spaces
        list.append(line) # append the word to the list
    return list

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 3:
        source, target = sys.argv[1], sys.argv[2]
    else:
        source="Human.txt"
        target="Google.txt"

    # create two lists, one of each file by calling the loadWords() function on the file
    list1 = loadWords(source)
    list2 = loadWords(target)

    # now you have two lists; each file has to have the words you are comparing on the same lines
    # now call you lev_distance function on each pair from those lists

    for i in range(0, len(list1)): # so now you are looping over a range of numbers, not lines
        print (lev_dist(list1[0], list2[i]))


#    print lev_dist(source, target)