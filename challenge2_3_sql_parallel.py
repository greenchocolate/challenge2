import sqlite3
import numpy,time
from multiprocessing import Pool

"""
reddit.db has three tables with the following columns:
authors: 'id', 'name'
comments: 'id', 'author_id', 'subreddit_id', 'parent_id', 'body'
subreddits: 'id', 'name'
where authors.id = comments.author_id and subreddits.id=subreddit_id
"""

def ten_biggest_depths(i):
    conn=sqlite3.connect('reddit.db')
    maxdepths=[0,0,0,0,0,0,0,0,0,0]          #keep track of the 10 deepest depths
    subids=['','','','','','','','','','']
    subnames=['','','','','','','','','','']
    for sub in conn.execute("select id, name from 'subreddits' where length(name)%8==?",(i,)):   #first, get all subreddits by id and name
        subid=sub[0]
        subname=sub[1]
        d1={}                   #dictionary to save 'parent_id', 'id' pairs
        d2={}                   #dictionary to save 'id', 'parent_id' pairs
        l=[]                    #list containing all comment ids in subreddit
        threads={}              #keeping track of maximal depths for each thread
        for comm in conn.execute("select id, parent_id from 'comments' where subreddit_id=?",(subid,)):
            id=comm[0]          #get all id, parent_id pairs for all comments in this subreddit and store in fields
            parent_id=comm[1]
            d2[id]=parent_id
            d1[parent_id]=id
            l.append(id)
        for id in l:                                #for all comments in this subreddit
            if id not in d1:                        #if the id is not parent of another id calculate depth
                c=0                                 #this means go up until we hit a top level command
                while id[0:3]!='t3_':
                    if id in d2:                    #this line makes sure we don't get an error from "orphans"
                        id=d2[id]                   #set id to parent id
                        c=c+1                       #and increase depth
                    else:
                        break
                if id[0:3]=='t3_':                      #only in case we reached a top level comment
                    thread=id                           #keep track of the maximum depth for each thread
                    if thread in threads:
                        if c > threads[thread]:
                            threads[thread]=c           #if it's larger than the element already there store it
                    else:
                        threads[thread]=c               #if there is no element already there store it
        aver_depth=0                                #compute average depth for each subreddit
        for thread in threads:
            aver_depth += threads[thread]
        aver_depth=aver_depth/max(len(threads),1)          #and keep track of the largest ones
        j = numpy.argmin(maxdepths)  # check if it is bigger then the largest 10 we found so far
        if aver_depth > maxdepths[j]:
            maxdepths[j] = aver_depth
            subids[j] = subid
            subnames[j] = subname
    return [maxdepths,subids,subnames]


if __name__ == '__main__':
    t1 = time.time()
    pool = Pool(processes=8)                                            #initialize a pool of 8 processors
    multiple_results = [pool.apply_async(ten_biggest_depths, (i,)) for i in range(8)]  #run code in parallel
    results=([res.get() for res in multiple_results])
    out=open('outputfile3.txt','w',encoding='utf8')
    for j in range(8):
        print(results[j])
    maxdepths=[]
    subids=[]
    subnames=[]
    for j in range(8):                                          #append results form 8 processors to three big lists
        maxdepths=maxdepths+results[j][0]
        subids=subids+results[j][1]
        subnames=subnames+results[j][2]
    maxind=numpy.argsort(maxdepths)[-10:]                        #find indexes of 10 largest vocabulary sizes
    for j in range(10):
        i=int(maxind[j])
        out.write(subids[i]+'    '+subnames[i]+'    '+str(maxdepths[i])+'\n')#and print them to file
    t2 = time.time()
    out.write(str(t2 - t1))
    out.close()
