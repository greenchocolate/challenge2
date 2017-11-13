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

symbols = ['\n','`','~','!','@','#','$','%','^','&','*','(',')','_','-','+','=','{','[',']','}','|','\\',':',';','"',"'",'<','>','.','?','/',',']
def ten_biggest_voc(i):
    conn=sqlite3.connect('reddit.db')
    maxwords=[0,0,0,0,0,0,0,0,0,0]          #keep track of the maximum number of vocabularies
    subids=['','','','','','','','','','']
    subnames=['','','','','','','','','','']
    for sub in conn.execute("select id, name from 'subreddits' where length(name)%8==?",(i,)):   #first, get all subreddits by id and name
        id=sub[0]                                           #mod 8, because we are processing different chunks on different processors
        name=sub[1]
        words=set()                                                  #initialize a set which will contain all distinct words
        for comment in conn.execute("select body from 'comments' where subreddit_id=?",(id,)):
            s = comment[0].lower()                                        #find the words and add them to the set
            for sym in symbols:
                s = s.replace(sym, " ")
            for w in s.split(" "):
                if len(w.replace(" ", "")) > 0:
                    words.add(w)
        l = len(words)                                                 #number of total words in comments for this subreddit
        j = numpy.argmin(maxwords)                                     #check if it is bigger then the largest 10 we found so far
        if l>maxwords[j]:
            maxwords[j]=l
            subids[j]=id
            subnames[j]=name
    return [maxwords,subids,subnames]


if __name__ == '__main__':
    t1 = time.time()
    # conn0 = sqlite3.connect('reddit.db')
    # conn0.execute("create index sub_id on 'comments'(subreddit_id)")
    pool = Pool(processes=8)                                            #initialize a pool of 8 processors
    multiple_results = [pool.apply_async(ten_biggest_voc, (i,)) for i in range(8)]  #run code in parallel
    results=([res.get() for res in multiple_results])
    out=open('outputfile.txt','w',encoding='utf8')
    for j in range(8):
        print(results[j])
    maxwords=[]
    subids=[]
    subnames=[]
    for j in range(8):                                          #append results form 8 processors to three big lists
        maxwords=maxwords+results[j][0]
        subids=subids+results[j][1]
        subnames=subnames+results[j][2]
    maxind=numpy.argsort(maxwords)[-10:]                        #find indexes of 10 largest vocabulary sizes
    for j in range(10):
        i=int(maxind[j])
        out.write(subids[i]+'    '+subnames[i]+'    '+str(maxwords[i])+'\n')#and print them to file
    t2 = time.time()
    out.write(str(t2 - t1))
    out.close()


