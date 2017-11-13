import sqlite3
import numpy,time
conn=sqlite3.connect('reddit.db')
output=open('output2.txt','w',encoding='utf-8')
t1=time.time()
i=0
subreddit=[]
max_pairs_count=[0,0,0,0,0,0,0,0,0,0]
max_pairs=[('',''),('',''),('',''),('',''),('',''),('',''),('',''),('',''),('',''),('','')]
#find subreddits in DESCENDING order of distinct author count
for comment in conn.execute("select subreddit_id, count(distinct author_id) from comments \
group by subreddit_id order by count(distinct author_id) desc"):
    m = numpy.argmin(max_pairs_count)
    # check if the total number of subauthors is smaller than the 10th biggest pair found so far.
    if comment[1]<max_pairs_count[m]:
        break
    else:
        current_id=comment[0]
        subreddit.append(current_id)
        print(comment[1])
        for j in range(i):
            c=0
            for c in conn.execute("select count(*) from (select distinct author_id from \
            comments where subreddit_id=? intersect select distinct author_id from comments \
            where subreddit_id=?)",(subreddit[j],current_id)):
                c=c[0]
                if c>max_pairs_count[m]:
                    print("new max = "+str(c))
                    max_pairs_count[m]=c
                    max_pairs[m]=(current_id,subreddit[j])
                    m=numpy.argmin(max_pairs_count)
    i=i+1
for i in range(10):
    output.write(str(max_pairs[i])+'     '+str(max_pairs_count[i])+'\n')#print results to file
t2 = time.time()
output.write(str(t2 - t1))
output.close()
