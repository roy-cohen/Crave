design problem tradeoffs
how to design the dishes table
use cases:
1. search for best dish in a city (requires sorting by the average score, taking top 5) 
	so I thought to have the averagescore be the clustering key, for efficient sorting and retrieval

Since I cannot update the primary key, I cannot use the averagescore as my clustering key. This means
I decided to use dish,citystate as my partioning key, so that all dishes for a specific citystate will
be stored on the same partition.

I would have to grab dish reviews for that citystate, and then do the sort on averagescores in the spark 
application. This should be ok, as the data returned for dish,city,state combo should not be too large


2. be able to search for multiple dishes in city, if the search is for a category(which contains multiple dishes)
    for example a search for noodles can return noodles, ramen, pho, pad thai, etc.

Since dish is part of my partitioning key, the IN clause is supported, and I can use that to look up multiple dishes 
in that citystate.
    
3. inserting new reviews (which would require updating the average score values) but you cannot update
    a primary key value  (it would also require to update for a specific business)

I can now update averagescore since it is not my clustering key.  I defined businessid as my clustering key
so that I can update a row citystate,dish, and businessid 

4. upon a search, get businesses for a specific city, state, and then search for dishes by keying on the business ID?
This requires 2 trips to cassandra on every search. In order to make this more efficient, I joined the business data and 
review data in spark, and added the citystate field to my review data.  This would make my batch processing job slower, but thats
ok because it is a batch processing job and is not time critical.  Another downside is extra storage, but cassandra is scalable and 
can handle millions of columns. In return, this makes my searches faster, as you can
directly key on the dishes table by citystate.  In fact citystate is part of the partitioning key, so all rows in the same
citystate will be on the same partition, which is efficient. 


