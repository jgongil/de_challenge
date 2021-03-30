## Task 2: Creates CSV that lists the minimum price, maximum price and total row count

summary = airbnb_df\
    .groupBy()\
    .min('price')\
    .collect()[0]\
    .__getitem__(0)\
,airbnb_df\
    .groupBy()\
    .max('price')\
    .collect()[0]\
    .__getitem__(0)\
,airbnb_df.count()

filename = '../out/out_2_2.txt'
with open(filename, 'w') as out:
    writer = csv.writer(out , lineterminator='\n')
    writer.writerow(summary)