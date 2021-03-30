## Task 3: Calculate the average number of bathrooms and bedrooms across all the properties listed in this data set with a price of > 5000 and a review score being exactly equalt to 10.

selected_df = airbnb_df\
    .filter((airbnb_df.price>5000 ) & (airbnb_df.review_scores_value==10))

avg_summary = selected_df\
    .groupBy()\
    .avg('bathrooms')\
    .collect()[0]\
    .__getitem__(0)\
,airbnb_df\
    .groupBy()\
    .avg('bedrooms')\
    .collect()[0]\
    .__getitem__(0)

filename = '../out/out_2_3.txt'
with open(filename, 'w') as out:
    writer = csv.writer(out , lineterminator='\n')
    writer.writerow(avg_summary)