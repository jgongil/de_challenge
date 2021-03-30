## Task 4: How many people can be accomodated by the property with the lowest price and highest rating?

naccomodates_bestdeal = airbnb_df\
    .orderBy(airbnb_df.price.asc(),airbnb_df.review_scores_rating.desc())\
    .select('accommodates')\
    .take(1)[0]\
    .__getitem__(0)

filename = '../out/out_2_3.txt'
with open(filename, 'w') as out:
    out.write(str(naccomodates_bestdeal))