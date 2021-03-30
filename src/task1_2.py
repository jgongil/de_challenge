# PART 1: Spark RDD API

## Task 2 - Part a: unique list of products

def create_pair(item): 
    return (item, 1) 

# returns list of pairs with unique key (product name), and value set as 1
groceries_summary = groceries_df.rdd\
    .flatMap(list)\
    .map(create_pair)\
    .reduceByKey(lambda a,b: a+b)\
    .filter(lambda x: x[0] != None)\

unique_products = groceries_summary.keys()

#uniqueProducts.coalesce(1).saveAsTextFile('./out_1_2a')

filename = '../out/out_1_2a.txt'
with open(filename, 'w') as out:
    for item in unique_products.collect():
        out.write(str(item) + '\n')
        
#unique_products.take(5)

## Task 2 - Part b: total count of products

total_items = sum(groceries_summary.values().collect())
filename = '../out/out_1_2b.txt'
with open(filename, 'w') as out:
    out.write('Count: \n' + str(total_items))