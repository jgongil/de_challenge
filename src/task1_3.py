## Task 3: Top 5 products

top5groceries = groceries_summary.takeOrdered(5,lambda x: -x[1])
filename = '../out/out_1_3.txt'
with open(filename, 'w') as out:
    #out.write(str(groceriesSummary.collect()))
    for item in top5groceries:
        out.write(str(item) + '\n')