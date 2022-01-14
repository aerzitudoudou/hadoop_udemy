#this script is to create an HBase table for movie ratings by user, then show we can quickly query it for individual users
#this is a good example of showing hbase storing sparse data
from starbase import Connection

c = Connection("127.0.0.1", "8000")

#create the schema
ratings = c.table('ratings')

if(ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

#within the ratings table, create a column family called 'rating'
ratings.create('rating')

#open u.data as read only
print("Parsing the ml-100k ratings data...\n")
ratingFile = open("d:/hadoop/ml-100k/u.data", "r")

#create a batch object from the rating table
batch = ratings.batch()

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})

ratingFile.close()

print("Committing ratings data to Hbase via REST service\n")
batch.commit(finalize=True)

print("Get back ratings for some users...\n")
print("Ratings for user ID 1:\n")
print(ratings.fetch("1"))
print("Ratings for user ID 33:\n")
print(ratings.fetch("33"))

ratings.drop()





