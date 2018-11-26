from os import listdir
from pymongo import MongoClient
import json
import csv

path = 'C:/Users/anhai/Desktop/SMU/MSDS7330_Database/MSDS7330_FinalTermProject/Data/'
client = MongoClient()
db = client.HotelReviews
files = []
filedata = {}
reviewslog = [["HotelID", "ReviewID", "Upload Status"]]
hotelslog = [["HotelID", "Upload Status"]]

for f in listdir(path):
    if f.endswith('.json'):
        files.append(f)

   
for f in files:        
    with open(path + f, 'rb') as hotel:
        filedata = json.load(hotel)
        collection = db.Hotel
        try:
            collection.insert_one(filedata["HotelInfo"])
            hotelslog.append([filedata["HotelInfo"]["HotelID"], "Success"])
        except Exception as e:
            hotelslog.append([filedata["HotelInfo"]["HotelID"], e])
        collection = db.Reviews
        for doc in filedata["Reviews"]:
            doc.update({'HotelID': filedata["HotelInfo"]["HotelID"]})
            for key, value in doc.items():
                if isinstance(value, dict):
                    for key in value: 
                        value[key.replace(".", "")] = value.pop(key)
                else:
                    value = value.encode('ascii','ignore').decode('unicode_escape')
                    doc.update({key: value})
            try:
                collection.insert_one(doc)
                reviewslog.append([doc["HotelID"], doc["ReviewID"], "Success"])
            except Exception as e: 
                reviewslog.append([doc["HotelID"], doc["ReviewID"], e])

            
out = open('ReviewsUploadLog.csv', 'w', newline='')
for item in reviewslog:
    csv.writer(out).writerow(item)
out.close()
    
out = open('HotelsUploadLog.csv', 'w', newline='')
for item in hotelslog:
    csv.writer(out).writerow(item)
out.close()

        

    