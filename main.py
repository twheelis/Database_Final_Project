#!flask/bin/python
from flask import Flask, jsonify, request
from os import listdir
from pymongo import MongoClient
from bs4 import BeautifulSoup
from bson import json_util
import json
import csv
import datetime

app = Flask(__name__)


@app.route('/')
def index():
    return "This is a sample rest service"


## rest service to upload json files within a given directory. This is a bulk upload, so it will upload all the json files in a specified folder.
@app.route('/bulkupload', methods=['GET'])
def uploadJSON():
    # Get path from query string
    path = request.args.get('path')

    if not path.endswith('/'):
        path = path + "/"

    # empty list to store all json files in directory
    files = []
    hotels = 0
    reviews = 0

    # save the names of all the json files in the directory saved to the path variable above
    for f in listdir(path):
        if f.endswith('.json'):
            files.append(f)

    # iterate through each json file
    for f in files:

        with open(path + f, 'rb') as hotel:
            # extract json into filedata dictionary
            data = json.load(hotel)
            summary = uploadreview(data)
            data = json.loads(summary.data)
            hotels += data["Hotels Uploaded"]
            reviews += data["Reviews Uploaded"]


    # turns everything into json
    return jsonify({"Upload Time": datetime.datetime.now().strftime("%c"),
                    "Working directory": path,
                    "Hotels Uploaded":hotels,
                    "Reviews Uploaded":reviews})


## rest service to upload a single hotel review and write the review and hotel logs. This can be done by making a superlong URL.
# sample curl to test this, if you're feeling fancy. Run this in your terminal to give yourself a sample review to try to test this code.
# curl --header "Content-Type: application/json" \
#   --request POST \
#   --data '{"Reviews":[{"Ratings":{"Service":"4","Cleanliness":"5","Overall":"5.0","Value":"4","Sleep Quality":"4","Rooms":"5","Location":"5"},"AuthorLocation":"Boston","Title":"“Excellent Hotel & Location”","Author":"gowharr32","ReviewID":"UR126946257","Content":"We enjoyed the Best Western Pioneer Square. My husband and I had a room with a king bed and it was clean, quiet, and attractive. Our sons were in a room with twin beds. Their room was in the corner on the main street and they said it was a little noisier and the neon light shone in. But later hotels on the trip made them appreciate this one more. We loved the old wood center staircase. Breakfast was included and everyone was happy with waffles, toast, cereal, and an egg meal. Location was great. We could walk to shops and restaurants as well as transportation. Pike Market was a reasonable walk. We enjoyed the nearby Gold Rush Museum. Very, very happy with our stay. Staff was helpful and knowledgeable.","Date":"March 29, 2012"}],"HotelInfo":{"Name":"BEST WESTERN PLUS Pioneer Square Hotel","HotelURL":"/ShowUserReviews-g60878-d72572-Reviews-BEST_WESTERN_PLUS_Pioneer_Square_Hotel-Seattle_Washington.html","Price":"$117 - $189*","Address":"<address class=\"addressReset\"> <span rel=\"v:address\"> <span dir=\"ltr\"><span class=\"street-address\" property=\"v:street-address\">77 Yesler Way</span>, <span class=\"locality\"><span property=\"v:locality\">Seattle</span>, <span property=\"v:region\">WA</span> <span property=\"v:postal-code\">98104-2530</span></span> </span> </span> </address>","HotelID":"72572","ImgURL":"http://media-cdn.tripadvisor.com/media/ProviderThumbnails/dirs/51/f5/51f5d5761c9d693626e59f8178be15442large.jpg"}}' \
#   http://localhost:5000/uploadReview
@app.route('/uploadReview', methods=['POST'])
def handlereview():
    data = request.json
    return uploadreview(data)


#query Hotels or Reviews collections by Key values.  arg variable will take in three arguments separated by +.
#first argument should either be "Hotel" or "Review"
#second argument should be one of the keys within the collection
#third argument is what value one wishes to query
#example query for all reviews for Hotel with the ID 72572: /query/Review+HotelID+72572
@app.route('/query/<arg>', methods=['GET'])
def query(arg):
    #convert arg string into arg list using "," as the delimiter
    arg = arg.split("+")
    # Mongo is the chosen database, make sure your MongoDB is running before execution of this program
    client = MongoClient()
    # database name is "HotelReviews"
    db = client.HotelReviews
    
    #determine if 'Hotel' or 'Reviews' collection should be used
    if arg[0] == 'Hotel':
        db = client.HotelReviews.Hotel
        return output(db, arg[1], arg[2])
    elif arg[0] == 'Reviews':
        db = client.HotelReviews.Reviews
        return output(db, arg[1], arg[2])
    else:
        return jsonify ({"Error": "Please indicate query field for 'Hotel' or 'Reviews'"})

    
def uploadreview(data):

    # Mongo is the chosen database, make sure your MongoDB is running before execution of this program
    client = MongoClient()

    # get current date and time
    currenttime = datetime.datetime.now()

    # database name is "HotelReviews"
    db = client.HotelReviews
    filedata = data

    # logs of uploaded collections
    reviewslog = [[currenttime.strftime("%c")], ["HotelID", "ReviewID", "Upload Status"]]
    hotelslog = [[currenttime.strftime("%c")], ["HotelID", "Upload Status"]]

    # "Hotel" collection to hold all hotel's data
    collection = db.Hotel

    try:
        # extract plain text from "Address"'s xml format
        if "Address" in filedata["HotelInfo"]:
            x = BeautifulSoup(filedata["HotelInfo"]["Address"]).get_text().strip()
            filedata["HotelInfo"].update({"Address": x})

        # add data to collection
        # update_one(filter, update, upsert=True)
        # filter - { key : value }
        # filter - tells mongo to look for a document for 'key' with specified 'value'
        # upsert = update/insert
        # upsert - updates if document is found, else insert
        collection.update_one({'HotelID': filedata["HotelInfo"]["HotelID"]}, {'$set': filedata["HotelInfo"]},
                                      upsert=True)
        # mark upload as "Success"
        hotelslog.append([filedata["HotelInfo"]["HotelID"], "Success"])
    except Exception as e:
        # mark unsuccessful upload with error message generated by the system
        hotelslog.append([filedata["HotelInfo"]["HotelID"], e])

    # "Reviews" collection to hold all users' reviews data
    collection = db.Reviews

    # iterate through each review
    for doc in filedata["Reviews"]:

        # add "HotelID" to each review for reference
        doc.update({'HotelID': filedata["HotelInfo"]["HotelID"]})

        # parse out key and value pairs inside each review
        for key, value in doc.items():

            # check for nested documents
            if isinstance(value, dict):
                for key in sorted(value):
                    # replace "." inside keys with blank to avoid error
                    value[key.replace(".", "")] = value.pop(key)
            else:
                # remove unicode characters within texts
                value = value.encode('ascii', 'ignore').decode('unicode_escape')
                doc.update({key: value})
        try:
            # add data to collection
            collection.insert_one(doc)

            # mark upload as "Success"
            reviewslog.append([doc["HotelID"], doc["ReviewID"], "Success"])
        except Exception as e:
            # mark unsuccessful upload with error message generated by the system
            reviewslog.append([doc["HotelID"], doc["ReviewID"], e])

    # this writes the logs to the external log files
    writereviewslog(reviewslog)
    writehotelslog(hotelslog)

    # turns everything into json
    return jsonify({"Upload Time": currenttime.strftime("%c"),
                    "Hotels Uploaded": len(hotelslog) - 2,
                    "Reviews Uploaded": len(reviewslog) - 2})  


def writereviewslog(reviewslog):
    # write logs to csv files, logs will write to directory in which this app is located in
    out = open('ReviewsUploadLog.csv', 'w', newline='')
    for item in reviewslog:
        csv.writer(out).writerow(item)
    out.close()


def writehotelslog(hotelslog):
    out = open('HotelsUploadLog.csv', 'w', newline='')
    for item in hotelslog:
        csv.writer(out).writerow(item)
    out.close()

#returns queried documents, db is the collection to be used, arg1 is the 'key', arg2 is the value searching for
def output(db, arg1, arg2):
    fileCursor = db.find()
    fileList = []
    
    #compile all documents that fits query terms
    for document in fileCursor:
        tempJSON = json.loads(json_util.dumps(document))
        #query will result in a hit if arg2 is found anywhere inside the string paired with the arg1 key
        try:
            #look at outer most keys
            if tempJSON[arg1].find(arg2) != -1:
                fileList.append(tempJSON)
        except Exception:
            try:
                #if arg1 is "Ratings" and arg2 is a key inside "Ratings", i.e. Cleanliness, Location, etc.
                #arg2 must match key names exactly
                #filter by one key name at a time
                if arg2 in list(tempJSON[arg1].keys()):
                    fileList.append(tempJSON)
            except Exception:
                try:
                    #look inside 'Ratings' keys
                    if tempJSON["Ratings"][arg1].find(arg2) != -1:
                        fileList.append(tempJSON)
                except Exception:
                    pass

    #return an error if query returns no record or return what is found by query
    if not fileList:
        return jsonify({"Error": "There is no record of %s in %s" %(arg2, arg1)})
        #return (tempJSON["HotelID"])
    else:
        return jsonify(fileList)    


# Starts the server for serving Rest Ser.vices
if __name__ == '__main__':
    app.run(debug=True)
