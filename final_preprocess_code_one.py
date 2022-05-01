from mrjob.job import MRJob
import csv
import time
import datetime
import uuid

class MRCountSum(MRJob):
    def mapper(self, _, line):            
        values = csv.reader([line])
        #check_lenght = list(values)
        for value in values:
            
            if(value[14] == "review_date"):
                yield "key", ("customId", "productId", "startRating", "timestamp", "productTitle", "productcategory")

            if((value[11] == "Y") and (value[14] != '')):
                index =  uuid.uuid4()
                customId = value[1]
                if(value[1] != "customer_id"):
                    customId = int(value[1])
                else:
                    customId = value[1]
                productId = value[3]
                productTitle = (value[5].replace(",","")).replace(" ","-")
                if(value[7] != "star_rating"):
                    startRating = int(value[7])
                else:    
                    startRating = value[7]
                productcategory = value[6]      
                
                try:
                    timestamp = value[15]
                    targetedValues = (customId,productId,startRating,int(timestamp),productTitle,productcategory)
                    yield int(index), targetedValues
                except (IndexError, ValueError):
                    #convert to datetime
                    try:
                        timestamp = value[14]
                        timeStampElement = datetime.datetime.strptime(timestamp,"%m/%d/%Y")
                        tuple = timeStampElement.timetuple()
                        timestamp = time.mktime(tuple)
                        
                        targetedValues = (customId,productId,startRating,int(timestamp),productTitle,productcategory)
                        yield int(index), targetedValues
                    except Exception as e:
                        timestamp = value[14]
                        targetedValues = (customId,productId,startRating, timestamp,productTitle,productcategory)
                        yield int(index), targetedValues


    def combiner_old(self, key, values):
        yield key, sum(values)
        
    def reducer_old(self, key, values):
        #if values is not None:
        yield key, values


if __name__ == '__main__':
    MRCountSum.run()
