from mrjob.job import MRJob

class MRCountSum(MRJob):
    def mapper(self, _, line): 
        if len(line):  
            col = line.split(",")
            product_id = col[1]
            rating = col[3]
            if rating != "\"startRating\"":
                targetedValues = (1, int(rating))  
                product_id = product_id.replace('"',"")
                yield product_id, targetedValues

    def reducer(self, key, values):
        count = 0
        lists = []
        for val in values:
            count = count + val[0]
            lists.append(val[1])

        sum = 0
        for t in lists:
            sum = sum + t

        mean = sum / len(lists)
        targetValues = (count, mean)
        yield key, targetValues

if __name__ == '__main__':
    MRCountSum.run()
