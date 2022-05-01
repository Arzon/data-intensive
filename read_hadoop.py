import pydoop.hdfs as hd
import pandas as pd
import numpy as np
import operator
import time

start_time = time.time()
with hd.open("/output_mapper/output3/part-00000") as f:
    dataset =  pd.read_csv(f)

#accumulate by productId
accumlate_rates = dataset.groupby('productId').agg(mean_startRating = ('startRating', 'mean'),
num_rate = ('startRating', 'count')).reset_index()

accumlate_startRatings_gt_one = accumlate_rates[accumlate_rates['num_rate'] > 100] #need to do it in mr

dataset_gt_one = pd.merge(dataset, accumlate_startRatings_gt_one[['productId']], on='productId', how='inner')

#user-item matrix 
user_item_matrix = dataset_gt_one.pivot_table(index='productTitle', columns='customId', values='startRating')

#normalization of user-tem matrix need for data
user_item_matrix_normalization = user_item_matrix.subtract(user_item_matrix.mean(axis=1), axis = 0)

#Pearson correlation
products_sim = user_item_matrix_normalization.T.corr()
view_product_sim = pd.DataFrame(products_sim.head(5))
print("--- Result of Pearson correlation---")
print("")
print("")
print(view_product_sim.head().to_string())


# Item-based recommendation function
def recommand_products(customer_id, num_similar_products, num_recommand):
  customer_id_unbaught = pd.DataFrame(user_item_matrix_normalization[customer_id].isna()).reset_index()
  customer_id_unbaught = customer_id_unbaught[customer_id_unbaught[customer_id]==True]['productTitle'].values.tolist()

  customer_id_watched = pd.DataFrame(user_item_matrix_normalization[customer_id].dropna(axis=0, how='all')\
                            .sort_values(ascending=False))\
                            .reset_index()\
                            .rename(columns={customer_id:'rating'})
  
  rating_prediction ={}  
         
  for product in customer_id_unbaught: 
    product_similarity_score = products_sim[[product]].reset_index().rename(columns={product:'similarity_score'})
    customer_id_watched_similarity = pd.merge(left=customer_id_watched, 
                                                right=product_similarity_score, 
                                                on='productTitle', 
                                                how='inner')\
                                        .sort_values('similarity_score', ascending=False)[:num_similar_products]
    predicted_rating = round(np.average(customer_id_watched_similarity['rating'], 
                                        weights=customer_id_watched_similarity['similarity_score']), 6)

    rating_prediction[product] = predicted_rating
  return sorted(rating_prediction.items(), key=operator.itemgetter(1), reverse=True)[:num_recommand]

products_recommand = recommand_products(customer_id=10045830, num_similar_products=5, num_recommand =3)
print("")
print("")
print("-- output result --")
print(products_recommand)

end_time = time.time() - start_time
final_time = time.strftime("%H:%M:%S", time.gmtime(end_time))
print("Total execution time: ", final_time)

