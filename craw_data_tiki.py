
import requests
import json
import pandas as pd
from tqdm import tqdm
import unicodedata
import time 
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor


def fetchProductID(headers, params, old_api_request_url, product_id_list):
    df_id_tmp = pd.DataFrame([], columns=["id"])
    pageNum = 1
    running = True
    while running:
        '''With the page number param available, data return only 2000 items with 51 pages,
          If you block this param, the data will fetch until the protocol crash which can up 
          to 1 million items but when you drop duplicates it return only 40 items'''   
        #print("Crawl page number: ", pageNum)
        params["page"] = pageNum
        '''params limit is 40 -> each loop fetch 40 items'''
        # Divided each batch with approximately 2000 items 
        if(len(product_id_list) >= 2000):
            print("Reach 2000 items")
            df_temp = pd.DataFrame(product_id_list, columns=["id"])
            df_id_tmp = pd.concat([df_id_tmp, df_temp], ignore_index=True)
            print(df_id_tmp.shape)
            # Truncate all the list
            del product_id_list[:]
                  
        response = requests.get(old_api_request_url, headers=headers, params=params)

        if response.status_code == 200:
            try:
                if(response.json()['data'] == []):
                    #print(pageNum)
                    break

                for product in response.json()["data"]:
                    id = str(product["id"])
                    product_id_list.append(id)
            except: pass
        else:
            print(f"Error, status code = {response.status_code}")
            pass
        pageNum+=1
    # Fill out the list that not reach 2000 items
    df_temp = pd.DataFrame(product_id_list, columns=["id"])
    df_id_tmp = pd.concat([df_id_tmp, df_temp], ignore_index=True)
    # Truncate all the list
    del product_id_list[:] 
    return df_id_tmp


def crawlProductData(list_id, url, headers, pars, product_detail_list):
    df_product_tmp = pd.DataFrame([])
    for id in tqdm(list_id, total=len(list_id)):
        # Divided each batch with maximum 2000 items 
        if(len(product_detail_list) >= 2000):
            print("Reach 2000 items")
            df_temp = pd.DataFrame(product_detail_list)
            df_product_tmp = pd.concat([df_product_tmp, df_temp], ignore_index=True)
            # Truncate all the list
            del product_detail_list[:]
            print(df_product.shape)

        response = requests.get(url.format(id), headers=headers, params=pars)
        
        if(response.status_code==200):
            try:
                product_detail_list.append(response.json())
            except:
                #print("Something else went wrong")
                pass

        else:
            print(f"Error, status code = {response.status_code}")
            pass

        # Fill out the list that not reach 2000 items
        df_temp = pd.DataFrame(product_detail_list)
        df_product_tmp = pd.concat([df_product_tmp, df_temp], ignore_index=True)
        # Truncate all the list
        del product_detail_list[:]
    return df_product_tmp


def dimSeller(df_tmp, df_fact):
    # Create dim seller
    seller = df_tmp['current_seller'].tolist()
    seller_list = [eval(p) for p in seller]
    df_seller = pd.DataFrame.from_dict(seller_list)
    df_seller = df_seller[['id', 'sku', 'name', 'link']]
    df_seller.rename(columns={'id': 'seller_id'}, inplace=True)
    # fact sale - seller id - product id
    df_fact = pd.concat([df_tmp, df_seller], axis=1, ignore_index=False)
    df_fact.drop(['sku', 'name', 'link', 'current_seller', 'brand', 'categories'], axis=1, inplace=True)
    # Clean dim seller
    df_seller_dim = df_seller.drop_duplicates(keep='first')

    return df_fact, df_seller_dim


def dimCategory(df_tmp, df_fact):
    # Create dim category
    category = df_tmp['categories'].tolist()
    category_list = [eval(p) for p in category]
    # Tạo url cho subcategory của sp
    for item in category_list:
        id = item['id']
        name = item['name']
        # Xử lý chuỗi Tiếng Việt
        name = name.lower()
        name = name.replace("đ", "d").replace(" ", "-")
        name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore')
        name = name.decode('utf-8')
        item['url'] = f"/{name}/c{id}"

    df_category = pd.DataFrame.from_dict(category_list)
    df_category = df_category[['id','name', 'url']]
    df_category.rename(columns={'id': 'category_id'}, inplace=True)
    # fact sale - seller_id - product_id - category_id
    df_fact = pd.concat([df_fact, df_category], axis=1, ignore_index=False)
    df_fact.drop(['name', 'url'], axis=1, inplace=True)
    # Clean dim category
    df_category_dim = df_category.drop_duplicates(keep='first')

    return df_fact, df_category_dim


def dimBrand(df_tmp, df_fact):
    # Create dim brand
    brand = df_tmp['brand'].tolist()
    brand_list = [eval(p) for p in brand]
    df_brand = pd.DataFrame.from_dict(brand_list)
    df_brand = df_brand[['id', 'name']]
    df_brand.rename(columns={'id': 'brand_id'}, inplace=True)
    # fact sale - seller_id - product_id - category_id - brand_id
    df_fact = pd.concat([df_fact, df_brand], axis=1, ignore_index=False)
    df_fact.drop(['name'], axis=1, inplace=True)
    # Clean dim brand
    df_brand_dim = df_brand.drop_duplicates(keep='first')

    return df_fact, df_brand_dim


def dimProduct(df):
    # Create dim product 
    df_product = df[['id', 'name', 'sku', 'price', 'list_price', 'discount', 'discount_rate', 'inventory_status', 'stock_item']]
    # stock_item: max_sale_qty, qty
    stock = df_product['stock_item'].tolist()
    stock_list = [eval(p) for p in stock]
    df_stock = pd.DataFrame.from_dict(stock_list)
    df_stock = df_stock[['max_sale_qty', 'qty']]
    # Dim Product
    df_product = pd.concat([df_product, df_stock], axis=1, ignore_index=False)
    df_product.drop(['stock_item'], axis=1, inplace=True)
    # Clean Dim Product
    df_product_dim = df_product.drop_duplicates(keep='first')

    return df_product_dim


def exportCSV(df_fact, df_product_dim, df_brand_dim, df_seller_dim, df_category_dim): 
    df_fact.to_csv('dataset/fact_sale.csv', index=False)
    df_product_dim.to_csv('dataset/dim_product.csv', encoding='utf-8-sig', index=False)
    df_brand_dim.to_csv('dataset/dim_brand.csv', encoding='utf-8-sig', index=False)
    df_seller_dim.to_csv('dataset/dim_seller.csv', encoding='utf-8-sig', index=False)
    df_category_dim.to_csv('dataset/dim_category.csv', encoding='utf-8-sig', index=False)


if __name__=='__main__':
    '''Parameters Define'''
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    }
    params = {
        "limit": "40", # limit number of items each page
        "include": "advertisement",
        "aggregation": "2",
        "category": "1846",
        "page": "1", # page number
        "urlkey": "laptop-may-vi-tinh-linh-kien" # product category
    }
    new_api_request_url = "https://tiki.vn/api/v2/products" # new api from tiki developer
    old_api_request_url = "https://tiki.vn/api/personalish/v1/blocks/listings" # current api from tiki webpage
    product_id_list = []
    url = "https://tiki.vn/api/v2/products/{}"
    pars = {"platform": "web", 
            "spid": "124939771"
    }
    product_detail_list = []
    df_id = pd.DataFrame([], columns=["id"])
    df_product = pd.DataFrame([])

    '''Fetch Product ID'''
    with ProcessPoolExecutor(max_workers=5) as executor:
        future = executor.submit(fetchProductID, headers, params, old_api_request_url, product_id_list)
        df_id = future.result()
        df_id.to_csv('dataset/product_id.csv')
    
    list_id = df_id["id"].values.tolist()

    '''Crawl Product Data'''
    start = time.time()
    with ProcessPoolExecutor(max_workers=5) as executor:
        future = executor.submit(crawlProductData, list_id, url, headers, pars, product_detail_list)
        df_product = future.result()
        df_product.to_csv('dataset/product_full_details.csv', encoding='utf-8-sig', index=False)
    print(f"Total time taken: {time.time() - start}")

    '''Extracting Featuring & Creating Star Schema'''
    df = pd.read_csv('dataset/product_full_details.csv')
    df_tmp = df[['current_seller', 'all_time_quantity_sold', 'id', 'brand', 'categories']]
    df_tmp.rename(columns={'id': 'product_id'}, inplace=True)

    df_fact = pd.DataFrame([])
    df_fact, df_seller_dim = dimSeller(df_tmp, df_fact)
    df_fact, df_brand_dim = dimBrand(df_tmp, df_fact)
    df_fact, df_category_dim = dimCategory(df_tmp, df_fact)
    df_product_dim = dimProduct(df)

    '''Export CSV'''
    exportCSV(df_fact, df_product_dim, df_brand_dim, df_seller_dim, df_category_dim)
    


    

# # %%
# # Fill out the list that not reach 2000 items
# df_temp = pd.DataFrame(product_id_list, columns=["id"])
# df_id = pd.concat([df_id, df_temp], ignore_index=True)
# # Truncate all the list
# del product_id_list[:]
# print(len(product_id_list))
# df_id.shape

# # %%
# df_id.head()

# # %%
# list_temp = df_id["id"].values.tolist()

# # %%
# print(len(list_temp))

# # %% [markdown]
# # <b> Fetch Product Details Data <b>

# # %%
# url = "https://tiki.vn/api/v2/products/{}"
# pars = {"platform": "web", 
#         "spid": "124939771"
# }
# product_detail_list = []
# product_json_list = []
# df_product = pd.DataFrame([])

# # %%
# for id in tqdm(list_temp, total=len(list_temp)):
#     response = requests.get('https://tiki.vn/api/v2/products/{}'.format(id), headers=headers, params=pars)
#     if(response.status_code==200):
#         #----------------
#         try:
#             #product_detail_list.append(response.text)
#             product = json.loads(response.text)         
#             product_detail_list.append(product)
#         except:
#             #print("Something else went wrong")
#             pass

#     else:
#         print(f"Error, status code = {response.status_code}")
#         pass

#     # Divided each batch with maximum 2000 items 
#     if(len(product_detail_list) == 2000):
#         print("Reach 2000 items")
#         df_temp = pd.DataFrame.from_dict(product_detail_list)
#         df_product = pd.concat([df_product, df_temp], ignore_index=True)
#         # Truncate all the list
#         del product_detail_list[:]
#         del product_json_list[:]
#         print(len(product_detail_list), len(product_json_list))
    
        

# # %%
# print(len(product_detail_list))
# df_product.shape

# # %%
# # Fill out the list that not reach 2000 items
# df_temp = pd.DataFrame.from_dict(product_detail_list)
# df_product = pd.concat([df_product, df_temp], ignore_index=True)
# # Truncate all the list
# del product_detail_list[:]
# print(len(product_detail_list))
# df_product.shape

# # %%
# df_product.head()

# # %%
# # print(df_product.shape) 
# #df_product.to_csv("product_full_details.csv", encoding='utf-8-sig', index=False)

# # %%
# df = pd.read_csv('product_full_details.csv')
# df.head()

# # %%
# df_tmp = df[['current_seller', 'all_time_quantity_sold', 'id', 'brand', 'categories']]
# df_tmp.rename(columns={'id': 'product_id'}, inplace=True)
# df_tmp.shape # 6367 

# # %%
# # Create dim seller
# seller = df_tmp['current_seller'].tolist()
# seller_list = [eval(p) for p in seller]
# df_seller = pd.DataFrame.from_dict(seller_list)
# df_seller = df_seller[['id', 'sku', 'name', 'link']]
# df_seller.rename(columns={'id': 'seller_id'}, inplace=True)
# # fact sale - seller id - product id
# df_fact = pd.concat([df_tmp, df_seller], axis=1, ignore_index=False)
# df_fact.drop(['sku', 'name', 'link', 'current_seller', 'brand', 'categories'], axis=1, inplace=True)


# # %%
# # Clean dim seller
# print(df_seller.shape) # 6367 
# df_seller_dim = df_seller.drop_duplicates(keep='first')
# print(df_seller_dim.shape) # 48
# df_seller_dim.head()

# # %%
# # Create dim category
# category = df_tmp['categories'].tolist()
# category_list = [eval(p) for p in category]
# # Tạo url cho subcategory của sp
# for item in category_list:
#     id = item['id']
#     name = item['name']
#     # Xử lý chuỗi Tiếng Việt
#     name = name.lower()
#     name = name.replace("đ", "d").replace(" ", "-")
#     name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore')
#     name = name.decode('utf-8')
#     item['url'] = f"/{name}/c{id}"

# df_category = pd.DataFrame.from_dict(category_list)
# df_category = df_category[['id','name', 'url']]
# df_category.rename(columns={'id': 'category_id'}, inplace=True)
# # fact sale - seller_id - product_id - category_id
# df_fact = pd.concat([df_fact, df_category], axis=1, ignore_index=False)
# df_fact.drop(['name', 'url'], axis=1, inplace=True)


# # %%
# # Clean dim category
# print(df_category.shape) # 6367 
# df_category_dim = df_category.drop_duplicates(keep='first')
# print(df_category_dim.shape) # 17
# df_category_dim.head()

# # %%
# # Create dim brand
# brand = df_tmp['brand'].tolist()
# brand_list = [eval(p) for p in brand]
# df_brand = pd.DataFrame.from_dict(brand_list)
# df_brand = df_brand[['id', 'name']]
# df_brand.rename(columns={'id': 'brand_id'}, inplace=True)
# # fact sale - seller_id - product_id - category_id - brand_id
# df_fact = pd.concat([df_fact, df_brand], axis=1, ignore_index=False)
# df_fact.drop(['name'], axis=1, inplace=True)


# # %%
# # Clean dim brand
# print(df_brand.shape) # 6367
# df_brand_dim = df_brand.drop_duplicates(keep='first')
# print(df_brand_dim.shape) # 17
# df_brand_dim.head()

# # %%
# # Create dim product 
# df_product = df[['id', 'name', 'sku', 'price', 'list_price', 'discount', 'discount_rate', 'inventory_status', 'stock_item']]
# df_product.head()

# # %%
# # stock_item: max_sale_qty, qty
# stock = df_product['stock_item'].tolist()
# stock_list = [eval(p) for p in stock]
# df_stock = pd.DataFrame.from_dict(stock_list)
# df_stock = df_stock[['max_sale_qty', 'qty']]
# # Dim Product
# df_product = pd.concat([df_product, df_stock], axis=1, ignore_index=False)
# df_product.drop(['stock_item'], axis=1, inplace=True)

# # %%
# # Clean Dim Product
# print(df_product.shape) # 6367 
# df_product_dim = df_product.drop_duplicates(keep='first')
# print(df_product_dim.shape) # 48 
# df_product_dim.head()

# # %%
# # We dont drop duplicates fact table since it involves to other attributes that we dont extract them to the schema
# print(df_fact.shape) # 6367 
# df_fact.head()

# # %%
# df_fact.to_csv('dataset/fact_sale.csv')
# df_product_dim.to_csv('dataset/dim_product.csv')
# df_brand_dim.to_csv('dataset/dim_brand.csv')
# df_seller_dim.to_csv('dataset/dim_seller.csv')
# df_category_dim.to_csv('dataset/dim_category.csv')


