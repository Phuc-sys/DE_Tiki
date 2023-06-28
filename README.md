# Objective
Building a project to crawl data from TIKI webpage, transform the data and then load it to MySQL.
# Overview
## Step 1: Get Product ID
First you go to TIKI webpage (https://tiki.vn/) and select a category you wish to list out products. In this example I chose category: laptop-may-vi-tinh-linh-kien.

**Right-click mouse to inspect the page, click on 'Fetch/XHR' and refresh the page to get the list of API in column Name, then I chose the API which return the data of each page (see image below)**

![image](https://github.com/Phuc-sys/DE_Tiki/assets/81355271/2a6de03d-4802-428a-95b8-440aff1cdd7d)

![Screenshot (40)](https://github.com/Phuc-sys/DE_Tiki/assets/81355271/3b717c41-5b85-49b7-9f65-9ca36e368d00)


In the image below, there are 2 API to fetch the Product ID, one is the API I get from above defined as the 'old_api_request_url', the other is from developer documents (You can choose either). The parameter 'urlkey' is where you define the category you wish to.

![image](https://github.com/Phuc-sys/DE_Tiki/assets/81355271/c64640b5-7dae-4199-a666-ca5216eccb99)

## Step 2: Get Product Data
You select any product and repeat the step above to get the API that return the product details.

![Screenshot (41)](https://github.com/Phuc-sys/DE_Tiki/assets/81355271/a1f2d4fe-1a29-4a25-ae85-f69cc2ba0ed7)

![Screenshot (42)](https://github.com/Phuc-sys/DE_Tiki/assets/81355271/01a696d4-fb0c-44b7-9115-2070ca95f13b)

## Step 3: Design DW Star Schema
From the product attributes, I extracted important featuring and devided them into dimension table which called dim_table.

![Screenshot 2023-06-25 224409](https://github.com/Phuc-sys/DE_Tiki/assets/81355271/48cc5d6c-0456-4a56-8698-56253dbc56c1)

## Step 4: Convert to pyhon file and apply ProcessPoolExecutor
Since the dataset is not big enough to obserrve the difference of time in crawling, a part due to initializing the executor does take time.

## Step 5: Import data to MySQL 
I have tried new tools which are Apache Nifi and Talend Open Studio, see more in 2 folders (apache_nifi and talend_open_studio). Unfortunately, I have tried on Apache Nifi but it dont success.






