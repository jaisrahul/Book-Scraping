import requests
from bs4 import BeautifulSoup as soup
from db_connections import Db_Connection
from curd_operations import Curd_Operation

def insert_books_data(List_books):
    columns = ['name','price','ratings','availability'] 
    rows = List_books

    schema_name = "books"
    table_name = "scrapdata"
    primarykey="name"
    
    file_name1 = "C://Users//AMIT KUMAR PATHAK//OneDrive//Desktop//TailNode//part_B//create_schema.pgsql"
    file_name2 = "C://Users//AMIT KUMAR PATHAK//OneDrive//Desktop//TailNode//part_B//create_scrapdata_table.pgsql"
    try:
        db = Curd_Operation(schema_name, table_name)
        db.create_table(file_name1)
        db.create_table(file_name2)
        db.insert_many(columns, rows)

    except Exception as err:
        print(f'Other error occurred: {err}') 

    else:
        print('Success!')


def data_extract(page_soup):
    List_books=[]
    for i in page_soup.findAll("article",{"class":"product_pod"}):
        row=[]
        name        =    i.div.img["alt"]
        price       =    i.find("p",attrs={"class":"price_color"}).text
        rating      =    i.find('p').get('class')[1]
        availability=    i.find('p',attrs={'class':"instock availability"}).text
        availability=availability.strip()
        row.append(name)
        row.append(price)
        row.append(rating)
        row.append(availability)
        List_books.append(row)
    
    insert_books_data(List_books)

def url_fetch():
    for i in range(1,51):
        url = f"http://books.toscrape.com/catalogue/page-{i}.html"

        req_page = requests.get(url)
        req_page.close()
        page_soup = soup(req_page.content,"html.parser")
        data_extract(page_soup)

if __name__ == "__main__":
    url_fetch()