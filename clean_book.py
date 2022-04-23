import pandas as pd 
#function to clean book-genre 
# book-genre value should be unique pair in book dimension table
# we assume first instance book-genre pair as correct entry
def clean_book_genre(df_book):
    df_book_cleaned = pd.DataFrame(columns=['book','genre'])
    master_list_book =[]
    for index,row in df_book.iterrows():
        if row['book'] not in master_list_book:
            df_book_cleaned = df_book_cleaned.append(row, ignore_index = True)
            master_list_book.append(row['book'])
    return df_book_cleaned