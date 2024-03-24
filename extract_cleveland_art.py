# https://openaccess-api.clevelandart.org/api/creators/?nationality=french&birth_year_before=1940
# https://openaccess-api.clevelandart.org/api/creators/
import requests
import pandas as pd
import datetime


def assignment_clevelandart(nationality, birth_year_after, death_year_before):
    """
    From the API endpoint in the comment, extract data of creators who are english in nationationality,
    birth year after 1960 and death year before 2020
    Your function should return data in JSON format
    """
    BASE_URL = "https://openaccess-api.clevelandart.org/api/creators/"
    params = {
    "nationality":nationality,
    "birth_year_after": birth_year_after,
    "death_year_before": death_year_before
    }
    try:
        cleveland = requests.get(BASE_URL, params=params)
        data = cleveland.json()

    except Exception as e:
        print(f'Error {e} Failed loading data from {BASE_URL}')

    return data


Results = assignment_clevelandart('American',1960 ,2020) 

print(Results)

"""TRANSFORMATION"""
def transform(extracted_data):
    """Perform transformation on json file and load to a dataframe

    Parameters:
      extracted_data : input data in json format

    Output:
      df : pandas dataframe of transformed data

    """

    # Extract data from the JSON
    creators = extracted_data['data']

    transformed_data = []

    # Iterate through each creator
    for creator in creators:
        name = creator['name']
        nationality = creator['nationality']
        description = creator['description']
        birth_year = int(creator['birth_year'])
        death_year = int(creator['death_year'])

        # Flatten the nested structure of artworks
        artworks = creator['artworks']
        for artwork in artworks:
            artwork_details = {
                'name': name,
                'nationality': nationality,
                'description': description,
                'birth_year': birth_year,
                'death_year': death_year,
                'artwork_id': artwork['id'],
                'accession_number': artwork['accession_number'],
                'artwork_title': artwork['title'],
                'artwork_tombstone': artwork['tombstone'],
                'artwork_url': artwork['url']
            }

            # Append transformed data to the list
            transformed_data.append(artwork_details)

    # Create DataFrame from transformed data
    df = pd.DataFrame(transformed_data)

    return df

# Call the transformation function on the extracted data
transformed_df = transform(Results)

# Display the transformed DataFrame
print(transformed_df.head())


"""LOADING"""
def load_to_csv(transformed_df, filename):
  cur_date = datetime.datetime.now()
  formatted_date = cur_date.strftime("%Y-%m-%d")

  transformed_df.to_csv(f"{filename}_{formatted_date}.csv")


loading = load_to_csv(transformed_df=transformed_df, filename="ClevelandData")