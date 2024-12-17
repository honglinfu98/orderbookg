import os
from google.cloud import storage
from orderbookg.settings import PROJECT_ROOT
# Set the path to your credentials file

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

def download_blob(bucket_name, source_blob_name, destination_file_name):
    try:
        print(f"Attempting to download: {source_blob_name}")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        # Create the destination file name in the current directory
        # destination_file_name = os.path.basename(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")
    except Exception as e:
        print(f"Error downloading {source_blob_name}: {e}")

def get_commodities_for_exchanges_and_instruments(bucket_name, exchanges, instruments, base_prefix):
    try:
        storage_client = storage.Client()
        result = {}  # Dictionary to store results

        for exchange in exchanges:
            result[exchange] = {}  # Initialize dictionary for each exchange
            for instrument in instruments:
                specific_level_prefix = f"{base_prefix}/{exchange}/{instrument}/"
                print(f"\nListing commodities under: {specific_level_prefix}")

                blobs = storage_client.list_blobs(bucket_name, prefix=specific_level_prefix, delimiter='/')
                commodities = set()

                for page in blobs.pages:
                    for prefix in page.prefixes:
                        commodity_name = prefix.split('/')[-2]  # Get the commodity name at the specified level
                        commodities.add(commodity_name)

                # Save the list of commodities for the specific exchange and instrument
                result[exchange][instrument] = list(commodities)

        return result
    except Exception as e:
        print(f"Error in get_commodities_for_exchanges_and_instruments: {e}")
        return None

def get_dates_for_commodities(bucket_name, available_options, base_prefix):
    try:
        storage_client = storage.Client()
        result_with_dates = {}  # Dictionary to store results with dates

        for exchange, instruments in available_options.items():
            result_with_dates[exchange] = {}
            for instrument, commodities in instruments.items():
                result_with_dates[exchange][instrument] = {}

                for commodity in commodities:
                    specific_level_prefix = f"{base_prefix}/{exchange}/{instrument}/{commodity}/"
                    print(f"\nListing dates under: {specific_level_prefix}")

                    blobs = storage_client.list_blobs(bucket_name, prefix=specific_level_prefix, delimiter='/')
                    dates = set()

                    for page in blobs.pages:
                        for prefix in page.prefixes:
                            date_name = prefix.split('/')[-2]  # Get the date directory name
                            dates.add(date_name)

                    # Save the list of dates for the specific exchange, instrument, and commodity
                    result_with_dates[exchange][instrument][commodity] = list(dates)

        return result_with_dates
    except Exception as e:
        print(f"Error in get_dates_for_commodities: {e}")
        return None


def get_filenames_for_dates(bucket_name, available_options, base_prefix):
    try:
        storage_client = storage.Client()
        result_with_filenames = {}  # Dictionary to store results with filenames

        for exchange, instruments in available_options.items():
            result_with_filenames[exchange] = {}
            for instrument, commodities in instruments.items():
                result_with_filenames[exchange][instrument] = {}

                for commodity in commodities:
                    result_with_filenames[exchange][instrument][commodity] = {}
                    
                    # Now we find the dates for each commodity
                    commodity_level_prefix = f"{base_prefix}/{exchange}/{instrument}/{commodity}/"
                    blobs = storage_client.list_blobs(bucket_name, prefix=commodity_level_prefix, delimiter='/')
                    
                    dates = set()
                    for page in blobs.pages:
                        for prefix in page.prefixes:
                            date_name = prefix.split('/')[-2]
                            dates.add(date_name)

                    # For each date, find the filenames
                    for date in dates:
                        specific_date_prefix = f"{commodity_level_prefix}{date}/"
                        print(f"\nListing filenames under: {specific_date_prefix}")

                        date_blobs = storage_client.list_blobs(bucket_name, prefix=specific_date_prefix)
                        filenames = [blob.name.split('/')[-1] for blob in date_blobs]  # Extract filenames

                        # Save the filenames under the specific exchange, instrument, commodity, and date
                        result_with_filenames[exchange][instrument][commodity][date] = filenames

        return result_with_filenames
    except Exception as e:
        print(f"Error in get_filenames_for_dates: {e}")
        return None
    


# The filename is like "full_order_book_bbit_perpetual-future_specusdt_2024-08-18.csv.gz". Help me to use the exchange, instrument, commodity, and date to get a f string and download the file.
def download_specific_file(bucket_name, exchange, instrument, commodity, month, date):
    try:
        raw_data_file_path = f"{PROJECT_ROOT}/data/raw/{exchange}/{instrument}/{commodity}/{month}"
        # check if the above directory exists. If now, create it
        if not os.path.exists(raw_data_file_path):
            os.makedirs(raw_data_file_path)

        filename = f"full_order_book_{exchange}_{instrument}_{commodity}_{date}.csv.gz"
        destination_file_name = f"{raw_data_file_path}/{filename}"
        download_blob(bucket_name, f"full_order_book/{exchange}/{instrument}/{commodity}/{month}/{filename}", destination_file_name)
    except Exception as e:
        print(f"Error in download_specific_files: {e}")




if __name__ == "__main__":
    #     # Define your bucket name, exchanges list, instruments list, and base prefix
    # bucket_name = 'kaiko-delivery-cloudburst'
    # exchanges = [
    #     'bbit', 'bbsp', 'bfly', 'bfnx', 'binc', 'bnus', 'bthb', 'btmx', 'cbse', 
    #     'delt', 'drbt', 'eris', 'gmni', 'hbdm', 'huob', 'itbi', 'krkn', 'nvdx', 
    #     'okcn', 'okex', 'oslx', 'stmp', 'upbt'
    # ]
    # instruments = ['spot', 'perpetual-future', 'etf', 'option_combo', 'future_combo']
    # base_prefix = 'full_order_book'  # Main folder in the URL path

    # # Get commodities at the same level as 'btc01nov24' for each exchange and instrument
    # available_options = get_commodities_for_exchanges_and_instruments(bucket_name, exchanges, instruments, base_prefix)

    # # Print the structured result
    # for exchange, instruments in available_options.items():
    #     print(f"\nExchange: {exchange}")
    #     for instrument, commodities in instruments.items():
    #         print(f"  Instrument: {instrument} -> Commodities: {commodities}")



    # # Get dates at the next level under each commodity
    # result_with_dates = get_dates_for_commodities(bucket_name, available_options, base_prefix)

    # # Print the structured result with dates
    # for exchange, instruments in result_with_dates.items():
    #     print(f"\nExchange: {exchange}")
    #     for instrument, commodities in instruments.items():
    #         print(f"  Instrument: {instrument}")
    #         for commodity, dates in commodities.items():
    #             print(f"    Commodity: {commodity} -> Dates: {dates}")


    # # Get filenames at the next level under each date
    # result_with_filenames = get_filenames_for_dates(bucket_name, available_options, base_prefix)

    # # Print the structured result with filenames
    # for exchange, instruments in result_with_filenames.items():
    #     print(f"\nExchange: {exchange}")
    #     for instrument, commodities in instruments.items():
    #         print(f"  Instrument: {instrument}")
    #         for commodity, dates in commodities.items():
    #             print(f"    Commodity: {commodity}")
    #             for date, filenames in dates.items():
    #                 print(f"      Date: {date} -> Filenames: {filenames}")


    # download specific file
    date_string_ls = []
    for i in range(18,22):
        date_string_ls.append(f"2024-08-{i}")
    for i in date_string_ls:
        download_specific_file(bucket_name, 'bnus', 'spot', 'btcusdt', '2024-08' ,i)

