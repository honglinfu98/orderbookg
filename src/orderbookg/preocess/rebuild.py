import glob
import os
import re
import pandas as pd
from collections import OrderedDict
from orderbookg.settings import PROJECT_ROOT


def infer_tick_size(prices):
    """
    Infer the tick size based on the precision of the prices.
    
    Args:
        prices (list): A list of prices (float).
        
    Returns:
        float: The inferred tick size as 10^(-decimal_places).
    """
    # Extract the number of decimal places for each price
    decimal_places = [
        len(str(price).split('.')[1]) if '.' in str(price) else 0
        for price in prices
    ]
    # Find the maximum precision across all prices
    max_decimal_places = max(decimal_places)
    # Tick size is 10^(-max_decimal_places)
    return 10**(-max_decimal_places)


def process_order_book(
    commodity: str,
    exchange: str,
    instrument: str,
    month: str,
    level: int,
) -> None:
    input_file_path = f"{PROJECT_ROOT}/data/raw/{exchange}/{instrument}/{commodity}/{month}"
    csv_file_list = glob.glob(
        f"{input_file_path}/*.gz"
    )  # Get the list of all the .csv files in the input_path directory.
    processed_output_file_path = f"{PROJECT_ROOT}/data/processed/{exchange}/{instrument}/{commodity}/{month}"
    statistics_output_file_path = f"{PROJECT_ROOT}/data/results/{exchange}/{instrument}/{commodity}/{month}"

    csv_orderbook = [
        name for name in csv_file_list if "order_book" in name
    ]  # Get the list of all the orderbook files in the input_path directory.
    csv_orderbook.sort()  # Sort the list of orderbook files.

    feature_names_raw = [
    "ASKp",
    "ASKs",
    "BIDp",
    "BIDs",
    ]  # Define sorted raw features' names.

    feature_names = []
    for i in range(1,  11):
        for j in range(4):
            feature_names += [
                feature_names_raw[j] + str(i)
            ]  # Add to raw features' names the level number.

    statistics_columns = ['mid_price', 'spread', 'best_ask_volume', 'best_bid_volume' ,'volume_imbalance']


    for orderbook_name in csv_orderbook[:2]:
        print(orderbook_name)

        # read the CSV file into a Pandas DataFrame
        orderbook_df = pd.read_csv(orderbook_name, delimiter=';')

        # Find the index of the first row with type 's'
        first_s_index = orderbook_df[orderbook_df['type'] == 's'].index[0]

        # Slice the dataframe to keep only rows with index >= first_s_index || Get rid of the first couple useless lines
        orderbook_df = orderbook_df.loc[first_s_index:]

        # Reset the index of the dataframe
        orderbook_df = orderbook_df.reset_index(drop=True)

        order_book_snapshots = []

        order_book_snapshot_statistics = []

        # create an empty order book dictionary using OrderedDict
        order_book = {'asks': OrderedDict(), 'bids': OrderedDict()}

        # loop over the rows in the DataFrame
        for i, row in orderbook_df.iterrows():
            # parse the asks and bids from the row
            asks = eval(row['asks'])
            bids = eval(row['bids'])
            
            # initialize the order book with the first snapshot or update it with the subsequent rows (there can be multiple type:s in a single file, in this case the order book snapshot is the new snapshot to be considered)
            if row['type'] == 's':

                # reset the order book with the new snapshot
                order_book = {'asks': OrderedDict(), 'bids': OrderedDict()}
                for price, amount in asks:
                    order_book['asks'][price] = amount
                for price, amount in bids:
                    order_book['bids'][price] = amount
                
            else:
                
                # loop over the asks and update the order book
                for price, amount in asks:
                    if amount == 0:
                        # remove the price level from the order book
                        if price in order_book['asks']:
                            del order_book['asks'][price]
                    else:
                        # update the amount in the order book
                        order_book['asks'][price] = amount
                
                # loop over the bids and update the order book
                for price, amount in bids:
                    if amount == 0:
                        # remove the price level from the order book
                        if price in order_book['bids']:
                            del order_book['bids'][price]
                    else:
                        # update the amount in the order book
                        order_book['bids'][price] = amount
                
                # Sort the order_book asks by price in ascending order
                order_book['asks'] = OrderedDict(sorted(order_book['asks'].items()))
                # Sort the order_book bids by price in descending order
                order_book['bids'] = OrderedDict(sorted(order_book['bids'].items(), reverse=True))

                # Save the level of the order book
                # Save the lowest 10 of the asks and its amount
                order_book_asks_level = list(order_book['asks'].items())[:level]
                # Save the highest 10 of the bids and its amount
                order_book_bids_level = list(order_book['bids'].items())[:level]

                # Save the order book snapshot as a row in pandas DataFrame with the correspond feature names
                order_book_snapshot = [row['timestamp']]
                for i in range(level):
                    order_book_snapshot += list(order_book_asks_level[i])
                    order_book_snapshot += list(order_book_bids_level[i])

                order_book_snapshots.append(order_book_snapshot)



                # save the spread for each snapshot using the formula: (best_ask - best_ask) / best_ask
                # access the best ask using ordered dictionary's first element
                best_ask_p = next(iter(order_book['asks']))
                # access the best bid using ordered dictionary's last element
                best_bid_p = next(iter(order_book['bids']))
                # access the best ask amount using ordered dictionary
                best_ask_a = order_book['asks'][best_ask_p]
                # access the best bid amount using ordered dictionary
                best_bid_a = order_book['bids'][best_bid_p]
                # mid price is the hightest bid price plus the lowest ask price divided by 2
                mid_price = (best_ask_p + best_bid_p) / 2
                # spread = (best_ask_p - best_bid_p) / best_ask_p
                spread = (best_ask_p - best_bid_p) / mid_price

                # actual ask depth
                

                # best ask volume 
                best_ask_v = best_ask_p * best_ask_a
                # best bid volume
                best_bid_v = best_bid_p * best_bid_a


                # calculate the mid price
                mid_price = (best_ask_p + best_bid_p) / 2
                # $\omega_t = \frac{V_t^b - V_t^a}{V_t^b + V_t^a} \in (-1, 1)$
                # $V_t^b, V_t^a$ where represent the liquidity posted at the best bid and the best ask, respectively, at time t
                volume_imbalance = (best_bid_v - best_ask_v) / (best_bid_v + best_ask_v)

                order_book_snapshot_statistic = [row['timestamp']]

                order_book_snapshot_statistics.append(order_book_snapshot_statistic + [mid_price,spread, best_ask_v, best_bid_v ,volume_imbalance])

                # Save the order_book_snapshot as pandas DataFrame

        order_book_snapshot_df = pd.DataFrame(order_book_snapshots, columns=['timestamp'] + feature_names)

        order_book_snapshot_statistics_df = pd.DataFrame(order_book_snapshot_statistics, columns=['timestamp'] + statistics_columns)
        #tick size
        tick_size = infer_tick_size(list(order_book["asks"].keys())+ list(order_book["bids"].keys()))
        # add one more column to the statistics dataframe for the tick size
        order_book_snapshot_statistics_df['tick_size'] = tick_size 

        # Save the order_book_snapshot_df and order_book_snapshot_statistics_df as CSV files  
        match = re.search(r'full_order_book_(.*?)\.csv', orderbook_name)

        # check if the above directory exists. If now, create it
        if not os.path.exists(processed_output_file_path):
            os.makedirs(processed_output_file_path)
        if not os.path.exists(statistics_output_file_path):
            os.makedirs(statistics_output_file_path)
 
        order_book_snapshot_df.to_csv(f"{processed_output_file_path}/{match[0]}", header=True, index=False)
        order_book_snapshot_statistics_df.to_csv(f"{statistics_output_file_path}/{match[0]}", header=True, index=False)



if __name__ == "__main__": 
    level = 10
    commodity = "btcusdt"
    exchange = "bnus"
    instrument = "spot"
    month = "2024-08"
    level = 10
    process_order_book(commodity, exchange, instrument, month, level)

