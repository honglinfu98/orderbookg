
import glob
import os
import re
import numpy as np
import pandas as pd

from orderbookg.settings import PROJECT_ROOT

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


def data_process(
    coin: str,
    exchange: str,
    instrument: str,
    month: str,
    # processed_input_file_path: str,
    # features_output_file_path: str,
    # log_file_path: str,
    horizons: [int],
    normalization_window: int,
    scaling: bool,
) -> None:
    processed_input_file_path = f"{PROJECT_ROOT}/data/processed/{exchange}/{instrument}/{coin}/{month}"
    csv_file_list = glob.glob(
        f"{processed_input_file_path}/*.csv"
    )  # Get the list of all the .csv files in the input_path directory.
    features_output_file_path = f"{PROJECT_ROOT}/data/features/{exchange}/{instrument}/{coin}/{month}"
    
    processed_orderbook = [
        name for name in csv_file_list if "order_book" in name
    ]  # Get the list of all the orderbook files in the input_path directory.
    processed_orderbook.sort()  # Sort the list of orderbook files.

    print(f"Data preprocessing loop started. SCALING: {str(scaling)}.")
    logs = []

    # Initialize dataframes for dynamic Z-score normalization.
    mean_df = pd.DataFrame()
    mean2_df = pd.DataFrame()
    nsamples_df = pd.DataFrame()


    for processed_orderbook_name in processed_orderbook:

        processed_orderbook = pd.read_csv(processed_orderbook_name)

        processed_orderbook.insert(
            0, "mid_price", (processed_orderbook["ASKp1"] + processed_orderbook["BIDp1"]) / 2
        )  # Add the mid-price column to the orderbook dataframe.


        # Dynamic z-score normalization.
        orderbook_mean_df = pd.DataFrame(
            processed_orderbook[feature_names].mean().values.reshape(-1, len(feature_names)),
            columns=feature_names,
        )
        orderbook_mean2_df = pd.DataFrame(
            (processed_orderbook[feature_names] ** 2)
            .mean()
            .values.reshape(-1, len(feature_names)),
            columns=feature_names,
        )
        orderbook_nsamples_df = pd.DataFrame(
            np.array([[len(processed_orderbook)]] * len(feature_names)).T,
            columns=feature_names,
        )

        if len(mean_df) < normalization_window:
            logs.append(
                f"{processed_orderbook_name} skipped. Initializing rolling z-score normalization."
            )
            # Don't save the first <normalization_window> days as we don't have enough days to normalize.
            mean_df = pd.concat([mean_df, orderbook_mean_df], ignore_index=True)
            mean2_df = pd.concat([mean2_df, orderbook_mean2_df], ignore_index=True)
            nsamples_df = pd.concat(
                [nsamples_df, orderbook_nsamples_df], ignore_index=True
            )
            continue
        else:
            z_mean_df = pd.DataFrame(
                (nsamples_df * mean_df).sum(axis=0) / nsamples_df.sum(axis=0)
            ).T  # Dynamically compute mean.
            z_stdev_df = pd.DataFrame(
                np.sqrt(
                    (nsamples_df * mean2_df).sum(axis=0) / nsamples_df.sum(axis=0)
                    - z_mean_df ** 2
                )
            )  # Dynamically compute standard deviation.

            # Broadcast to df_orderbook size.
            z_mean_df = z_mean_df.loc[z_mean_df.index.repeat(len(processed_orderbook))]
            z_stdev_df = z_stdev_df.loc[z_stdev_df.index.repeat(len(processed_orderbook))]
            z_mean_df.index = processed_orderbook.index
            z_stdev_df.index = processed_orderbook.index
            if scaling is True:
                processed_orderbook[feature_names] = (processed_orderbook[feature_names] - z_mean_df) / z_stdev_df  # Apply normalization.

            # Roll forward by dropping first rows and adding most recent mean and mean2.
            mean_df = mean_df.iloc[1:, :]
            mean2_df = mean2_df.iloc[1:, :]
            nsamples_df = nsamples_df.iloc[1:, :]

            mean_df = pd.concat([mean_df, orderbook_mean_df], ignore_index=True)
            mean2_df = pd.concat([mean2_df, orderbook_mean2_df], ignore_index=True)
            nsamples_df = pd.concat(
                [nsamples_df, orderbook_nsamples_df], ignore_index=True
            )

        # Create labels with simple delta prices.
        rolling_mid = processed_orderbook["mid_price"]
        rolling_mid = rolling_mid.to_numpy().flatten()
        for h in horizons:·
            delta_ticks = rolling_mid[h:] - processed_orderbook["mid_price"][:-h]
            processed_orderbook[f"Raw_Target_{str(h)}"] = delta_ticks

        # Create labels applying smoothing.
        for h in horizons:·
            rolling_mid_minus = processed_orderbook['mid_price'].rolling(window=h, min_periods=h).mean().shift(h)
            rolling_mid_plus = processed_orderbook["mid_price"].rolling(window=h, min_periods=h).mean().to_numpy().flatten()
            smooth_pct_change = rolling_mid_plus - rolling_mid_minus
            processed_orderbook[f"Smooth_Target_{str(h)}"] = smooth_pct_change

        # Drop the mid-price column and transform seconds column into a readable format.
        processed_orderbook = processed_orderbook.drop(["mid_price"], axis=1)

        # Drop elements which cannot be used for training.
        processed_orderbook = processed_orderbook.dropna()
        processed_orderbook.drop_duplicates(inplace=True, keep='last', subset='timestamp')

        # Save processed files.
        # Save the order_book_snapshot_df and order_book_snapshot_statistics_df as CSV files  
        match = re.search(r'full_order_book_(.*?)\.csv', processed_orderbook_name)

        # check if the above directory exists. If now, create it
        if not os.path.exists(features_output_file_path):
            os.makedirs(features_output_file_path)
 
        processed_orderbook.to_csv(f"{features_output_file_path}/{match[0]}", header=True, index=False)

        logs.append(f"{processed_orderbook_name} completed.")
        print(f"{processed_orderbook_name} completed.")


if __name__ == "__main__":
    data_process(
        coin="btcusdt",
        exchange="bnus", 
        instrument="spot",
        month="2024-08",
        # log_file_path="./data/logs/",
        horizons=[1, 5, 10],
        normalization_window=2,
        scaling=True,
    )