import os
import requests
import gzip, io
import logging
import pandas as pd

logging.basicConfig()
logger = logging.getLogger(__name__)




def download(data_remote, intermediate_local, data_local):
    """
    download the remote data and clean up locally

    :param data_remote: Remote data link
    :type data_remote: str
    :param intermediate_local: intermediate files
    :type intermediate_local: str
    :param data_local: local file folder
    :type data_local: str
    """

    req = requests.get(data_remote)

    inter_cache = os.path.join(intermediate_local, "cache.gz")

    with open(inter_cache, "wb") as fp:
        fp.write(req.content)

    if os.path.isfile(inter_cache):
        logger.info(f"{inter_cache}")


    with gzip.GzipFile(inter_cache, "rb") as gz_b:
        data_content = gz_b.read()

    with open(data_local, 'wb') as fp:
        fp.write(data_content)


def get_nuts_codes(level):

    df = pd.read_csv(
        # "https://raw.githubusercontent.com/datumorphism/dataset-eu-nuts/master/dataset/nuts_v2016__2018_2020.csv"
        "https://raw.githubusercontent.com/datumorphism/dataset-eu-nuts/master/dataset/nuts_v2021__2021_.csv"
    )

    df = df.loc[~df[level].isna()]

    return df[["nuts_code", level]]


def parse_data(data_local, parsed_data_local):

    df = pd.read_csv(data_local, sep="\t")
    df.drop_duplicates(inplace=True)

    logger.info(f"{df.describe()}\n{df.info()}")

    # Split the first column
    df["country"] = df["unit,tra_mode,geo\\time"].apply(lambda x: x.split(",")[-1])
    df["transport_mode"] = df["unit,tra_mode,geo\\time"].apply(lambda x: x.split(",")[-2])
    # df["unit"] = df["unit,tra_mode,geo\\time"].apply(lambda x: x.split(",")[0])

    cols = [i for i in df.columns.tolist() if i != "unit,tra_mode,geo\\time"]
    info_cols = ["transport_mode", "country"]
    year_cols = [i for i in cols if i not in info_cols]

    # Melt the dataframe for easier pandas manipulations
    df_melted = pd.melt(
        df, id_vars=info_cols, value_vars=year_cols, var_name="year", value_name="value"
    )

    # Sort the column order for a more readable output
    order_cols = ['transport_mode', 'country', 'year', 'value']
    df_melted = df_melted[order_cols]

    # Transport mode full name
    MODES = {
        "IWW": "inland_waterways",
        "RAIL": "rail",
        "RAIL_IWW_AVD": "rail_inland_waterways_sum_of_available",
        "ROAD": "road"
    }

    df_melted.transport_mode.replace(MODES, inplace=True)

    # Clean up values
    df_melted.year = df_melted.year.apply(lambda x: x.strip())

    df_melted.value = df_melted.value.apply(lambda x: x.strip() if isinstance(x, str) else x)
    df_melted["is_estimated"] = df_melted.value.apply(lambda x: True if "e" in x else False)
    df_melted["not_applicable"] = df_melted.value.apply(lambda x: True if "z" in x else False)

    df_melted.value = df_melted.value.apply(lambda x: x.split(" ")[0].strip() if isinstance(x, str) else x)
    df_melted.value.replace({":": None, ": ": None}, inplace=True)
    df_melted.value = df_melted.value.apply(lambda x: f"{float(x)/100:0.3f}" if x else None)

    # Export
    df_melted.to_csv(
        parsed_data_local, index=False
    )




if __name__ == "__main__":

    DATA_REMOTE = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&downfile=data%2Ft2020_rk320.tsv.gz"
    CACHE_FOLDER = "/tmp"

    DATA_LOCAL = "/tmp/freight_modal_split.tsv"
    PARSED_DATA_LOCAL = "dataset/eurostats_freight_modal_split.csv"

    download(DATA_REMOTE, CACHE_FOLDER, DATA_LOCAL)

    parse_data(DATA_LOCAL, PARSED_DATA_LOCAL)

    pass