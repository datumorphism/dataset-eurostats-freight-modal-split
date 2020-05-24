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
    df["nuts_2"] = df["tra_infr,unit,geo\\time"].apply(lambda x: x.split(",")[-1])
    df["country"] = df["nuts_2"].apply(lambda x: x[:2])
    df["unit"] = df["tra_infr,unit,geo\\time"].apply(lambda x: x.split(",")[-2])
    # df["transport_infrastructure"] = df["tra_infr,unit,geo\\time"].apply(lambda x: x.split(",")[0])
    df["transport_infrastructure"] = "motorways"

    cols = [i for i in df.columns.tolist() if i != "tra_infr,unit,geo\\time"]
    info_cols = ["transport_infrastructure", "country", "nuts_2", "unit"]
    year_cols = [i for i in cols if i not in info_cols]

    # Melt the dataframe for easier pandas manipulations
    df_melted = pd.melt(df, id_vars=info_cols, value_vars=year_cols, var_name="year", value_name="value")

    # Sort the column order for a more readable output
    order_cols = [
        'transport_infrastructure', 'country', 'nuts_2', 'year', 'value', 'unit'
    ]
    df_melted = df_melted[order_cols]

    # Remove : in the values
    df_melted.value = df_melted.value.apply(lambda x: x.strip() if isinstance(x, str) else x)
    df_melted.replace({":": None, ": ": None}, inplace=True)
    df_melted.year = df_melted.year.apply(lambda x: x.strip())

    # Attach the name of the column
    # df_nuts_2 = get_nuts_codes('nuts_2')
    # df_res = pd.merge(df_melted, df_nuts_2, how="left", left_on="nuts_2", right_on="nuts_code")
    # df_res.drop("nuts_code", axis=1, inplace=True)
    # df_res.rename(columns={
    #     "nuts_2_y": "nuts_name",
    #     "nuts_2_x": "nuts_code"
    # }, inplace=True)

    # Split into two tables

    df_unit_km = df_melted.loc[df_melted.unit == "KM"]
    df_unit_km_per_thousand_square_km = df_melted.loc[df_melted.unit == "KM_TKM2"]

    # export
    df_unit_km.to_csv(
        parsed_data_local["unit_km"], index=False
    )
    df_unit_km_per_thousand_square_km.to_csv(
        parsed_data_local["unit_km_per_thousand_square_km"], index=False
    )



if __name__ == "__main__":

    DATA_REMOTE = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2Ftgs00114.tsv.gz"
    CACHE_FOLDER = "/tmp"

    DATA_LOCAL = "/tmp/motorway-network.tsv"
    PARSED_DATA_LOCAL = {
        "unit_km": "dataset/motorway_network_unit_km.csv",
        "unit_km_per_thousand_square_km": "dataset/motorway_network_unit_km_per_thousand_square_km.csv"
    }

    download(DATA_REMOTE, CACHE_FOLDER, DATA_LOCAL)

    parse_data(DATA_LOCAL, PARSED_DATA_LOCAL)

    pass