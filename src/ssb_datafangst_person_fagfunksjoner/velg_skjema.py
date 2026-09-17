import os
#import plotly.io as pio
import pandas as pd
import dapla as dp
from datetime import datetime
import numpy as np
#from dapla import FileClient
import pyarrow.parquet as pq
#import plotly.express as px
from IPython.display import display, clear_output, HTML
import ipywidgets as widgets    


def velg_skjema():
    from pathlib import Path
    import pyarrow.parquet as pq
    

    files = [
        str(p)
        for p in Path("/buckets/produkt/skjemadatabase").glob("*.parquet")
    ]

    df = pq.ParquetDataset(files).read().to_pandas()
    df = df.dropna(subset=["InstrumentId"])

    skjemanavn_values = sorted(df["Skjemanavn"].unique())

    dropdown_widget = widgets.Dropdown(
        options=[""] + skjemanavn_values,
        value="",
        description="Velg skjema:",
    )

    style = {
        "description_width": "initial",
        "width": "500px",
        "max_width": "400px",
        "max_height": "100px",
    }

    dropdown_widget.style = style
    output = widgets.Output()

    start_date_widget = widgets.DatePicker(
        description="Dato fra:",
        style=style,
        max=datetime.today().date(),
    )

    end_date_widget = widgets.DatePicker(
        description="Dato til:",
        style=style,
        max=datetime.today().date(),
    )

    valg = {
        "InstrumentId": None,
        "skjemanavn": None,
        "start_dato": None,
        "slutt_dato": None
    }

    def on_dropdown_change(change):
        skjemanavn = change.new

        if not skjemanavn:
            return

        instrument_id = df.loc[
            df["Skjemanavn"] == skjemanavn,
            "InstrumentId",
        ].iloc[0]

        valg["InstrumentId"] = instrument_id
        valg["skjemanavn"] = skjemanavn

        with output:
            clear_output()
            print(
                f"Du har valgt {skjemanavn}, "
                f"InstrumentId er {instrument_id}"
            )

    def on_start_date_change(change):
        valg["start_dato"] = change.new

    def on_end_date_change(change):
        valg["slutt_dato"] = change.new

    dropdown_widget.observe(on_dropdown_change, names="value")
    start_date_widget.observe(on_start_date_change, names="value",)
    end_date_widget.observe(on_end_date_change, names="value",)

    text_widget = widgets.HTML(
        value="La dato stå blank om du ønsker å se på all data for denne undersøkelsen."
    )

    display(
        dropdown_widget,
        text_widget,
        start_date_widget,
        end_date_widget,
        output,
    )

    return valg

# +
# def skjema_info(): 
#     """
#     Oppdaterer og lagrer variabler med info om undersøkelsen etter bruker har valgt undersøkelse i dropdown. Valget blir også lagret så dette slipper å bli gjort i hver notebook. 
#     """
#     global InstrumentId 
#     global skjemanavn 
#     b =' \033[1m'
#     bb = '\033[0m'
    
#     skjemanavn =  dropdown_widget.value
#     if skjemanavn != '': 
#         filepath = "gs://ssb-datafangst-person-data-produkt-prod/skjemadatabase/*.parquet"
#         fs = FileClient.get_gcs_file_system()

#         files = fs.glob(filepath) 
#         df = (
#             pq.ParquetDataset(files, 
#                         filesystem=fs, 
#                        )
#         ).read().to_pandas() 

#         df = df.dropna(subset=['InstrumentId'])

#         skjemanavn =  dropdown_widget.value
#         InstrumentId = df.loc[df['Skjemanavn'] == dropdown_widget.value, 'InstrumentId'].values[0] 
#         %store InstrumentId
#         %store skjemanavn 
#         print(f"\nDu har valgt å se data om {b}{skjemanavn}{bb}, InstrumentId er {InstrumentId}")

#         return InstrumentId, skjemanavn
#     else: 
#         print('velg et skjema i dropdown listen')    
# # funksjon for å oppdatere info her
# -



