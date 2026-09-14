try:
    import os
    import plotly.io as pio
    import pandas as pd
    import dapla as dp
    from datetime import datetime
    import numpy as np
    from dapla import FileClient
    import pyarrow.parquet as pq
    import plotly.express as px
    import ipywidgets as widgets
    from IPython.display import display, clear_output, HTML
    import ipywidgets as widgets
    import pyarrow.parquet as pq
except ImportError:
    print(b, "\nDu mangler nødvendige pakker",bb)
    print("Installer nødvendige pakker:\n"
          "Gå til menyen oppe i venstre hjørne: File > New > Terminal\n"
          "I terminalen: skriv inn (uten $) og klikk enter:\n"
          "$ cd datafangst-person\n"
          "$ poetry install")
    exit(1)



def skjema_info(): 
    """
    Oppdaterer og lagrer variabler med info om undersøkelsen etter bruker har valgt undersøkelse i dropdown. Valget blir også lagret så dette slipper å bli gjort i hver notebook. 
    """
    global InstrumentId 
    global skjemanavn 
    b =' \033[1m'
    bb = '\033[0m'
    
    skjemanavn =  dropdown_widget.value
    if skjemanavn != '': 
        filepath = "gs://ssb-datafangst-person-data-produkt-prod/skjemadatabase/*.parquet"
        fs = FileClient.get_gcs_file_system()

        files = fs.glob(filepath) 
        df = (
            pq.ParquetDataset(files, 
                        filesystem=fs, 
                       )
        ).read().to_pandas() 

        df = df.dropna(subset=['InstrumentId'])

        skjemanavn =  dropdown_widget.value
        InstrumentId = df.loc[df['Skjemanavn'] == dropdown_widget.value, 'InstrumentId'].values[0] 
        %store InstrumentId
        %store skjemanavn 
        print(f"\nDu har valgt å se data om {b}{skjemanavn}{bb}, InstrumentId er {InstrumentId}")

        return InstrumentId, skjemanavn
    else: 
        print('velg et skjema i dropdown listen')    
# funksjon for å oppdatere info her


def velg_skjema():

    filepath = "gs://ssb-datafangst-person-data-produkt-prod/skjemadatabase/*.parquet"
    fs = FileClient.get_gcs_file_system()

    files = fs.glob(filepath)
    df = (
        pq.ParquetDataset(files,
                    filesystem=fs,
                   )
    ).read().to_pandas()

    df =df.dropna(subset=['InstrumentId'])


    global dropdown_widget
    global InstrumentId
    global filsti_utvalg
    global skjemanavn
    global siste_spm
    global forste_spm

    # Dato widget
    global start_date_widget
    global end_date_widget


    skjemanavn_values = sorted(df['Skjemanavn'].unique().tolist())

        try:
        %store -r skjemanavn
        if skjemanavn in df['Skjemanavn'].unique().tolist():
            %store -r InstrumentId
            %store -r siste_spm
            skjemanavn_list = sorted(df.query('Skjemanavn != @skjemanavn')['Skjemanavn'].unique().tolist())
            dropdown_widget = widgets.Dropdown(
                options=[skjemanavn] + skjemanavn_list,  # Include navn as the first value
                value=skjemanavn,  # Set the initial value
                description='Velg skjema:'
            )
        else:
            dropdown_widget = widgets.Dropdown(
            options=[''] + list(skjemanavn_values),  # Include navn as the first value
            value='',  # Set the initial value
            description='Velg skjema:'
        )

    except:
        print("Ignorer meldingen 'no stored variable og alias skjemanavn'")
        dropdown_widget = widgets.Dropdown(
            options=[''] + list(skjemanavn_values),  # Include navn as the first value
            value='',  # Set the initial value
            description='Velg skjema:'
        )
     style = {'description_width': 'initial', 'width': '500px', 'max_width': '400px', 'max_height': '100px'}
    dropdown_widget.style = style
    output = widgets.Output()

    # Define a function to handle widget changes
    def on_dropdown_change(change):
        global selected_value
        selected_value = change.new
        with output:
            clear_output()  # Clear previous output
            InstrumentId, skjemanavn = skjema_info()

    # Attach the function to the widget's change event
    dropdown_widget.observe(on_dropdown_change, names='value')

    ## Tekst widget
    text_widget = widgets.HTML(value="La dato stå blank om du ønsker å se på all data for denne undersøkelsen.")

    ## Date widget
    ## Eki: Lagt max dato in start og slutt dato. Hvis brukeren velger dato i framtiden, velges det automatisk dagensdato.
    start_date_widget = widgets.DatePicker(description='Dato fra:', style=style, max = datetime.today().date())
    end_date_widget = widgets.DatePicker(description='Dato til:', style=style, max = datetime.today().date())

    # Display drop down
    display(dropdown_widget, text_widget, start_date_widget, end_date_widget)

    if output is not None:
        display(output)

