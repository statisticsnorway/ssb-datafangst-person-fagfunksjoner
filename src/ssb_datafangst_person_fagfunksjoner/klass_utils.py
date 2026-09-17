# # Eksempel på bruk - hente landsinndeling fra kommunenummer
# # Antar dataene kun har kommunenummer, da må man først hente fylkesinndeling:
# dokrappdf = add_klass_mapping(
#     dokrappdf,
#     column="municipalityNumber",
#     source="Standard for kommuneinndeling",
#     target="Standard for fylkesinndeling",
#     date="2026-09-03",
#     output="code",
#     new_column="fylke"
# )

# # Deretter landsinndeling
# dokrappdf = add_klass_mapping(
#     dokrappdf,
#     column="fylke",
#     source="Standard for fylkesinndeling",
#     target="Standard for landsdelsinndeling",
#     date="2026-09-03",
#     output="name",
#     new_column="landsdel"
# )


import pandas as pd
import requests

from datetime import date as date_type
from io import StringIO


KLASS_BASE_URL = "https://data.ssb.no/api/klass/v1"


def get_klass_id(classification, date):
    """
    Returnerer KLASS-id for en klassifikasjon som er gyldig
    på den angitte datoen.

    Parameters
    ----------
    classification : int or str
        Enten KLASS-id, for eksempel 131,
        eller eksakt klassifikasjonsnavn,
        for eksempel "Standard for kommuneinndeling".

    date : str
        Dato på formatet YYYY-MM-DD.

    Returns
    -------
    int
        KLASS-id.
    """

    if isinstance(classification, int):
        return classification

    if not isinstance(classification, str):
        raise TypeError(
            "classification må være enten int eller str"
        )

    requested_date = date_type.fromisoformat(date)

    url = f"{KLASS_BASE_URL}/classifications/search"

    response = requests.get(
        url,
        params={"query": classification},
        headers={"Accept": "application/json"}
    )
    response.raise_for_status()

    results = (
        response.json()
        .get("_embedded", {})
        .get("searchResults", [])
    )

    exact_matches = {
        result["id"]: result
        for result in results
        if result["name"].casefold() == classification.casefold()
    }

    valid_matches = []

    for classification_id in exact_matches:

        response = requests.get(
            f"{KLASS_BASE_URL}/classifications/{classification_id}",
            headers={"Accept": "application/json"}
        )
        response.raise_for_status()

        details = response.json()

        for version in details.get("versions", []):

            valid_from = date_type.fromisoformat(
                version["validFrom"]
            )

            valid_to = (
                date_type.fromisoformat(version["validTo"])
                if version.get("validTo")
                else None
            )

            is_valid = (
                valid_from <= requested_date
                and (
                    valid_to is None
                    or requested_date < valid_to
                )
            )

            if is_valid:
                valid_matches.append(classification_id)
                break

    if len(valid_matches) == 1:
        return valid_matches[0]

    if len(valid_matches) == 0:
        raise ValueError(
            f"Fant ingen KLASS-klassifikasjon med navn "
            f"'{classification}' som var gyldig {date}"
        )

    raise ValueError(
        f"Fant flere KLASS-klassifikasjoner med navn "
        f"'{classification}' som var gyldige {date}: "
        f"{valid_matches}"
    )


def add_klass_mapping(
    df,
    column,
    source,
    target,
    date,
    output="code",
    new_column="klass"
):
    """
    Legger til en kolonne i en DataFrame basert på en
    korrespondanse mellom to klassifikasjoner i SSB KLASS.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame som skal utvides.

    column : str
        Kolonnen i df som inneholder source-kodene.

    source : int or str
        KLASS-id eller eksakt navn på source-klassifikasjonen.

    target : int or str
        KLASS-id eller eksakt navn på target-klassifikasjonen.

    date : str
        Dato på formatet YYYY-MM-DD.
        Mappingen som var gyldig på denne datoen brukes.

    output : {"code", "name"}, default "code"
        Om resultatet skal være target-kode eller target-navn.

    new_column : str, default "klass"
        Navn på den nye kolonnen.

    Returns
    -------
    pandas.DataFrame
        Samme DataFrame med ny kolonne.
    """

    if output not in {"code", "name"}:
        raise ValueError(
            "output må være 'code' eller 'name'"
        )

    source_id = get_klass_id(source, date)
    target_id = get_klass_id(target, date)

    url = (
        f"{KLASS_BASE_URL}/classifications/"
        f"{source_id}/correspondsAt"
    )

    response = requests.get(
        url,
        params={
            "targetClassificationId": target_id,
            "date": date
        },
        headers={
            "Accept": "text/csv; charset=UTF-8"
        }
    )

    response.raise_for_status()

    klass_map = pd.read_csv(
        StringIO(response.text),
        dtype="string"
    )

    target_column = {
        "code": "targetCode",
        "name": "targetName"
    }[output]

    mapping = (
        klass_map
        .drop_duplicates("sourceCode")
        .set_index("sourceCode")[target_column]
    )

    df[new_column] = (
        df[column]
        .astype("string")
        .map(mapping)
    )

    return df
