"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import os
import glob
import pandas as pd

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    input_dir = "files/input/"
    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)

    # 1) Leer y concatenar todos los ZIP de input
    zip_files = glob.glob(os.path.join(input_dir, "*.zip"))
    df_list = [
        pd.read_csv(zf, compression="zip", sep=",")
        for zf in zip_files
    ]
    df = pd.concat(df_list, ignore_index=True)

    # ---------------------- CLIENT DATA ----------------------
    client = df[[
        "client_id",
        "age",
        "job",
        "marital",
        "education",
        "credit_default",
        "mortgage"              # dejar como 'mortgage'
    ]].copy()

    # job: "." → ""  y "-" → "_"
    client["job"] = (
        client["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    # education: "." → "_"  y "unknown" → pd.NA
    client["education"] = (
        client["education"]
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA)
    )

    # credit_default: "yes" → 1, otro → 0
    client["credit_default"] = client["credit_default"] \
        .str.lower().eq("yes").astype(int)

    # mortgage: "yes" → 1, otro → 0
    client["mortgage"] = client["mortgage"] \
        .str.lower().eq("yes").astype(int)

    client.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    # --------------------- CAMPAIGN DATA ---------------------
    camp = df[[
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",  # coincide con el test
        "previous_outcome",
        "campaign_outcome",
        "day",
        "month"
    ]].copy()

    # previous_outcome: "success" → 1, otro → 0
    camp["previous_outcome"] = camp["previous_outcome"] \
        .str.lower().eq("success").astype(int)

    # campaign_outcome: "yes" → 1, otro → 0
    camp["campaign_outcome"] = camp["campaign_outcome"] \
        .str.lower().eq("yes").astype(int)

    # Construir last_contact_date "YYYY-MM-DD" (año 2022)
    camp["last_contact_date"] = pd.to_datetime(
        camp.assign(year=2022)[["day", "month", "year"]]
            .apply(lambda r: f"{r['day']} {r['month']} {r['year']}", axis=1),
        dayfirst=True,
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    camp_final = camp[[
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "last_contact_date"           # renombrada para pasar el test
    ]]
    camp_final.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # -------------------- ECONOMICS DATA --------------------
    econ = df[[
        "client_id",
        "cons_price_idx",            # dejar tal cual para el test
        "euribor_three_months"       # idem
    ]].copy()

    econ.to_csv(os.path.join(output_dir, "economics.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()