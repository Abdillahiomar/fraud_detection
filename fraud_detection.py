# -*- coding: utf-8 -*-
"""
Created on Sun Jun  8 17:01:55 2025

@author: HP
"""
import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np

st.set_page_config(page_title="Détection Scénario RDS->MCH->SD ", layout="wide")
st.title("🕵️ Détection Améliorée de Scénarios RDS->MCH->SD")

uploaded_file = st.file_uploader("📤 Charger le fichier CSV des transactions", type=["csv"])

if uploaded_file:
    CHUNKSIZE = 100_000  # nombre de lignes par morceau
    suspicious = []

    chunk_iter = pd.read_csv(uploaded_file, chunksize=CHUNKSIZE)

    for chunk in chunk_iter:
        # Prétraitement local du chunk
        chunk['INITATE_DATE'] = pd.to_datetime(chunk['INITATE_DATE'], errors='coerce')
        chunk['REASON_NAME'] = chunk['REASON_NAME'].astype(str).str.strip().str.lower()
        chunk['DEBIT_MSISDN'] = chunk['DEBIT_MSISDN'].astype(str).str.strip()
        chunk['CREDIT_MSISDN'] = chunk['CREDIT_MSISDN'].astype(str).str.strip()
        chunk['DATE'] = chunk['INITATE_DATE'].dt.date

        for day in chunk['DATE'].dropna().unique():
            daily = chunk[chunk['DATE'] == day]
            mp = daily[daily['REASON_NAME'].str.contains("merchant payment", na=False)]

            for _, mp_row in mp.iterrows():
                merchant = mp_row['CREDIT_MSISDN']
                client = mp_row['DEBIT_MSISDN']
                amount = mp_row['ACTUAL_AMOUNT']
                time1 = mp_row['INITATE_DATE']

                bco = daily[
                    (daily['REASON_NAME'].str.contains("cash out", na=False)) &
                    (daily['DEBIT_MSISDN'] == merchant) &
                    (daily['ACTUAL_AMOUNT'] == amount) &
                    (daily['INITATE_DATE'] > time1)
                ]

                for _, bco_row in bco.iterrows():
                    bco_time = bco_row['INITATE_DATE']
                    cashout_to = bco_row['CREDIT_MSISDN']
                    delay = (bco_time - time1).total_seconds() / 60

                    risk_score = 0
                    flags = []

                    if delay < 10:
                        risk_score += 40
                        flags.append("Cashout rapide (<10 min)")

                    if amount >= 20000:
                        risk_score += 30
                        flags.append("Montant élevé (>=20,000)")

                    if client == cashout_to:
                        risk_score += 30
                        flags.append("Client identique au receveur cashout")

                    if risk_score == 0:
                        risk_score = 10
                        flags.append("Activité inhabituelle détectée")

                    suspicious.append({
                        'date': day,
                        'merchant': merchant,
                        'client': client,
                        'mp_time': time1,
                        'bco_time': bco_time,
                        'delay_minutes': delay,
                        'amount': amount,
                        'mp_reason': mp_row['REASON_NAME'],
                        'bco_reason': bco_row['REASON_NAME'],
                        'cashout_to': cashout_to,
                        'risk_score': risk_score,
                        'flags': "; ".join(flags),
                    })

    result_df = pd.DataFrame(suspicious)

    if not result_df.empty:
        grouped_df = result_df.groupby(['date', 'merchant']).agg(
            nb_cas=('amount', 'count'),
            montant_total=('amount', 'sum'),
            score_moyen=('risk_score', 'mean')
        ).reset_index()

        st.subheader("📋 Détails Cas Individuels avec Scoring")
        st.dataframe(result_df)

        st.subheader("📊 Résumé Groupé avec Score Moyen")
        st.dataframe(grouped_df)

        # Export Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            result_df.to_excel(writer, sheet_name="Détails Cas Individuels", index=False)
            grouped_df.to_excel(writer, sheet_name="Résumé Groupé", index=False)
        st.download_button("📥 Télécharger les résultats Excel", data=output.getvalue(),
                           file_name="analyse_circulaire_amelioree.xlsx")
    else:
        st.warning("Aucun scénario circulaire suspect détecté.")
else:
    st.info("Veuillez charger un fichier CSV des transactions pour démarrer l'analyse.")
