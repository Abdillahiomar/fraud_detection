import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np

st.set_page_config(page_title="Détection Scénario RDS->MCH->SD ", layout="wide")
st.title("🕵️ Détection Améliorée de Scénarios RDS->MCH->SD")

uploaded_file = st.file_uploader("📤 Charger le fichier CSV des transactions", type=["csv"])

if uploaded_file:
    # ✅ Sauvegarde en mémoire du fichier pour le réutiliser plusieurs fois
    file_bytes = uploaded_file.read()
    file_buffer = BytesIO(file_bytes)

    CHUNKSIZE = 100_000
    suspicious = []
    repeat_clients = []

    # Première lecture pour la détection principale
    chunk_iter = pd.read_csv(BytesIO(file_bytes), chunksize=CHUNKSIZE)

    for chunk in chunk_iter:
        # Prétraitement local du chunk
        chunk['INITATE_DATE'] = pd.to_datetime(chunk['INITATE_DATE'], errors='coerce')
        chunk['REASON_NAME'] = chunk['REASON_NAME'].astype(str).str.strip().str.lower()
        chunk['DEBIT_MSISDN'] = chunk['DEBIT_MSISDN'].astype(str).str.strip()
        chunk['CREDIT_MSISDN'] = chunk['CREDIT_MSISDN'].astype(str).str.strip()
        chunk['DATE'] = chunk['INITATE_DATE'].dt.date

        # Détection des clients répétitifs
        mp_all = chunk[chunk['REASON_NAME'].str.contains("merchant payment", na=False)]
        repeat = (
            mp_all.groupby(['DATE', 'DEBIT_MSISDN', 'CREDIT_MSISDN'])
            .size()
            .reset_index(name='nb_paiements')
        )
        repeat = repeat[repeat['nb_paiements'] > 2]
        if not repeat.empty:
            repeat_clients.append(repeat)

        # 🔍 Analyse des scénarios circulaires
        for day in chunk['DATE'].dropna().unique():
            daily = chunk[chunk['DATE'] == day]
            mp = daily[daily['REASON_NAME'].str.contains("merchant payment", na=False)]

            for _, mp_row in mp.iterrows():
                merchant = mp_row['CREDIT_MSISDN']
                client = mp_row['DEBIT_MSISDN']
                amount = mp_row['ACTUAL_AMOUNT']
                time1 = mp_row['INITATE_DATE']

                # Dernier Cash In du client avant paiement
                ci = daily[
                    (daily['REASON_NAME'].str.contains("cash in", na=False)) &
                    (daily['CREDIT_MSISDN'] == client) &
                    (daily['INITATE_DATE'] < time1)
                ].sort_values(by='INITATE_DATE', ascending=False).head(1)

                if ci.empty:
                    continue

                cashin_from = ci.iloc[0]['DEBIT_MSISDN']
                ci_time = ci.iloc[0]['INITATE_DATE']

                # Cash Out du marchand après paiement
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

                    if cashin_from == cashout_to:
                        risk_score += 50
                        flags.append("Même distributeur CashIn & CashOut")

                    if risk_score == 0:
                        risk_score = 10
                        flags.append("Activité inhabituelle détectée")

                    suspicious.append({
                        'date': day,
                        'cashin_from': cashin_from,
                        'ci_time': ci_time,
                        'client': client,
                        'merchant': merchant,
                        'mp_time': time1,
                        'mp_reason': mp_row['REASON_NAME'],
                        'bco_time': bco_time,
                        'bco_reason': bco_row['REASON_NAME'],
                        'amount': amount,
                        'cashout_to': cashout_to,
                        'delay_minutes': delay,
                        'risk_score': risk_score,
                        'flags': "; ".join(flags),
                    })

    # 🟡 Ré-analyse pour "Customer Redeem Point to Balance"
    redeem_df_list = []
    for chunk in pd.read_csv(BytesIO(file_bytes), chunksize=CHUNKSIZE):
        chunk['REASON_NAME'] = chunk['REASON_NAME'].astype(str).str.strip().str.lower()
        chunk['DEBIT_MSISDN'] = chunk['DEBIT_MSISDN'].astype(str).str.strip()
        chunk['CREDIT_MSISDN'] = chunk['CREDIT_MSISDN'].astype(str).str.strip()

        redeem = chunk[chunk['REASON_NAME'].str.contains("customer redeem point to balance", na=False)]
        if not redeem.empty:
            grouped_redeem = (
                redeem.groupby('CREDIT_MSISDN')
                .agg(
                    volume=('ACTUAL_AMOUNT', 'count'),
                    valeur=('ACTUAL_AMOUNT', 'sum')
                )
                .reset_index()
                .rename(columns={'DEBIT_MSISDN': 'client'})
            )
            redeem_df_list.append(grouped_redeem)

    redeem_df = pd.concat(redeem_df_list, ignore_index=True) if redeem_df_list else pd.DataFrame()

    # 🧭 Affichage côte à côte
    if repeat_clients:
        repeat_df = pd.concat(repeat_clients, ignore_index=True)
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("⚠️ Clients ayant effectué >2 paiements / marchand / jour")
            st.dataframe(repeat_df)

        with col2:
            st.subheader("✨ Clients ayant converti des points de fidélité")
            if not redeem_df.empty:
                st.dataframe(redeem_df)
            else:
                st.info("Aucune conversion de points détectée.")

    # 📊 Affichage des résultats principaux
    result_df = pd.DataFrame(suspicious)
    if not result_df.empty:
        grouped_df = result_df.groupby(['date', 'merchant']).agg(
            nb_cas=('amount', 'count'),
            montant_total=('amount', 'sum'),
            score_moyen=('risk_score', 'mean')
        ).reset_index()

        st.subheader("📋 Détails Cas Individuels")
        st.dataframe(result_df)

        st.subheader("📊 Résumé Groupé")
        st.dataframe(grouped_df)
    else:
        st.warning("Aucun scénario circulaire suspect détecté.")
