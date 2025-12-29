import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np

st.set_page_config(page_title="Détection des Scénarios des fraudes ", layout="wide")
st.title("🕵️ Détection des Scénarios de fraude")

uploaded_file = st.file_uploader("📤 Charger le fichier CSV des transactions", type=["csv"])

if uploaded_file:
    # ✅ Sauvegarde en mémoire du fichier pour le réutiliser plusieurs fois
    file_bytes = uploaded_file.read()
    file_buffer = BytesIO(file_bytes)

    CHUNKSIZE = 100_000
    suspicious = []
    repeat_clients = []
    repeat_w2b = []
    repeat_cashin = []
    cashin_then_w2b = []
    b2w_then_w2b = []

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
            mp_all.groupby(['DEBIT_MSISDN', 'CREDIT_MSISDN'])
            .size()
            .reset_index(name='nb_paiements')
        )
        repeat = repeat[repeat['nb_paiements'] > 2]
        
        
        if not repeat.empty:
            repeat_clients.append(repeat)
        
        # Détection des clients répétitifs de Cash In
        cashin_all = chunk[chunk['REASON_NAME'].str.contains("customer cash in", na=False)] 
        repeat_1 = (
            cashin_all.groupby(['DEBIT_MSISDN', 'CREDIT_MSISDN'])
            .size()
            .reset_index(name='nb_cashin')
        )
        repeat_1 = repeat_1[repeat_1['nb_cashin'] >=2 ]
        
        # Détection des clients répétitifs de W2B
        all_w2b = chunk[chunk['REASON_NAME'].str.contains("w2b", na=False)] 
        repeate_w2b = (
            all_w2b.groupby([ 'DEBIT_MSISDN','CREDIT_MSISDN'])
            .size()
            .reset_index(name='nb_W2B')
        )
        repeate_w2b = repeate_w2b[repeate_w2b['nb_W2B'] >=2 ]
        
        
        
        if not repeat.empty:
            repeat_cashin.append(repeat_1)
            repeat_w2b.append(repeate_w2b)
            #repeat_cashin.append(repeat_reg_cash_in)

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
                        risk_score += 90
                        flags.append("Montant élevé (>=20,000)")

                    if client == cashout_to:
                        risk_score += 30
                        flags.append("Client identique au receveur cashout")

                    if cashin_from == cashout_to:
                        risk_score += 100
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
    
    cashin_fragmente = []
    for chunk in pd.read_csv(BytesIO(file_bytes), chunksize=CHUNKSIZE):
        chunk['REASON_NAME'] = chunk['REASON_NAME'].astype(str).str.strip().str.lower()
        chunk['DEBIT_MSISDN'] = chunk['DEBIT_MSISDN'].astype(str).str.strip()
        chunk['CREDIT_MSISDN'] = chunk['CREDIT_MSISDN'].astype(str).str.strip()
        
        cashin = chunk[chunk['REASON_NAME'].str.contains("customer cashin", na=False)]
        if not cashin.empty:
            grouped_cashin = (
                cashin.groupby('CREDIT_MSISDN')
                .agg(
                    volume=('ACTUAL_AMOUNT', 'count'),
                    valeur=('ACTUAL_AMOUNT', 'sum')
                )
                .reset_index()
                .rename(columns={'DEBIT_MSISDN': 'client'})
            )
            cashin_fragmente.append(grouped_cashin)
    cahin_df = pd.concat(cashin_fragmente, ignore_index=True) if cashin_fragmente else pd.DataFrame()
                                                         

    # 🧭 Affichage côte à côte
    if repeat_clients:
        repeat_df = pd.concat(repeat_clients, ignore_index=True)
        repeat_cashin_df = pd.concat(repeat_cashin, ignore_index=True)
        repeat_w2b_df = pd.concat(repeat_w2b, ignore_index=True)
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.subheader("⚠️ Paiement Marchant >2")
            if not repeat_df.empty:
                st.dataframe(repeat_df)
            else:
                st.info("Aucune paiement marchant.")

        with col2:
            st.subheader("✨ Points de Fidélité")
            if not redeem_df.empty:
                st.dataframe(redeem_df)
            else:
                st.info("Aucune conversion de points détectée.")
        with col3:
            st.subheader("✨ CASH IN")
            if not repeat_cashin_df.empty:
                st.dataframe(repeat_cashin_df)
            else:
                st.info("Aucune transaction de Cash In.")
        with col4:
            st.subheader("✨ W2B")
            if not repeate_w2b.empty:
                st.dataframe(repeat_w2b_df)
            else:
                st.info("Aucune transaction de Cash In.")
            
        

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
        
    # 🔍 Détection Cash In → W2B (client)
    chunk['INITATE_DATE'] = pd.to_datetime(chunk['INITATE_DATE'], errors='coerce')
    chunk['DATE'] = chunk['INITATE_DATE'].dt.date
    
    cashin_tx = chunk[chunk['REASON_NAME'].str.contains("customer cash in", na=False)]
    w2b_tx = chunk[chunk['REASON_NAME'].str.contains("w2b", na=False)]
    
    for day in chunk['DATE'].dropna().unique():
        daily_cashin = cashin_tx[cashin_tx['DATE'] == day]
        daily_w2b = w2b_tx[w2b_tx['DATE'] == day]
    
        for _, ci in daily_cashin.iterrows():
            client = ci['CREDIT_MSISDN']
            ci_time = ci['INITATE_DATE']
    
            w2b_after = daily_w2b[
                (daily_w2b['DEBIT_MSISDN'] == client) &
                (daily_w2b['INITATE_DATE'] > ci_time)
            ]
    
            for _, w2b in w2b_after.iterrows():
                delay = (w2b['INITATE_DATE'] - ci_time).total_seconds() / 60
    
                cashin_then_w2b.append({
                    'date': day,
                    'Distributeur' : ci['DEBIT_MSISDN'],
                    'client': client,
                    'cashin_amount': ci['ACTUAL_AMOUNT'],
                    'cashin_time': ci_time,
                    'w2b_amount': w2b['ACTUAL_AMOUNT'],
                    'w2b_time': w2b['INITATE_DATE'],
                    'Banque': w2b['CREDIT_MSISDN'],
                    'delay_minutes': delay,
                    'scenario': 'Cash In suivi de W2B'
                })


        if cashin_then_w2b:
            st.subheader("🚨 Cash In suivi de W2B")
            st.dataframe(pd.DataFrame(cashin_then_w2b))
        
    
    # 📊 Analyse des répétitions Cash In suivi de W2B
    if cashin_then_w2b:
        scenario_df_cashin_w2b = pd.DataFrame(cashin_then_w2b)
    
        repetition_df_cashin_then_w2b = (
            scenario_df_cashin_w2b
            .groupby(['Distributeur', 'client'])
            .agg(
                nb_occurrences_1=('scenario', 'count'),
                montant_total_b2w_1 = ('cashin_amount', 'sum'),
                montant_total_w2b_1=('w2b_amount', 'sum'),
                premiere_date_1=('date', 'min'),
                derniere_date_1=('date', 'max')
            )
            .reset_index()
        )
    
        # Filtrer uniquement les couples suspects
        repetition_df_cashin_then_w2b = repetition_df_cashin_then_w2b[repetition_df_cashin_then_w2b['nb_occurrences_1'] >= 1]
    
        st.subheader("🚩 Couples SD  → RDS répétant le scénario cashin -> w2b")
        if not repetition_df_cashin_then_w2b.empty:
            st.dataframe(repetition_df_cashin_then_w2b)
        else:
            st.info("Aucun couple n’a répété ce scénario plus d’une fois.")
    
    # 🔍 Détection B2W → Send Money → W2B (Client A → Client B)
    b2w_send_w2b = []
    
    chunk['INITATE_DATE'] = pd.to_datetime(chunk['INITATE_DATE'], errors='coerce')
    chunk['DATE'] = chunk['INITATE_DATE'].dt.date
    
    b2w_tx = chunk[chunk['REASON_NAME'].str.contains("b2w", na=False)]
    send_tx = chunk[chunk['REASON_NAME'].str.contains("send money", na=False)]
    w2b_tx = chunk[chunk['REASON_NAME'].str.contains("w2b", na=False)]
    
    for day in chunk['DATE'].dropna().unique():
        daily_b2w = b2w_tx[b2w_tx['DATE'] == day]
        daily_send = send_tx[send_tx['DATE'] == day]
        daily_w2b = w2b_tx[w2b_tx['DATE'] == day]
    
        for _, b2w in daily_b2w.iterrows():
            client_a = b2w['CREDIT_MSISDN']
            b2w_time = b2w['INITATE_DATE']
    
            # Send Money de A → B après B2W
            send_after = daily_send[
                (daily_send['DEBIT_MSISDN'] == client_a) &
                (daily_send['INITATE_DATE'] > b2w_time)
            ]
    
            for _, sm in send_after.iterrows():
                client_b = sm['CREDIT_MSISDN']
                sm_time = sm['INITATE_DATE']
    
                # W2B de B après réception
                w2b_after = daily_w2b[
                    (daily_w2b['DEBIT_MSISDN'] == client_b) &
                    (daily_w2b['INITATE_DATE'] > sm_time)
                ]
    
                for _, w2b in w2b_after.iterrows():
                    delay_1 = (sm_time - b2w_time).total_seconds() / 60
                    delay_2 = (w2b['INITATE_DATE'] - sm_time).total_seconds() / 60
    
                    b2w_send_w2b.append({
                        'date': day,
                        'Source Bank': b2w['DEBIT_MSISDN'],
                        'client_A': client_a,
                        'b2w_amount': b2w['ACTUAL_AMOUNT'],
                        'b2w_time': b2w_time,
                        'client_B': client_b,
                        'send_amount': sm['ACTUAL_AMOUNT'],
                        'send_time': sm_time,
                        'w2b_amount': w2b['ACTUAL_AMOUNT'],
                        'w2b_time': w2b['INITATE_DATE'],
                        'Destination Bank': w2b['CREDIT_MSISDN'],
                        'delay_B2W_to_Send_min': delay_1,
                        'delay_Send_to_W2B_min': delay_2,
                        'scenario': 'B2W → Send Money → W2B'
                    })
    
    # 📊 Affichage
    if b2w_send_w2b:
        st.subheader("🚨 B2W → Send Money → W2B")
        st.dataframe(pd.DataFrame(b2w_send_w2b))
    else:
        st.info("Aucun scénario B2W → Send Money → W2B détecté.")

    
    # 📊 Analyse des répétitions B2W → Send Money → W2B
    if b2w_send_w2b:
        scenario_df = pd.DataFrame(b2w_send_w2b)
    
        repetition_df = (
            scenario_df
            .groupby(['client_A', 'client_B'])
            .agg(
                nb_occurrences=('scenario', 'count'),
                montant_total_b2w = ('b2w_amount', 'sum'),
                montant_total_send=('send_amount', 'sum'),
                montant_total_w2b=('w2b_amount', 'sum'),
                premiere_date=('date', 'min'),
                derniere_date=('date', 'max')
            )
            .reset_index()
        )
    
        # Filtrer uniquement les couples suspects
        repetition_df = repetition_df[repetition_df['nb_occurrences'] >= 1]
    
        st.subheader("🚩 Couples Client A → Client B répétant le scénario")
        if not repetition_df.empty:
            st.dataframe(repetition_df)
        else:
            st.info("Aucun couple n’a répété ce scénario plus d’une fois.")



