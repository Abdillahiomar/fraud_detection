import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np
from datetime import datetime

st.set_page_config(page_title="Détection des Scénarios de fraude", layout="wide")
st.title("🕵️ Détection des Scénarios de fraude")

uploaded_file = st.file_uploader("📤 Charger le fichier CSV des transactions", type=["csv"])

if uploaded_file:
    # ✅ Lecture unique du fichier avec optimisations
    with st.spinner("Chargement et prétraitement des données..."):
        df = pd.read_csv(
            uploaded_file,
            dtype={
                'DEBIT_MSISDN': 'str',
                'CREDIT_MSISDN': 'str',
                'REASON_NAME': 'str',
                'ACTUAL_AMOUNT': 'float32'
            },
            parse_dates=['INITATE_DATE']
        )
        
        # Prétraitement global une seule fois
        df['REASON_NAME'] = df['REASON_NAME'].str.strip().str.lower()
        df['DEBIT_MSISDN'] = df['DEBIT_MSISDN'].str.strip()
        df['CREDIT_MSISDN'] = df['CREDIT_MSISDN'].str.strip()
        df['DATE'] = df['INITATE_DATE'].dt.date
        
        # Créer des indexes pour accélérer les filtres
        df = df.sort_values('INITATE_DATE').reset_index(drop=True)

    # ✅ Pré-filtrage par type de transaction (une seule fois)
    with st.spinner("Classification des transactions..."):
        mp_all = df[df['REASON_NAME'].str.contains("merchant payment", na=False)].copy()
        cashin_all = df[df['REASON_NAME'].str.contains("customer cash in", na=False)].copy()
        cashout_all = df[df['REASON_NAME'].str.contains("cash out", na=False)].copy()
        w2b_all = df[df['REASON_NAME'].str.contains("w2b", na=False)].copy()
        b2w_all = df[df['REASON_NAME'].str.contains("b2w", na=False)].copy()
        send_all = df[df['REASON_NAME'].str.contains("send money", na=False)].copy()
        redeem_all = df[df['REASON_NAME'].str.contains("customer redeem point to balance", na=False)].copy()

    # ==========================================
    # 1️⃣ DÉTECTIONS SIMPLES (Agrégations)
    # ==========================================
    with st.spinner("Détection des patterns répétitifs..."):
        
        # Paiements marchands répétitifs
        repeat_df = (
            mp_all.groupby(['DEBIT_MSISDN', 'CREDIT_MSISDN'], as_index=False)
            .size()
            .rename(columns={'size': 'nb_paiements'})
            .query('nb_paiements > 2')
        )
        
        # Cash In répétitifs
        repeat_cashin_df = (
            cashin_all.groupby(['DEBIT_MSISDN', 'CREDIT_MSISDN'], as_index=False)
            .size()
            .rename(columns={'size': 'nb_cashin'})
            .query('nb_cashin >= 2')
        )
        
        # W2B répétitifs
        repeat_w2b_df = (
            w2b_all.groupby(['DEBIT_MSISDN', 'CREDIT_MSISDN'], as_index=False)
            .size()
            .rename(columns={'size': 'nb_W2B'})
            .query('nb_W2B >= 2')
        )
        
        # Points de fidélité
        redeem_df = (
            redeem_all.groupby('CREDIT_MSISDN', as_index=False)
            .agg(
                volume=('ACTUAL_AMOUNT', 'count'),
                valeur=('ACTUAL_AMOUNT', 'sum')
            )
        )
        
        # Cash In fragmenté
        cashin_df = (
            cashin_all.groupby('CREDIT_MSISDN', as_index=False)
            .agg(
                volume=('ACTUAL_AMOUNT', 'count'),
                valeur=('ACTUAL_AMOUNT', 'sum')
            )
        )

    # 📊 Affichage des résultats simples
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.subheader("⚠️ Paiement Marchand >2")
        if not repeat_df.empty:
            st.dataframe(repeat_df, use_container_width=True)
        else:
            st.info("Aucun paiement marchand répétitif.")

    with col2:
        st.subheader("✨ Points de Fidélité")
        if not redeem_df.empty:
            st.dataframe(redeem_df, use_container_width=True)
        else:
            st.info("Aucune conversion de points.")

    with col3:
        st.subheader("✨ CASH IN")
        if not repeat_cashin_df.empty:
            st.dataframe(repeat_cashin_df, use_container_width=True)
        else:
            st.info("Aucun Cash In répétitif.")

    with col4:
        st.subheader("✨ W2B")
        if not repeat_w2b_df.empty:
            st.dataframe(repeat_w2b_df, use_container_width=True)
        else:
            st.info("Aucun W2B répétitif.")

    # ==========================================
    # 2️⃣ DÉTECTION CIRCULAIRE OPTIMISÉE
    # ==========================================
    with st.spinner("Analyse des scénarios circulaires (optimisée)..."):
        suspicious = []
        
        # Grouper par jour une seule fois
        mp_daily = mp_all.set_index('DATE').sort_values('INITATE_DATE')
        cashin_daily = cashin_all.set_index('DATE').sort_values('INITATE_DATE')
        cashout_daily = cashout_all.set_index('DATE').sort_values('INITATE_DATE')
        
        unique_days = mp_all['DATE'].unique()
        
        for day in unique_days:
            if day not in mp_daily.index:
                continue
                
            mp_day = mp_daily.loc[day] if isinstance(mp_daily.loc[day], pd.DataFrame) else mp_daily.loc[[day]]
            
            if day not in cashin_daily.index or day not in cashout_daily.index:
                continue
                
            cashin_day = cashin_daily.loc[day] if isinstance(cashin_daily.loc[day], pd.DataFrame) else cashin_daily.loc[[day]]
            cashout_day = cashout_daily.loc[day] if isinstance(cashout_daily.loc[day], pd.DataFrame) else cashout_daily.loc[[day]]
            
            # Créer des dictionnaires pour accès rapide
            cashin_by_client = cashin_day.groupby('CREDIT_MSISDN')
            cashout_by_merchant = cashout_day.groupby('DEBIT_MSISDN')
            
            for _, mp_row in mp_day.iterrows():
                merchant = mp_row['CREDIT_MSISDN']
                client = mp_row['DEBIT_MSISDN']
                amount = mp_row['ACTUAL_AMOUNT']
                time1 = mp_row['INITATE_DATE']
                
                # Dernier Cash In du client
                if client not in cashin_by_client.groups:
                    continue
                    
                ci_group = cashin_by_client.get_group(client)
                ci_before = ci_group[ci_group['INITATE_DATE'] < time1]
                
                if ci_before.empty:
                    continue
                    
                ci = ci_before.iloc[-1]  # Le plus récent
                cashin_from = ci['DEBIT_MSISDN']
                ci_time = ci['INITATE_DATE']
                
                # Cash Out du marchand
                if merchant not in cashout_by_merchant.groups:
                    continue
                    
                bco_group = cashout_by_merchant.get_group(merchant)
                bco_matches = bco_group[
                    (bco_group['ACTUAL_AMOUNT'] == amount) &
                    (bco_group['INITATE_DATE'] > time1)
                ]
                
                for _, bco_row in bco_matches.iterrows():
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
                        flags.append("Client = receveur cashout")
                    
                    if cashin_from == cashout_to:
                        risk_score += 100
                        flags.append("Même distributeur CashIn & CashOut")
                    
                    if risk_score == 0:
                        risk_score = 10
                        flags.append("Activité inhabituelle")
                    
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

    # Affichage scénarios circulaires
    result_df = pd.DataFrame(suspicious)
    if not result_df.empty:
        grouped_df = result_df.groupby(['date', 'merchant']).agg(
            nb_cas=('amount', 'count'),
            montant_total=('amount', 'sum'),
            score_moyen=('risk_score', 'mean')
        ).reset_index()

        st.subheader("📋 Détails Cas Individuels")
        st.dataframe(result_df, use_container_width=True)

        st.subheader("📊 Résumé Groupé")
        st.dataframe(grouped_df, use_container_width=True)
    else:
        st.warning("Aucun scénario circulaire suspect.")

    # ==========================================
    # 3️⃣ SCÉNARIOS CHAÎNÉS OPTIMISÉS
    # ==========================================
    
    # 🔍 Cash In → W2B
    with st.spinner("Détection Cash In → W2B..."):
        cashin_then_w2b = []
        
        cashin_indexed = cashin_all.set_index('DATE').sort_values('INITATE_DATE')
        w2b_indexed = w2b_all.set_index('DATE').sort_values('INITATE_DATE')
        
        for day in cashin_all['DATE'].unique():
            if day not in cashin_indexed.index or day not in w2b_indexed.index:
                continue
                
            ci_day = cashin_indexed.loc[day] if isinstance(cashin_indexed.loc[day], pd.DataFrame) else cashin_indexed.loc[[day]]
            w2b_day = w2b_indexed.loc[day] if isinstance(w2b_indexed.loc[day], pd.DataFrame) else w2b_indexed.loc[[day]]
            
            # Merge optimisé
            merged = pd.merge(
                ci_day.reset_index(),
                w2b_day.reset_index(),
                left_on='CREDIT_MSISDN',
                right_on='DEBIT_MSISDN',
                suffixes=('_ci', '_w2b')
            )
            
            # Filtrer: W2B après Cash In
            merged = merged[merged['INITATE_DATE_w2b'] > merged['INITATE_DATE_ci']]
            
            if not merged.empty:
                merged['delay_minutes'] = (merged['INITATE_DATE_w2b'] - merged['INITATE_DATE_ci']).dt.total_seconds() / 60
                
                for _, row in merged.iterrows():
                    cashin_then_w2b.append({
                        'date': day,
                        'Distributeur': row['DEBIT_MSISDN_ci'],
                        'client': row['CREDIT_MSISDN_ci'],
                        'cashin_amount': row['ACTUAL_AMOUNT_ci'],
                        'cashin_time': row['INITATE_DATE_ci'],
                        'w2b_amount': row['ACTUAL_AMOUNT_w2b'],
                        'w2b_time': row['INITATE_DATE_w2b'],
                        'Banque': row['CREDIT_MSISDN_w2b'],
                        'delay_minutes': row['delay_minutes'],
                        'scenario': 'Cash In suivi de W2B'
                    })

    if cashin_then_w2b:
        scenario_df_cashin_w2b = pd.DataFrame(cashin_then_w2b)
        
        st.subheader("🚨 Cash In suivi de W2B")
        st.dataframe(scenario_df_cashin_w2b, use_container_width=True)
        
        # Répétitions
        repetition_df = (
            scenario_df_cashin_w2b
            .groupby(['Distributeur', 'client'], as_index=False)
            .agg(
                nb_occurrences=('scenario', 'count'),
                montant_total_cashin=('cashin_amount', 'sum'),
                montant_total_w2b=('w2b_amount', 'sum'),
                premiere_date=('date', 'min'),
                derniere_date=('date', 'max')
            )
            .query('nb_occurrences >= 1')
        )
        
        st.subheader("🚩 Couples SD → RDS répétant le scénario")
        if not repetition_df.empty:
            st.dataframe(repetition_df, use_container_width=True)
        else:
            st.info("Aucun couple répétitif.")
    else:
        st.info("Aucun scénario Cash In → W2B détecté.")


   
    
    # 🔍 DÉTECTION DE CHAÎNES CASH IN → SEND (N fois) → W2B
    with st.spinner("Détection des chaînes Cash In → Send Money (N) → W2B..."):
        
        def find_money_chains(ci_row, send_day, w2b_day, max_depth=10):
            """
            Trouve toutes les chaînes de Send Money partant d'un Cash In jusqu'à un W2B
            
            Args:
                ci_row: La transaction Cash In de départ
                send_day: DataFrame des Send Money du jour
                w2b_day: DataFrame des W2B du jour
                max_depth: Profondeur maximale de recherche (nombre max de Send Money)
            
            Returns:
                List of chains (chaque chain est une liste de transactions)
            """
            chains = []
            
            def explore_chain(current_client, current_time, path, depth):
                """Exploration récursive des chaînes"""
                
                # Limite de profondeur pour éviter les boucles infinies
                if depth > max_depth:
                    return
                
                # Vérifier si ce client fait un W2B après current_time
                w2b_matches = w2b_day[
                    (w2b_day['DEBIT_MSISDN'] == current_client) &
                    (w2b_day['INITATE_DATE'] > current_time)
                ]
                
                # Si on trouve un W2B, on a une chaîne complète
                for _, w2b_row in w2b_matches.iterrows():
                    complete_chain = path + [{
                        'type': 'w2b',
                        'from': w2b_row['DEBIT_MSISDN'],
                        'to': w2b_row['CREDIT_MSISDN'],
                        'amount': w2b_row['ACTUAL_AMOUNT'],
                        'time': w2b_row['INITATE_DATE'],
                        'bank': w2b_row['CREDIT_MSISDN']
                    }]
                    chains.append(complete_chain)
                
                # Chercher les Send Money suivants
                next_sends = send_day[
                    (send_day['DEBIT_MSISDN'] == current_client) &
                    (send_day['INITATE_DATE'] > current_time)
                ]
                
                # Explorer chaque Send Money trouvé
                for _, send_row in next_sends.iterrows():
                    next_client = send_row['CREDIT_MSISDN']
                    next_time = send_row['INITATE_DATE']
                    
                    # Éviter les cycles (client qui s'envoie à lui-même dans la chaîne)
                    clients_in_path = [step['to'] for step in path if step['type'] == 'send']
                    if next_client in clients_in_path:
                        continue
                    
                    new_path = path + [{
                        'type': 'send',
                        'from': send_row['DEBIT_MSISDN'],
                        'to': send_row['CREDIT_MSISDN'],
                        'amount': send_row['ACTUAL_AMOUNT'],
                        'time': send_row['INITATE_DATE']
                    }]
                    
                    # Exploration récursive
                    explore_chain(next_client, next_time, new_path, depth + 1)
            
            # Démarrer l'exploration depuis le client qui reçoit le Cash In
            initial_client = ci_row['CREDIT_MSISDN']
            initial_time = ci_row['INITATE_DATE']
            
            initial_path = [{
                'type': 'cashin',
                'from': ci_row['DEBIT_MSISDN'],
                'to': ci_row['CREDIT_MSISDN'],
                'amount': ci_row['ACTUAL_AMOUNT'],
                'time': ci_row['INITATE_DATE'],
                'distributor': ci_row['DEBIT_MSISDN']
            }]
            
            explore_chain(initial_client, initial_time, initial_path, depth=1)
            
            return chains
        
        # Collecte de toutes les chaînes détectées
        all_chains = []
        
        for day in cashin_all['DATE'].unique():
            ci_day = cashin_all[cashin_all['DATE'] == day].copy()
            send_day = send_all[send_all['DATE'] == day].copy()
            w2b_day = w2b_all[w2b_all['DATE'] == day].copy()
            
            if ci_day.empty or send_day.empty or w2b_day.empty:
                continue
            
            # Pour chaque Cash In, chercher les chaînes
            for _, ci_row in ci_day.iterrows():
                chains = find_money_chains(ci_row, send_day, w2b_day, max_depth=10)
                
                # Traiter chaque chaîne trouvée
                for chain in chains:
                    # Calculer les métriques de la chaîne
                    nb_send = sum(1 for step in chain if step['type'] == 'send')
                    
                    # Extraire les clients impliqués
                    clients = [step['to'] for step in chain if step['type'] in ['cashin', 'send']]
                    
                    # Calculer le délai total
                    first_time = chain[0]['time']
                    last_time = chain[-1]['time']
                    total_delay = (last_time - first_time).total_seconds() / 60
                    
                    # Calculer les montants
                    cashin_amount = chain[0]['amount']
                    w2b_amount = chain[-1]['amount']
                    send_amounts = [step['amount'] for step in chain if step['type'] == 'send']
                    
                    # Score de suspicion
                    risk_score = 0
                    flags = []
                    
                    # Plus il y a de Send Money, plus c'est suspect
                    if nb_send >= 5:
                        risk_score += 100
                        flags.append(f"Chaîne très longue ({nb_send} Send Money)")
                    elif nb_send >= 3:
                        risk_score += 60
                        flags.append(f"Chaîne longue ({nb_send} Send Money)")
                    elif nb_send >= 2:
                        risk_score += 30
                        flags.append(f"Chaîne moyenne ({nb_send} Send Money)")
                    
                    # Délai court = plus suspect
                    if total_delay < 30:
                        risk_score += 50
                        flags.append(f"Très rapide (<30 min)")
                    elif total_delay < 60:
                        risk_score += 30
                        flags.append(f"Rapide (<1h)")
                    
                    # Montants élevés
                    if cashin_amount >= 50000:
                        risk_score += 40
                        flags.append("Montant élevé")
                    
                    # Commission potentielle
                    # D-Money: 2.56% sur Cash In, 0% sur Send Money
                    cashin_commission = cashin_amount * 0.0256
                    
                    # Commission totale = seulement sur le Cash In
                    total_commission = cashin_commission
                    
                    # Commission par intermédiaire (si la chaîne est longue, la commission est diluée)
                    commission_per_intermediary = cashin_commission / (nb_send + 1) if nb_send > 0 else cashin_commission
                    
                    all_chains.append({
                        'date': day,
                        'distributor': chain[0]['from'],
                        'nb_send_money': nb_send,
                        'clients_chain': ' → '.join(clients),
                        'cashin_amount': cashin_amount,
                        'cashin_time': first_time,
                        'w2b_amount': w2b_amount,
                        'w2b_time': last_time,
                        'w2b_bank': chain[-1]['bank'],
                        'total_delay_minutes': round(total_delay, 2),
                        'cashin_commission_djf': round(cashin_commission, 2),
                        'commission_per_person': round(commission_per_intermediary, 2),
                        'risk_score': risk_score,
                        'flags': "; ".join(flags),
                        'full_chain': ' → '.join([f"{step['type'].upper()}({step['amount']:.0f})" for step in chain])
                    })
        
        # Affichage des résultats
        if all_chains:
            chains_df = pd.DataFrame(all_chains)
            
            # Trier par nombre de Send Money (les plus longs d'abord)
            chains_df = chains_df.sort_values(['nb_send_money', 'risk_score'], ascending=[False, False])
            
            st.subheader("🚨 Chaînes Cash In → Send Money (N) → W2B")
            
            # Métriques clés
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Chaînes", len(chains_df))
            with col2:
                st.metric("Chaîne Max", chains_df['nb_send_money'].max(), "Send Money")
            with col3:
                total_commission = chains_df['cashin_commission_djf'].sum()
                st.metric("Commission Cash In Total", f"{total_commission:.2f} DJF")
            with col4:
                avg_length = chains_df['nb_send_money'].mean()
                st.metric("Longueur Moyenne", f"{avg_length:.1f}")
            
            # Tableau détaillé
            st.dataframe(
                chains_df[[
                    'date', 'distributor', 'nb_send_money', 'clients_chain',
                    'cashin_amount', 'cashin_commission_djf', 'commission_per_person',
                    'w2b_amount', 'total_delay_minutes', 'risk_score', 'flags'
                ]],
                use_container_width=True
            )
            
            # Afficher quelques exemples de chaînes complètes
            st.subheader("🔍 Détail des chaînes les plus suspectes")
            top_chains = chains_df.nlargest(5, 'risk_score')
            for idx, row in top_chains.iterrows():
                with st.expander(f"⚠️ Chaîne {idx+1}: {row['nb_send_money']} Send Money - Score: {row['risk_score']}"):
                    st.write(f"**Date:** {row['date']}")
                    st.write(f"**Distributeur:** {row['distributor']}")
                    st.write(f"**Flux complet:** {row['full_chain']}")
                    st.write(f"**Clients:** {row['clients_chain']}")
                    st.write(f"**Délai total:** {row['total_delay_minutes']} minutes")
                    st.write(f"**Commission Cash In:** {row['cashin_commission_djf']:.2f} DJF (2.56%)")
                    st.write(f"**Commission par personne:** {row['commission_per_person']:.2f} DJF")
                    st.write(f"**Flags:** {row['flags']}")
            
            # Analyse des répétitions par distributeur
            st.subheader("📊 Analyse par Distributeur")
            distributor_analysis = (
                chains_df.groupby('distributor')
                .agg(
                    nb_chaines=('nb_send_money', 'count'),
                    longueur_moyenne=('nb_send_money', 'mean'),
                    commission_cashin_totale=('cashin_commission_djf', 'sum'),
                    montant_total_cashin=('cashin_amount', 'sum'),
                    score_moyen=('risk_score', 'mean')
                )
                .reset_index()
                .sort_values('commission_cashin_totale', ascending=False)
            )
            st.dataframe(distributor_analysis, use_container_width=True)
            
            # Analyse des clients récurrents
            st.subheader("👥 Clients Récurrents dans les Chaînes")
            all_clients_in_chains = []
            for clients_str in chains_df['clients_chain']:
                all_clients_in_chains.extend(clients_str.split(' → '))
            
            client_frequency = pd.Series(all_clients_in_chains).value_counts().reset_index()
            client_frequency.columns = ['Client', 'Nb_Apparitions']
            client_frequency = client_frequency[client_frequency['Nb_Apparitions'] > 1]
            
            if not client_frequency.empty:
                st.dataframe(client_frequency, use_container_width=True)
            else:
                st.info("Aucun client n'apparaît dans plusieurs chaînes.")
        else:
            st.info("Aucune chaîne Cash In → Send Money → W2B détectée.")

    # 🔍 B2W → Send Money → W2B
    with st.spinner("Détection B2W → Send → W2B..."):
        b2w_send_w2b = []
        
        for day in b2w_all['DATE'].unique():
            b2w_day = b2w_all[b2w_all['DATE'] == day].copy()
            send_day = send_all[send_all['DATE'] == day].copy()
            w2b_day = w2b_all[w2b_all['DATE'] == day].copy()
            
            if b2w_day.empty or send_day.empty or w2b_day.empty:
                continue
            
            # Renommer les colonnes AVANT le merge
            b2w_day = b2w_day.rename(columns={
                'INITATE_DATE': 'INITATE_DATE_b2w',
                'ACTUAL_AMOUNT': 'ACTUAL_AMOUNT_b2w',
                'DEBIT_MSISDN': 'DEBIT_MSISDN_b2w',
                'CREDIT_MSISDN': 'CREDIT_MSISDN_b2w'
            })
            
            send_day = send_day.rename(columns={
                'INITATE_DATE': 'INITATE_DATE_send',
                'ACTUAL_AMOUNT': 'ACTUAL_AMOUNT_send',
                'DEBIT_MSISDN': 'DEBIT_MSISDN_send',
                'CREDIT_MSISDN': 'CREDIT_MSISDN_send'
            })
            
            w2b_day = w2b_day.rename(columns={
                'INITATE_DATE': 'INITATE_DATE_w2b',
                'ACTUAL_AMOUNT': 'ACTUAL_AMOUNT_w2b',
                'DEBIT_MSISDN': 'DEBIT_MSISDN_w2b',
                'CREDIT_MSISDN': 'CREDIT_MSISDN_w2b'
            })
            
            # Merge B2W → Send
            step1 = pd.merge(
                b2w_day,
                send_day,
                left_on='CREDIT_MSISDN_b2w',
                right_on='DEBIT_MSISDN_send',
                how='inner'
            )
            step1 = step1[step1['INITATE_DATE_send'] > step1['INITATE_DATE_b2w']]
            
            if step1.empty:
                continue
            
            # Merge Send → W2B
            step2 = pd.merge(
                step1,
                w2b_day,
                left_on='CREDIT_MSISDN_send',
                right_on='DEBIT_MSISDN_w2b',
                how='inner'
            )
            step2 = step2[step2['INITATE_DATE_w2b'] > step2['INITATE_DATE_send']]
            
            if not step2.empty:
                step2['delay_1'] = (step2['INITATE_DATE_send'] - step2['INITATE_DATE_b2w']).dt.total_seconds() / 60
                step2['delay_2'] = (step2['INITATE_DATE_w2b'] - step2['INITATE_DATE_send']).dt.total_seconds() / 60
                
                for _, row in step2.iterrows():
                    b2w_send_w2b.append({
                        'date': day,
                        'Source Bank': row['DEBIT_MSISDN_b2w'],
                        'client_A': row['CREDIT_MSISDN_b2w'],
                        'b2w_amount': row['ACTUAL_AMOUNT_b2w'],
                        'b2w_time': row['INITATE_DATE_b2w'],
                        'client_B': row['CREDIT_MSISDN_send'],
                        'send_amount': row['ACTUAL_AMOUNT_send'],
                        'sm_time_1': row['INITATE_DATE_send'],
                        'w2b_amount': row['ACTUAL_AMOUNT_w2b'],
                        'w2b_time': row['INITATE_DATE_w2b'],
                        'Destination Bank': row['CREDIT_MSISDN_w2b'],
                        'delay_B2W_to_Send_min': row['delay_1'],
                        'delay_Send_to_W2B_min': row['delay_2'],
                        'scenario': 'B2W → Send Money → W2B'
                    })

    if b2w_send_w2b:
        scenario_df = pd.DataFrame(b2w_send_w2b)
        
        st.subheader("🚨 B2W → Send Money → W2B")
        st.dataframe(scenario_df, use_container_width=True)
        
        # Répétitions
        repetition_df = (
            scenario_df
            .groupby(['client_A', 'client_B'], as_index=False)
            .agg(
                nb_occurrences=('scenario', 'count'),
                montant_total_b2w=('b2w_amount', 'sum'),
                montant_total_send=('send_amount', 'sum'),
                montant_total_w2b=('w2b_amount', 'sum'),
                premiere_date=('date', 'min'),
                derniere_date=('date', 'max')
            )
            .query('nb_occurrences >= 1')
        )
        
        st.subheader("🚩 Couples Client A → Client B répétant le scénario")
        if not repetition_df.empty:
            st.dataframe(repetition_df, use_container_width=True)
        else:
            st.info("Aucun couple répétitif.")
    else:
        st.info("Aucun scénario B2W → Send → W2B détecté.")

    st.success("✅ Analyse terminée!")
