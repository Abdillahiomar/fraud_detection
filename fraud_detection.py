import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np
from datetime import datetime
import mysql.connector
from mysql.connector import Error


DB_CONFIG = {
    "host":     "192.168.100.50",
    "user":     "streamlit",
    "password": "Password!1",
    "database": "fraud_detections",
    "connect_timeout": 10,
}

def get_connection():
    """Crée une nouvelle connexion à chaque appel."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        st.error(f"Erreur connexion DB : {e}")
        return None


@st.cache_data(show_spinner=False)
def load_from_db(date_debut, date_fin):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        query = """
            SELECT
                  ORDERID, TRANS_STATUS, INITATE_DATE,
                  REASON_TYPE, REASON_NAME,
                  DEBIT_MSISDN, CREDIT_MSISDN,
                  ACTUAL_AMOUNT, FEE, COMMISSION, TAX, AVAILABLE_BALANCE
            FROM transactions
            WHERE INITATE_DATE BETWEEN %s AND %s
        """
        chunks = pd.read_sql(query, conn, params=(date_debut, date_fin), chunksize=100_000)
        df = pd.concat(chunks, ignore_index=True)

        df['INITATE_DATE']  = pd.to_datetime(df['INITATE_DATE'],  errors='coerce')
        df['ACTUAL_AMOUNT'] = pd.to_numeric(df['ACTUAL_AMOUNT'],  errors='coerce').fillna(0)
        return df

    except Error as e:
        st.error(f"Erreur lors de la requête : {e}")
        return pd.DataFrame()

    finally:
        if conn.is_connected():
            conn.close()   # ← toujours fermer après usage


st.set_page_config(page_title="Détection des Scénarios de fraude", layout="wide")
st.title("🕵️ Détection des Scénarios de fraude")


st.caption("Importez votre fichier CSV extrait depuis la base de données pour analyser les transactions suspectes.")


#uploaded_file = st.file_uploader("📤 Charger le fichier CSV des transactions", type=["csv"])

with st.sidebar:
    st.header("📂 Fichier CSV")
    
    st.header("📅 Période d'analyse")

    date_debut = st.date_input("Date début")
    date_fin   = st.date_input("Date fin")
    
    date_debut = str(date_debut) + " 00:00:00"
    date_fin   = str(date_fin)   + " 23:59:59"

    charger = st.button("Charger les données")

if not charger:
    # ✅ Lecture unique du fichier avec optimisations
    with st.spinner("Chargement et prétraitement des données..."):
        df = load_from_db(date_debut, date_fin)
        
        
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


   
    
    # ══════════════════════════════════════════════════════════════════
    # DÉTECTION : Cycling de commission
    # Schéma : Agent fait CASH IN → Send Money (N) → W2B
    #          puis recommence avec le MÊME argent le MÊME jour
    #          pour accumuler des commissions artificielles
    #
    # Critères d'alerte :
    #   - Même agent, même jour
    #   - Même montant (± tolérance)
    #   - Nombre de cycles ≥ seuil paramétrable
    # ══════════════════════════════════════════════════════════════════

    with st.spinner("Détection des cycles de commission (Cash In → Send → W2B)..."):

        # ── Paramètres utilisateur ─────────────────────────────────────
        st.markdown("#### ⚙️ Paramètres de détection")
        col_p1, col_p2, col_p3 = st.columns(3)
        with col_p1:
            min_cycles = st.number_input(
                "Nombre minimum de cycles pour alerte",
                min_value=2, max_value=20, value=2, step=1,
                help="Un cycle = 1 Cash In + chaîne Send Money + 1 W2B, même montant, même agent"
            )
        with col_p2:
            amount_tolerance = st.slider(
                "Tolérance montant (%)",
                min_value=0, max_value=10, value=1, step=1,
                help="Écart autorisé entre les montants pour considérer que c'est le même argent"
            ) / 100
        with col_p3:
            max_depth = st.number_input(
                "Profondeur max Send Money",
                min_value=1, max_value=20, value=10, step=1,
                help="Nombre maximum de Send Money dans une chaîne"
            )

        st.divider()

        # ── Fonction utilitaire ────────────────────────────────────────
        def same_amount(a, b, tol=amount_tolerance):
            if a == 0:
                return False
            return abs(a - b) / a <= tol

        # ── Recherche d'une chaîne SEND → W2B pour un montant donné ───
        def find_chain(start_client, start_time, target_amount,
                       send_day, w2b_day, max_depth):
            """
            Explore récursivement les Send Money du même montant
            jusqu'à trouver un W2B.
            Retourne la première chaîne complète trouvée, ou None.
            """
            def explore(client, t, path, depth):
                if depth > max_depth:
                    return None

                # Y a-t-il un W2B depuis ce client après t ?
                w2b_hits = w2b_day[
                    (w2b_day['DEBIT_MSISDN'] == client) &
                    (w2b_day['INITATE_DATE'] > t)
                ]
                for _, w in w2b_hits.iterrows():
                    return path + [{
                        'type':   'W2B',
                        'from':   w['DEBIT_MSISDN'],
                        'to':     w['CREDIT_MSISDN'],
                        'amount': w['ACTUAL_AMOUNT'],
                        'time':   w['INITATE_DATE'],
                    }]

                # Send Money avec le même montant
                sends = send_day[
                    (send_day['DEBIT_MSISDN'] == client) &
                    (send_day['INITATE_DATE'] > t) &
                    (send_day['ACTUAL_AMOUNT'].apply(
                        lambda x: same_amount(x, target_amount)
                    ))
                ]
                visited = {s['from'] for s in path} | {s['to'] for s in path}
                for _, s in sends.iterrows():
                    if s['CREDIT_MSISDN'] in visited:
                        continue
                    result = explore(
                        s['CREDIT_MSISDN'],
                        s['INITATE_DATE'],
                        path + [{
                            'type':   'SEND',
                            'from':   s['DEBIT_MSISDN'],
                            'to':     s['CREDIT_MSISDN'],
                            'amount': s['ACTUAL_AMOUNT'],
                            'time':   s['INITATE_DATE'],
                        }],
                        depth + 1
                    )
                    if result:
                        return result
                return None

            return explore(start_client, start_time, [], 1)

        # ── Boucle principale : jour par jour ─────────────────────────
        # Structure résultat : une ligne par AGENT × JOUR × MONTANT
        # contenant tous les cycles détectés

        agent_day_cycles = {}   # clé : (agent, day, amount_bucket)

        for day in cashin_all['DATE'].unique():
            ci_day   = cashin_all[cashin_all['DATE'] == day].copy()
            send_day = send_all[send_all['DATE'] == day].copy()
            w2b_day  = w2b_all[w2b_all['DATE'] == day].copy()

            if ci_day.empty or send_day.empty or w2b_day.empty:
                continue

            for _, ci in ci_day.iterrows():
                agent      = ci['DEBIT_MSISDN']
                ci_amount  = ci['ACTUAL_AMOUNT']
                ci_client  = ci['CREDIT_MSISDN']
                ci_time    = ci['INITATE_DATE']

                # Chercher la chaîne Send → W2B pour ce Cash In
                chain = find_chain(
                    ci_client, ci_time, ci_amount,
                    send_day, w2b_day, max_depth
                )
                if chain is None:
                    continue   # pas de W2B en bout de chaîne → pas un cycle

                # Calculer métriques du cycle
                nb_send   = sum(1 for s in chain if s['type'] == 'SEND')
                w2b_time  = chain[-1]['time']
                w2b_amt   = chain[-1]['amount']
                delay_min = (w2b_time - ci_time).total_seconds() / 60

                full_chain = ' → '.join(
                    f"{s['type']}({s['amount']:,.0f})" for s in
                    [{'type':'CASHIN','amount':ci_amount}] + chain
                )
                clients_path = ' → '.join(
                    [ci_client] +
                    [s['to'] for s in chain if s['type'] == 'SEND']
                )

                # Regrouper par (agent, jour, montant) pour compter les cycles
                # On arrondit le montant à 100 FDJ pour tolérance naturelle
                amount_bucket = round(ci_amount / 100) * 100
                key = (agent, day, amount_bucket)

                if key not in agent_day_cycles:
                    agent_day_cycles[key] = {
                        'agent':          agent,
                        'date':           day,
                        'montant_ref':    ci_amount,
                        'cycles':         [],
                    }

                agent_day_cycles[key]['cycles'].append({
                    'ci_time':      ci_time,
                    'w2b_time':     w2b_time,
                    'ci_amount':    ci_amount,
                    'w2b_amount':   w2b_amt,
                    'nb_send':      nb_send,
                    'delay_min':    round(delay_min, 1),
                    'clients_path': clients_path,
                    'full_chain':   full_chain,
                    'montant_flag': 'MÊME MONTANT' if same_amount(ci_amount, w2b_amt) else 'MONTANT DIFFÉRENT',
                    'delta_fdj':    round(ci_amount - w2b_amt, 2),
                })

        # ── Construire le DataFrame des alertes ────────────────────────
        rows = []
        for key, data in agent_day_cycles.items():
            nb_cycles = len(data['cycles'])
            if nb_cycles < min_cycles:
                continue   # sous le seuil → pas d'alerte

            cycles      = data['cycles']
            total_ci    = sum(c['ci_amount']  for c in cycles)
            total_w2b   = sum(c['w2b_amount'] for c in cycles)
            commission  = total_ci * 0.0256   # 2.56% uniquement sur Cash In

            # Durée totale d'activité ce jour-là
            first_ci    = min(c['ci_time']  for c in cycles)
            last_w2b    = max(c['w2b_time'] for c in cycles)
            span_min    = (last_w2b - first_ci).total_seconds() / 60

            # Délai moyen par cycle
            avg_delay   = sum(c['delay_min'] for c in cycles) / nb_cycles

            # Nb Send Money moyen par cycle
            avg_send    = sum(c['nb_send']   for c in cycles) / nb_cycles

            # Flag montant : tous les cycles ont le même montant ?
            flags_montant = set(c['montant_flag'] for c in cycles)
            if flags_montant == {'MÊME MONTANT'}:
                flag_global = '✅ MÊME MONTANT (tous cycles)'
            elif flags_montant == {'MONTANT DIFFÉRENT'}:
                flag_global = '⚠️ MONTANT DIFFÉRENT (tous cycles)'
            else:
                flag_global = '🔀 MIXTE'

            # Score de risque
            risk  = 0
            flags = []

            if nb_cycles >= 5:
                risk += 100; flags.append(f"{nb_cycles} cycles détectés")
            elif nb_cycles >= 3:
                risk += 60;  flags.append(f"{nb_cycles} cycles détectés")
            else:
                risk += 30;  flags.append(f"{nb_cycles} cycles détectés")

            if avg_send >= 3:
                risk += 40; flags.append(f"Chaîne longue (moy. {avg_send:.1f} Send/cycle)")

            if avg_delay < 30:
                risk += 50; flags.append("Cycles très rapides (< 30 min en moyenne)")
            elif avg_delay < 60:
                risk += 30; flags.append("Cycles rapides (< 1h en moyenne)")

            if total_ci >= 200_000:
                risk += 40; flags.append(f"Volume élevé ({total_ci:,.0f} FDJ)")

            if '✅ MÊME MONTANT' in flag_global:
                risk += 30; flags.append("Montant identique à chaque cycle (argent recyclé)")

            rows.append({
                'date':              data['date'],
                'agent':             data['agent'],
                'nb_cycles':         nb_cycles,
                'montant_par_cycle': data['montant_ref'],
                'total_cashin_fdj':  round(total_ci, 2),
                'total_w2b_fdj':     round(total_w2b, 2),
                'commission_gagnee': round(commission, 2),
                'commission_si_1':   round(data['montant_ref'] * 0.0256, 2),
                'surplus_commission':round(commission - data['montant_ref'] * 0.0256, 2),
                'flag_montant':      flag_global,
                'avg_send_per_cycle':round(avg_send, 1),
                'avg_delay_min':     round(avg_delay, 1),
                'span_minutes':      round(span_min, 1),
                'risk_score':        risk,
                'flags':             '; '.join(flags),
                '_cycles_detail':    cycles,   # pour le détail (non affiché dans tableau)
            })

        # ── Affichage ──────────────────────────────────────────────────
        if not rows:
            st.info(f"Aucun agent avec ≥ {min_cycles} cycles détectés aujourd'hui.")
        else:
            alerts_df = pd.DataFrame(rows).sort_values('risk_score', ascending=False)

            st.subheader("🚨 Agents en cycling de commission")
            st.caption(
                f"Critères : même agent · même jour · même montant (±{int(amount_tolerance*100)}%) · "
                f"≥ {min_cycles} cycles Cash In → Send Money → W2B"
            )

            # ── KPIs ──
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Agents alertés",       len(alerts_df))
            c2.metric("Total cycles",          int(alerts_df['nb_cycles'].sum()))
            c3.metric("Commission totale",     f"{alerts_df['commission_gagnee'].sum():,.0f} FDJ")
            c4.metric("Surplus commission",    f"{alerts_df['surplus_commission'].sum():,.0f} FDJ",
                      help="Commission perçue - commission légitime sur 1 seul Cash In")
            c5.metric("Volume total Cash In",  f"{alerts_df['total_cashin_fdj'].sum():,.0f} FDJ")

            # ── Tableau principal ──
            st.dataframe(
                alerts_df.drop(columns=['_cycles_detail']),
                use_container_width=True,
                hide_index=True,
                column_config={
                    'nb_cycles': st.column_config.NumberColumn(
                        '🔄 Nb cycles',
                        help="Nombre de fois que l'agent a répété le schéma ce jour-là"
                    ),
                    'surplus_commission': st.column_config.NumberColumn(
                        '💰 Surplus commission (FDJ)',
                        help="Commission frauduleuse = commission totale - commission d'1 seul Cash In légitime",
                        format="%,.0f"
                    ),
                    'flag_montant': st.column_config.TextColumn(
                        '🏷️ Flag montant',
                    ),
                    'risk_score': st.column_config.ProgressColumn(
                        'Score risque', min_value=0, max_value=270, format="%d"
                    ),
                    'commission_gagnee': st.column_config.NumberColumn(format="%,.0f"),
                    'total_cashin_fdj':  st.column_config.NumberColumn(format="%,.0f"),
                    'total_w2b_fdj':     st.column_config.NumberColumn(format="%,.0f"),
                    'montant_par_cycle': st.column_config.NumberColumn(format="%,.0f"),
                }
            )

            # ── Détail cycle par cycle ──
            st.subheader("🔍 Détail par agent")
            for _, row in alerts_df.iterrows():
                with st.expander(
                    f"🔄 {row['agent']}  |  {row['nb_cycles']} cycles  "
                    f"|  {row['montant_par_cycle']:,.0f} FDJ/cycle  "
                    f"|  Surplus : {row['surplus_commission']:,.0f} FDJ  "
                    f"|  Score : {row['risk_score']}"
                ):
                    st.markdown(f"""
                    | Indicateur | Valeur |
                    |---|---|
                    | Date | {row['date']} |
                    | Montant par cycle | {row['montant_par_cycle']:,.0f} FDJ |
                    | Nombre de cycles | {row['nb_cycles']} |
                    | Commission totale perçue | {row['commission_gagnee']:,.0f} FDJ |
                    | Commission légitime (1 cycle) | {row['commission_si_1']:,.0f} FDJ |
                    | **Surplus frauduleux** | **{row['surplus_commission']:,.0f} FDJ** |
                    | Send Money moyen / cycle | {row['avg_send_per_cycle']} |
                    | Délai moyen / cycle | {row['avg_delay_min']} min |
                    | Durée totale activité | {row['span_minutes']} min |
                    | Flag montant | {row['flag_montant']} |
                    | Flags | {row['flags']} |
                    """)

                    st.markdown("**Détail des cycles :**")
                    for i, cycle in enumerate(row['_cycles_detail'], 1):
                        flag_icon = "✅" if cycle['montant_flag'] == 'MÊME MONTANT' else "⚠️"
                        st.markdown(
                            f"**Cycle {i}** {flag_icon}  `{cycle['full_chain']}`  "
                            f"— délai : {cycle['delay_min']} min  "
                            f"— {cycle['nb_send']} Send Money  "
                            f"— delta : {cycle['delta_fdj']:,.0f} FDJ"
                        )

            # ── Export Excel ──
            from io import BytesIO
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                alerts_df.drop(columns=['_cycles_detail']).to_excel(
                    writer, sheet_name='Alertes_cycling', index=False
                )
                # Feuille détail : une ligne par cycle
                detail_rows = []
                for _, row in alerts_df.iterrows():
                    for i, c in enumerate(row['_cycles_detail'], 1):
                        detail_rows.append({
                            'agent':         row['agent'],
                            'date':          row['date'],
                            'cycle_num':     i,
                            'ci_time':       c['ci_time'],
                            'w2b_time':      c['w2b_time'],
                            'ci_amount':     c['ci_amount'],
                            'w2b_amount':    c['w2b_amount'],
                            'nb_send':       c['nb_send'],
                            'delay_min':     c['delay_min'],
                            'montant_flag':  c['montant_flag'],
                            'delta_fdj':     c['delta_fdj'],
                            'clients_path':  c['clients_path'],
                            'full_chain':    c['full_chain'],
                        })
                pd.DataFrame(detail_rows).to_excel(
                    writer, sheet_name='Détail_cycles', index=False
                )

            st.download_button(
                "⬇️ Exporter les alertes (Excel)",
                data=buf.getvalue(),
                file_name=f"cycling_commission_{pd.Timestamp.today().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
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
