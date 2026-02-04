import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def parse_2024(filepath):
    """Parse 2024 file - threshold >= 1000 EUR, columns: Politieke partij, Neveninstelling, Totaalbedrag, Naam gever, Adres gever, Naam UBO, Woonplaats UBO, Bedragen > 1000, Datum, Toelichting"""
    df = pd.read_excel(filepath, engine='odf', header=None)

    # Find header row for detail data
    header_row = None
    for i in range(len(df)):
        vals = [str(v) for v in df.iloc[i] if str(v) != 'nan']
        if 'Naam gever' in vals or 'Naam donateur' in vals:
            header_row = i
            break

    if header_row is None:
        return pd.DataFrame()

    # Extract detail data
    detail = df.iloc[header_row+1:].copy()
    detail.columns = ['partij', 'neveninstelling', 'totaalbedrag', 'naam_gever', 'adres_gever',
                       'naam_ubo', 'woonplaats_ubo', 'bedrag', 'datum', 'toelichting']

    # Forward-fill party name
    detail['partij'] = detail['partij'].ffill()

    # The totaalbedrag is only on the first row per donor-party combo
    # naam_gever identifies a new donor entry
    # We need to forward-fill naam_gever within each party group

    # Build records: each row with a naam_gever starts a new donor
    records = []
    current_party = None
    current_donor = None
    current_city = None
    current_total = None
    current_neveninstelling = None

    for _, row in detail.iterrows():
        party = row['partij'] if pd.notna(row['partij']) else current_party
        if pd.isna(party):
            continue
        current_party = party

        # Check if this row has a donor name
        if pd.notna(row['naam_gever']):
            current_donor = str(row['naam_gever'])
            current_city = str(row['adres_gever']) if pd.notna(row['adres_gever']) else ''

        if pd.notna(row['neveninstelling']) and isinstance(row['neveninstelling'], str):
            current_neveninstelling = row['neveninstelling']
        elif pd.notna(row['neveninstelling']):
            current_neveninstelling = None

        if pd.notna(row['totaalbedrag']):
            try:
                current_total = float(row['totaalbedrag'])
            except:
                current_total = None

        # If there's a specific donation amount
        if pd.notna(row['bedrag']):
            try:
                amount = float(row['bedrag'])
            except:
                continue

            datum = row['datum'] if pd.notna(row['datum']) else ''
            ubo_name = str(row['naam_ubo']) if pd.notna(row['naam_ubo']) else ''
            ubo_city = str(row['woonplaats_ubo']) if pd.notna(row['woonplaats_ubo']) else ''
            toelichting = str(row['toelichting']) if pd.notna(row['toelichting']) else ''

            records.append({
                'year': 2024,
                'partij': current_party,
                'neveninstelling': current_neveninstelling if current_neveninstelling else '',
                'naam_donateur': current_donor,
                'adres_gever': current_city,
                'ubo': ubo_name,
                'ubo_woonplaats': ubo_city,
                'bedrag': amount,
                'totaal_donateur': current_total,
                'datum': str(datum).split(' ')[0] if datum else '',
                'toelichting': toelichting
            })

    return pd.DataFrame(records)


def parse_substantial(filepath, year):
    """Parse 2023/2025/2026 files - threshold >= 10000 EUR"""
    df = pd.read_excel(filepath, engine='odf', header=None)

    # Find header row for detail data
    header_row = None
    for i in range(len(df)):
        vals = [str(v) for v in df.iloc[i] if str(v) != 'nan']
        if 'Naam donateur' in vals or 'Naam gever' in vals:
            header_row = i
            break

    if header_row is None:
        return pd.DataFrame()

    headers = [str(v) if pd.notna(v) else '' for v in df.iloc[header_row]]
    ncols = len(headers)

    detail = df.iloc[header_row+1:].copy()

    records = []
    current_party = None
    current_donor = None
    current_city = None
    current_total = None
    current_neveninstelling = None
    current_ubo = None

    for _, row in detail.iterrows():
        vals = row.values

        # Column mapping depends on year
        party_val = vals[0]
        neveninstelling_val = vals[1]
        total_val = vals[2]

        if year == 2023:
            # Columns: Politieke partij, Neveninstelling, Totaal 2023, Naam donateur, Adres gever, Bedragen > 10000, Datum
            donor_val = vals[3]
            adres_val = vals[4]
            ubo_val = None
            bedrag_val = vals[5]
            datum_val = vals[6] if ncols > 6 else None
        else:
            # 2025/2026: Politieke partij, Neveninstelling, Totaal, Naam donateur, Adres gever, UBO, Bedragen > 10000, Datum, Toelichting
            donor_val = vals[3]
            adres_val = vals[4]
            ubo_val = vals[5]
            bedrag_val = vals[6]
            datum_val = vals[7] if ncols > 7 else None

        if pd.notna(party_val) and str(party_val).strip():
            current_party = str(party_val).strip()

        if pd.notna(neveninstelling_val) and isinstance(neveninstelling_val, str) and neveninstelling_val.strip():
            current_neveninstelling = neveninstelling_val.strip()
        elif pd.notna(neveninstelling_val) and not isinstance(neveninstelling_val, str):
            # It's a number (totaalbedrag shifted)
            current_neveninstelling = ''

        if pd.notna(total_val):
            try:
                current_total = float(total_val)
            except:
                pass

        if pd.notna(donor_val) and str(donor_val).strip():
            current_donor = str(donor_val).strip()

        if pd.notna(adres_val) and str(adres_val).strip():
            current_city = str(adres_val).strip()

        if ubo_val is not None and pd.notna(ubo_val) and str(ubo_val).strip():
            current_ubo = str(ubo_val).strip()
        else:
            if pd.notna(donor_val):
                current_ubo = ''

        if current_party is None or current_donor is None:
            continue

        if pd.notna(bedrag_val):
            try:
                amount = float(bedrag_val)
            except:
                continue

            datum = str(datum_val).split(' ')[0] if pd.notna(datum_val) else ''

            records.append({
                'year': year,
                'partij': current_party,
                'neveninstelling': current_neveninstelling if current_neveninstelling else '',
                'naam_donateur': current_donor,
                'adres_gever': current_city if current_city else '',
                'ubo': current_ubo if current_ubo else '',
                'ubo_woonplaats': '',
                'bedrag': amount,
                'totaal_donateur': current_total,
                'datum': datum,
                'toelichting': ''
            })

    return pd.DataFrame(records)


# Parse all files
print("Parsing donation data...")
df_2024 = parse_2024('data/giften_2024.ods')
df_2023 = parse_substantial('data/giften_2023.ods', 2023)
df_2025 = parse_substantial('data/giften_2025.ods', 2025)
df_2026 = parse_substantial('data/giften_2026.ods', 2026)

print(f"2023: {len(df_2023)} individual donation records")
print(f"2024: {len(df_2024)} individual donation records")
print(f"2025: {len(df_2025)} individual donation records")
print(f"2026: {len(df_2026)} individual donation records")

# Combine all
all_donations = pd.concat([df_2023, df_2024, df_2025, df_2026], ignore_index=True)
print(f"\nTotal combined: {len(all_donations)} individual donation records")

# Create donor-level summary per year (aggregate individual payments)
donor_year = all_donations.groupby(['year', 'partij', 'naam_donateur']).agg(
    totaal_bedrag=('bedrag', 'sum'),
    aantal_donaties=('bedrag', 'count'),
    adres=('adres_gever', 'first'),
    ubo=('ubo', 'first'),
    neveninstelling=('neveninstelling', 'first')
).reset_index()

print(f"\nUnique donor-party-year combinations: {len(donor_year)}")

# Save all individual donations
all_donations.to_csv('data/all_individual_donations.csv', index=False, encoding='utf-8-sig')

# Save donor-year summary
donor_year.to_csv('data/donor_year_summary.csv', index=False, encoding='utf-8-sig')

print("\n" + "="*80)
print("ANALYSIS: DONATIONS PER PARTY PER YEAR")
print("="*80)

party_year = donor_year.groupby(['year', 'partij']).agg(
    totaal=('totaal_bedrag', 'sum'),
    aantal_donateurs=('naam_donateur', 'nunique')
).reset_index()

# Pivot for nice display
pivot_amount = party_year.pivot_table(index='partij', columns='year', values='totaal', fill_value=0)
pivot_amount['Totaal'] = pivot_amount.sum(axis=1)
pivot_amount = pivot_amount.sort_values('Totaal', ascending=False)

print("\nTotal donation amounts per party per year (EUR):")
print(pivot_amount.to_string(float_format=lambda x: f'{x:,.0f}'))

pivot_donors = party_year.pivot_table(index='partij', columns='year', values='aantal_donateurs', fill_value=0)
pivot_donors['Totaal'] = pivot_donors.sum(axis=1)
pivot_donors = pivot_donors.sort_values('Totaal', ascending=False)

print("\n\nNumber of donors per party per year:")
print(pivot_donors.to_string())

print("\n" + "="*80)
print("TOP 30 LARGEST INDIVIDUAL DONORS (ACROSS ALL YEARS)")
print("="*80)

# Aggregate across years for each donor
top_donors = all_donations.groupby(['naam_donateur']).agg(
    totaal_bedrag=('bedrag', 'sum'),
    partijen=('partij', lambda x: ', '.join(sorted(set(x)))),
    jaren=('year', lambda x: ', '.join(sorted(set(str(y) for y in x)))),
    adres=('adres_gever', 'first'),
    ubo=('ubo', 'first')
).reset_index().sort_values('totaal_bedrag', ascending=False).head(30)

for i, row in top_donors.iterrows():
    ubo_info = f" (UBO: {row['ubo']})" if row['ubo'] else ""
    print(f"\n  {row['naam_donateur']}: EUR {row['totaal_bedrag']:,.0f}")
    print(f"    Partij(en): {row['partijen']}")
    print(f"    Jaren: {row['jaren']}")
    print(f"    Adres: {row['adres']}{ubo_info}")

print("\n" + "="*80)
print("RECURRING DONORS (DONATED IN MULTIPLE YEARS)")
print("="*80)

# Find donors who appear in multiple years
donor_years = all_donations.groupby('naam_donateur').agg(
    years=('year', lambda x: sorted(set(x))),
    n_years=('year', lambda x: len(set(x))),
    totaal=('bedrag', 'sum'),
    partijen=('partij', lambda x: ', '.join(sorted(set(x)))),
    adres=('adres_gever', 'first')
).reset_index()

recurring = donor_years[donor_years['n_years'] > 1].sort_values('totaal', ascending=False)

print(f"\n{len(recurring)} donors donated in multiple years:\n")
for _, row in recurring.iterrows():
    years_str = ', '.join(str(y) for y in row['years'])
    print(f"  {row['naam_donateur']}: EUR {row['totaal']:,.0f} over {row['n_years']} years ({years_str})")
    print(f"    Partij(en): {row['partijen']}, Adres: {row['adres']}")

print("\n" + "="*80)
print("DONORS WHO GAVE TO MULTIPLE PARTIES")
print("="*80)

multi_party = all_donations.groupby('naam_donateur').agg(
    partijen=('partij', lambda x: sorted(set(x))),
    n_partijen=('partij', lambda x: len(set(x))),
    totaal=('bedrag', 'sum'),
    jaren=('year', lambda x: sorted(set(x)))
).reset_index()

multi = multi_party[multi_party['n_partijen'] > 1].sort_values('totaal', ascending=False)

if len(multi) > 0:
    print(f"\n{len(multi)} donors gave to multiple parties:\n")
    for _, row in multi.iterrows():
        parties_str = ', '.join(row['partijen'])
        years_str = ', '.join(str(y) for y in row['jaren'])
        print(f"  {row['naam_donateur']}: EUR {row['totaal']:,.0f}")
        print(f"    Partijen: {parties_str}")
        print(f"    Jaren: {years_str}")
else:
    print("\nNo donors found who gave to multiple parties.")

# Save recurring donors analysis
recurring.to_csv('data/recurring_donors.csv', index=False, encoding='utf-8-sig')
print("\n\nFiles saved:")
print("  - data/all_individual_donations.csv")
print("  - data/donor_year_summary.csv")
print("  - data/recurring_donors.csv")
