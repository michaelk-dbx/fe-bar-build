"""Synthetic source-system exports for MKR & Co., a fictional US homeowners insurer.

Simulates what MKR & Co.'s systems drop into the landing zone:
  policy_admin/  policyholders.csv, policies.csv
  vendors/       contractors.csv
  weather/       weather_events.csv   (NOAA-style storm events feed)
  claims_fnol/   claims_<yyyy-mm>.jsonl  (first notice of loss, one file per report month)
  siu/           claim_outcomes.csv  (legacy SIU referral + investigation outcome, closed claims only)
and, outside the landing zone:
  _simulation_truth/fraud_truth.csv  (which claims really were fraud; lets us measure leakage)

Fraud is planted through known property-fraud patterns so the model has real signal:
  inception        loss shortly after the policy starts or coverage is raised
  weather_mismatch hail/wind claim with no storm recorded in that area on that date
  contractor_ring  a small set of contractors who inflate and fabricate claims
  inflated         claimed amount far above what the peril and home size justify
  late_report      reported weeks after the loss
  narrative        vague story, missing receipts, pressure for a quick cash settlement
Legitimate claims share some of these traits (noise), so no single rule separates them.

Usage: uv run python src/data_gen/generate.py --out data/raw
"""

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

TODAY = date(2026, 11, 1)
HISTORY_START = date(2023, 1, 1)
OPEN_WINDOW_DAYS = 30  # claims reported in the last 30 days are still open (no SIU outcome yet)

# Hail/wind belt plus hurricane and freeze exposure; weights ~ share of the book
STATES = {"TX": 0.24, "FL": 0.16, "OK": 0.09, "CO": 0.09, "GA": 0.09,
          "NC": 0.09, "LA": 0.07, "KS": 0.06, "NE": 0.05, "MO": 0.06}
HAIL_STATES = {"TX", "OK", "CO", "KS", "NE", "MO"}
HURRICANE_STATES = {"FL", "LA", "NC", "TX", "GA"}
FREEZE_STATES = {"TX", "OK", "CO", "KS", "NE", "MO", "NC", "GA"}

# Median claim size by peril (USD) and log-normal spread
PERIL_SEVERITY = {"water": (11_000, 0.7), "fire": (42_000, 0.9), "theft": (6_000, 0.6),
                  "hail": (14_000, 0.6), "wind": (9_000, 0.7), "freeze": (10_000, 0.6)}
NON_WEATHER_PERILS = {"water": 0.55, "fire": 0.15, "theft": 0.30}
NON_WEATHER_ANNUAL_RATE = 0.03  # non-weather claims per policy-year

NARRATIVES = {
    "water": ["Supply line under the {room} sink failed; water spread to {n} rooms.",
              "Water heater leak discovered in the {room}; flooring and drywall damaged.",
              "Washing machine hose burst, soaking the {room} and the subfloor below."],
    "fire": ["Kitchen fire started at the stove; smoke damage throughout {n} rooms.",
             "Electrical fire in the {room} wall; fire department responded.",
             "Dryer vent fire spread into the {room}; significant smoke and soot."],
    "theft": ["Home burglarized while insured was traveling; electronics and jewelry taken.",
              "Garage broken into overnight; tools and two bicycles stolen.",
              "Rear door forced open; police report filed, several items missing from the {room}."],
    "hail": ["Hail storm damaged roof shingles and gutters; dents on siding.",
             "Large hail broke two skylights and damaged the roof over the {room}.",
             "Roof and window screens damaged by hail; interior leak in the {room}."],
    "wind": ["High winds lifted roof shingles; fence section blown down.",
             "Tree limb fell on the roof during the storm; leak into the {room}.",
             "Wind tore off part of the roof; tarp installed temporarily."],
    "freeze": ["Pipe froze and burst in the {room} wall during the cold snap.",
               "Frozen pipe in the attic split; ceiling collapsed in the {room}.",
               "Sprinkler line froze and cracked; water damage to the {room}."],
}
ROOMS = ["kitchen", "living room", "basement", "primary bedroom", "bathroom", "garage", "laundry room"]
RED_FLAG_PHRASES = [
    "Insured states all receipts were lost in the damage.",
    "Contractor already began full replacement before inspection.",
    "Insured is requesting a quick cash settlement.",
    "Insured is unsure of the exact date of loss.",
    "Damage first noticed several weeks after it occurred.",
    "Insured recently increased contents coverage.",
]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/raw")
    p.add_argument("--policies", type=int, default=50_000)
    p.add_argument("--fraud-rate", type=float, default=0.08)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def rand_date(rng, start: date, end: date) -> date:
    return start + timedelta(days=int(rng.integers(0, (end - start).days + 1)))


def gen_policyholders_and_policies(rng, fake, n):
    states = rng.choice(list(STATES), size=n, p=list(STATES.values()))
    holders, policies = [], []
    for i, st in enumerate(states):
        ph_id, pol_id = f"PH{i + 1:06d}", f"POL{i + 1:06d}"
        zip_code = fake.zipcode_in_state(st)
        holders.append({
            "policyholder_id": ph_id, "full_name": fake.name(), "email": fake.email(),
            "phone": fake.numerify("###-###-####"), "street_address": fake.street_address(),
            "city": fake.city(), "state": st, "zip_code": zip_code,
            "date_of_birth": fake.date_of_birth(minimum_age=24, maximum_age=85).isoformat(),
        })
        sq_ft = int(np.clip(rng.lognormal(np.log(2000), 0.35), 700, 7000))
        dwelling = int(round(sq_ft * rng.uniform(140, 260), -3))
        effective = rand_date(rng, date(2021, 1, 1), date(2026, 6, 30))
        # ~6% of policies raise coverage mid-term (legitimately, e.g. after a renovation, or not)
        increase = (rand_date(rng, effective + timedelta(days=60), TODAY - timedelta(days=10))
                    if rng.random() < 0.06 and (TODAY - effective).days > 90 else None)
        policies.append({
            "policy_id": pol_id, "policyholder_id": ph_id,
            "product": rng.choice(["HO-3", "HO-5", "DP-3"], p=[0.8, 0.15, 0.05]),
            "state": st, "zip_code": zip_code, "effective_date": effective.isoformat(),
            "coverage_increase_date": increase.isoformat() if increase else None,
            "dwelling_coverage": dwelling, "deductible": int(rng.choice([1000, 2500, 5000], p=[0.45, 0.4, 0.15])),
            "year_built": int(rng.integers(1955, 2025)), "roof_age_years": int(rng.integers(0, 26)),
            "square_feet": sq_ft, "annual_premium": int(dwelling * rng.uniform(0.004, 0.009)),
        })
    return pd.DataFrame(holders), pd.DataFrame(policies)


def gen_contractors(rng, fake):
    rows = []
    for st in STATES:
        for _ in range(int(400 * STATES[st]) + 5):
            rows.append({"contractor_id": f"CTR{len(rows) + 1:04d}",
                         "contractor_name": f"{fake.last_name()} {rng.choice(['Roofing', 'Restoration', 'Builders', 'Home Repair', 'Construction'])}",
                         "state": st, "license_number": fake.bothify(f"{st}-#######"),
                         "years_in_business": int(rng.integers(1, 30))})
    df = pd.DataFrame(rows)
    # The ring: 14 contractors in high-volume states. Known only to the simulation.
    ring = set(df[df.state.isin(["TX", "FL", "GA", "OK"])].sample(14, random_state=7).contractor_id)
    return df, ring


def gen_weather_events(rng, zip3_by_state):
    events = []
    for year in range(HISTORY_START.year, TODAY.year + 1):
        for st, zip3s in zip3_by_state.items():
            plans = []
            if st in HAIL_STATES:
                plans += [("hail", date(year, 4, 1), date(year, 7, 31), 4), ("wind", date(year, 3, 1), date(year, 6, 30), 2)]
            if st in HURRICANE_STATES:
                plans += [("wind", date(year, 8, 1), date(year, 10, 31), 1)]
            if st in FREEZE_STATES:
                plans += [("freeze", date(year, 1, 1), date(year, 2, 28), 1)]
            plans += [("wind", date(year, 1, 1), date(year, 12, 31), 1)]  # severe thunderstorms anywhere
            for peril, start, end, n in plans:
                for _ in range(rng.poisson(n)):
                    begin = rand_date(rng, start, end)
                    if begin >= TODAY:
                        continue
                    k = max(1, int(len(zip3s) * rng.uniform(0.15, 0.45)))
                    events.append({"event_id": f"WX{len(events) + 1:05d}", "state": st, "peril": peril,
                                   "begin_date": begin.isoformat(),
                                   "end_date": (begin + timedelta(days=int(rng.integers(0, 3)))).isoformat(),
                                   "severity": int(rng.choice([1, 2, 3, 4, 5], p=[0.3, 0.3, 0.2, 0.13, 0.07])),
                                   "affected_zip3": ",".join(sorted(rng.choice(zip3s, size=k, replace=False)))})
    return pd.DataFrame(events)


def claim_amount(rng, peril, dwelling):
    median, sigma = PERIL_SEVERITY[peril]
    return float(min(round(rng.lognormal(np.log(median), sigma), 2), dwelling))


def narrative(rng, peril, red_flags=0):
    text = rng.choice(NARRATIVES[peril]).format(room=rng.choice(ROOMS), n=int(rng.integers(2, 5)))
    if red_flags:
        text += " " + " ".join(rng.choice(RED_FLAG_PHRASES, size=red_flags, replace=False))
    return text


def gen_legit_claims(rng, policies, weather, contractors_by_state):
    claims = []
    pol = policies.assign(zip3=policies.zip_code.str[:3], eff=pd.to_datetime(policies.effective_date).dt.date)

    def add(row, peril, loss):
        lag = int(rng.exponential(4))
        if rng.random() < 0.04:  # noise: some honest claims are reported late too
            lag = int(rng.integers(30, 90))
        uses_contractor = peril != "theft" and rng.random() < 0.7
        claims.append({"policy_id": row.policy_id, "peril": peril, "loss_date": loss,
                       "report_date": loss + timedelta(days=lag),
                       "claimed_amount": claim_amount(rng, peril, row.dwelling_coverage),
                       "contractor_id": rng.choice(contractors_by_state[row.state]) if uses_contractor else None,
                       "public_adjuster": bool(rng.random() < 0.08),
                       "description": narrative(rng, peril, red_flags=int(rng.random() < 0.06)),
                       "is_fraud": False, "fraud_patterns": ""})

    # Weather claims: homes in an event's footprint file at a rate that grows with severity
    for ev in weather.itertuples():
        begin = date.fromisoformat(ev.begin_date)
        exposed = pol[(pol.state == ev.state) & pol.zip3.isin(ev.affected_zip3.split(",")) & (pol.eff <= begin)]
        hit = exposed[rng.random(len(exposed)) < 0.004 * ev.severity]
        for row in hit.itertuples():
            add(row, ev.peril, rand_date(rng, begin, date.fromisoformat(ev.end_date)))

    # Non-weather claims: Poisson over each policy's exposure in the history window
    for row in pol.itertuples():
        start = max(row.eff, HISTORY_START)
        years = max((TODAY - start).days, 0) / 365
        for _ in range(rng.poisson(NON_WEATHER_ANNUAL_RATE * years)):
            peril = rng.choice(list(NON_WEATHER_PERILS), p=list(NON_WEATHER_PERILS.values()))
            add(row, peril, rand_date(rng, start, TODAY - timedelta(days=1)))
    return claims


def storm_on(weather_idx, state, zip3, d):
    return any(b <= d <= e and zip3 in z for b, e, z in weather_idx.get(state, []))


def gen_fraud_claims(rng, n, policies, weather, contractors_by_state, ring_by_state):
    weather_idx = {}
    for ev in weather.itertuples():
        weather_idx.setdefault(ev.state, []).append(
            (date.fromisoformat(ev.begin_date) - timedelta(days=3), date.fromisoformat(ev.end_date) + timedelta(days=3),
             set(ev.affected_zip3.split(","))))
    # Fraudsters skew toward repeat offenders: 30% of fraud comes from a small pool of policies
    repeat_pool = policies.sample(int(n * 0.1), random_state=11)
    claims = []
    while len(claims) < n:
        row = (repeat_pool if rng.random() < 0.3 else policies).sample(1, random_state=int(rng.integers(1e9))).iloc[0]
        eff = date.fromisoformat(row.effective_date)
        start = max(eff, HISTORY_START)
        if (TODAY - start).days < 20:
            continue
        patterns = list(rng.choice(["inception", "weather_mismatch", "contractor_ring", "inflated", "late_report", "narrative"],
                                   size=int(rng.choice([1, 2, 3], p=[0.35, 0.45, 0.2])), replace=False,
                                   p=[0.18, 0.18, 0.2, 0.2, 0.12, 0.12]))
        if "contractor_ring" in patterns and row.state not in ring_by_state:
            patterns.remove("contractor_ring")
            if not patterns:
                continue

        peril = rng.choice(["water", "hail", "wind", "theft", "fire"], p=[0.35, 0.2, 0.2, 0.15, 0.1])
        if "weather_mismatch" in patterns:
            peril = rng.choice(["hail", "wind"])
        loss = rand_date(rng, start, TODAY - timedelta(days=1))
        if "inception" in patterns:
            anchor = date.fromisoformat(row.coverage_increase_date) if pd.notna(row.coverage_increase_date) else eff
            loss = anchor + timedelta(days=int(rng.integers(5, 60)))
            if loss >= TODAY or loss < HISTORY_START:
                continue
        if "weather_mismatch" in patterns:
            zip3 = row.zip_code[:3]
            for _ in range(20):
                if not storm_on(weather_idx, row.state, zip3, loss):
                    break
                loss = rand_date(rng, start, TODAY - timedelta(days=1))
            else:
                continue

        amount = claim_amount(rng, "fire" if peril == "fire" else peril, row.dwelling_coverage)
        if "inflated" in patterns or "contractor_ring" in patterns:
            amount = min(round(amount * rng.uniform(1.8, 3.5), 2), float(row.dwelling_coverage))
        lag = int(rng.integers(30, 120)) if "late_report" in patterns else int(rng.exponential(5))
        if "contractor_ring" in patterns:
            contractor = rng.choice(ring_by_state[row.state])
        else:
            contractor = rng.choice(contractors_by_state[row.state]) if peril != "theft" and rng.random() < 0.7 else None
        flags = int(rng.integers(1, 3)) if "narrative" in patterns else int(rng.random() < 0.15)
        claims.append({"policy_id": row.policy_id, "peril": peril, "loss_date": loss,
                       "report_date": loss + timedelta(days=lag), "claimed_amount": amount,
                       "contractor_id": contractor, "public_adjuster": bool(rng.random() < 0.35),
                       "description": narrative(rng, peril, red_flags=flags),
                       "is_fraud": True, "fraud_patterns": "|".join(sorted(patterns))})
    return claims


def legacy_siu_outcomes(rng, claims, policies):
    """Today's process: a few static rules plus adjuster gut feel decide SIU referrals."""
    pol = policies.set_index("policy_id")
    rows = []
    for c in claims.itertuples():
        if c.report_date > TODAY - timedelta(days=OPEN_WINDOW_DAYS):
            continue  # still open
        days_since_eff = (c.loss_date - date.fromisoformat(pol.at[c.policy_id, "effective_date"])).days
        rule_hit = c.claimed_amount > 60_000 or days_since_eff < 30
        p_refer = 0.45 if rule_hit else (0.12 if c.is_fraud else 0.05)  # adjusters catch some fraud on instinct
        referred = rng.random() < p_refer
        confirmed = referred and c.is_fraud and rng.random() < 0.85
        deductible = pol.at[c.policy_id, "deductible"]
        rows.append({"claim_id": c.claim_id, "closed_date": (c.report_date + timedelta(days=int(rng.integers(5, 45)))).isoformat(),
                     "siu_referred": referred,
                     "siu_outcome": ("confirmed_fraud" if confirmed else "cleared") if referred else None,
                     "paid_amount": 0.0 if confirmed else round(max(c.claimed_amount - deductible, 0), 2)})
    return pd.DataFrame(rows)


def main():
    args = parse_args()
    rng = np.random.default_rng(args.seed)
    fake = Faker("en_US")
    Faker.seed(args.seed)
    out = Path(args.out)

    holders, policies = gen_policyholders_and_policies(rng, fake, args.policies)
    contractors, ring = gen_contractors(rng, fake)
    contractors_by_state = {st: g.contractor_id.tolist() for st, g in contractors[~contractors.contractor_id.isin(ring)].groupby("state")}
    ring_by_state = {st: g.contractor_id.tolist() for st, g in contractors[contractors.contractor_id.isin(ring)].groupby("state")}
    zip3_by_state = {st: sorted(g.zip_code.str[:3].unique()) for st, g in policies.groupby("state")}
    weather = gen_weather_events(rng, zip3_by_state)

    legit = gen_legit_claims(rng, policies, weather, contractors_by_state)
    n_fraud = int(len(legit) * args.fraud_rate / (1 - args.fraud_rate))
    fraud = gen_fraud_claims(rng, n_fraud, policies, weather, contractors_by_state, ring_by_state)

    claims = pd.DataFrame(legit + fraud)
    claims = claims[claims.report_date <= TODAY].sort_values(["report_date", "policy_id"]).reset_index(drop=True)
    claims.insert(0, "claim_id", [f"CLM{i + 1:07d}" for i in range(len(claims))])
    outcomes = legacy_siu_outcomes(rng, claims, policies)

    # --- write landing zone ---
    for sub in ["policy_admin", "vendors", "weather", "claims_fnol", "siu", "_simulation_truth"]:
        (out / sub).mkdir(parents=True, exist_ok=True)
    holders.to_csv(out / "policy_admin/policyholders.csv", index=False)
    policies.to_csv(out / "policy_admin/policies.csv", index=False)
    contractors.to_csv(out / "vendors/contractors.csv", index=False)
    weather.to_csv(out / "weather/weather_events.csv", index=False)
    outcomes.to_csv(out / "siu/claim_outcomes.csv", index=False)
    public = claims.drop(columns=["is_fraud", "fraud_patterns"])
    for month, g in public.groupby(public.report_date.map(lambda d: d.strftime("%Y-%m"))):
        with open(out / f"claims_fnol/claims_{month}.jsonl", "w") as f:
            for rec in g.to_dict("records"):
                rec["loss_date"], rec["report_date"] = rec["loss_date"].isoformat(), rec["report_date"].isoformat()
                rec["contractor_id"] = rec["contractor_id"] or None
                f.write(json.dumps(rec) + "\n")
    claims[["claim_id", "is_fraud", "fraud_patterns"]] \
        .assign(contractor_in_ring=claims.contractor_id.isin(ring)) \
        .to_csv(out / "_simulation_truth/fraud_truth.csv", index=False)

    # --- run summary (committed as execution evidence) ---
    closed = outcomes.merge(claims[["claim_id", "is_fraud", "claimed_amount"]], on="claim_id")
    referred = closed[closed.siu_referred]
    leaked = closed[closed.is_fraud & (closed.siu_outcome != "confirmed_fraud")]
    pattern_counts = pd.Series([p for ps in claims.fraud_patterns if ps for p in ps.split("|")]).value_counts()
    print(f"MKR & Co. synthetic data — seed={args.seed}, as of {TODAY}")
    print(f"policyholders/policies : {len(policies):,}")
    print(f"contractors            : {len(contractors):,} ({len(ring)} in fraud ring)")
    print(f"weather events         : {len(weather):,}")
    print(f"claims (FNOL)          : {len(claims):,} across {claims.report_date.map(lambda d: d.strftime('%Y-%m')).nunique()} monthly files")
    print(f"  fraud (ground truth) : {claims.is_fraud.sum():,} ({claims.is_fraud.mean():.1%})")
    print(f"  open (last {OPEN_WINDOW_DAYS} days)  : {len(claims) - len(closed):,}")
    print(f"  claims by peril      : { {str(k): int(v) for k, v in claims.peril.value_counts().items()} }")
    print(f"fraud patterns planted : { {str(k): int(v) for k, v in pattern_counts.items()} }")
    print("--- legacy SIU process (closed claims) ---")
    print(f"referred to SIU        : {len(referred):,} ({len(referred) / len(closed):.1%} of closed)")
    print(f"SIU hit rate           : {(referred.siu_outcome == 'confirmed_fraud').mean():.1%}")
    print(f"fraud caught           : {(closed.is_fraud & (closed.siu_outcome == 'confirmed_fraud')).sum():,} of {closed.is_fraud.sum():,}")
    print(f"fraud leakage (paid)   : ${leaked.paid_amount.sum():,.0f} on {len(leaked):,} claims")
    print(f"total paid             : ${closed.paid_amount.sum():,.0f}")


if __name__ == "__main__":
    main()
