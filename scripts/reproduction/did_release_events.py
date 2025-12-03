"""
DiD analysis of the probability that ReleaseEvents occured after then ChatGPT ban
in Italy, using data from Austria and France as control groups.
This is part of the replication of https://arxiv.org/abs/2304.09339
"""

import pandas as pd
import os
from typing import Any, cast
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf
from pathlib import Path


def csv_to_df(csv_file: str, extra_columns: dict[str, Any]) -> pd.DataFrame:
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    # pyarrow better handles nullable types
    # default is using numpy which converts int columns to float if it contains null
    # because it converts to NaN which is float
    df = pd.read_csv(csv_file, dtype_backend="pyarrow")

    # we need to convert back to numpy so smf can handle it
    df = df.convert_dtypes(dtype_backend="numpy_nullable")

    for col, val in extra_columns.items():
        df[col] = val

    return df


def read_and_merge_dfs() -> pd.DataFrame:
    countries = ["austria", "france", "italy"]
    # countries = ["austria", "italy"]

    merged: pd.DataFrame | None = None
    for country in countries:
        country_df = csv_to_df(
            f"{Path(__file__).resolve().parent.parent}/large_data/events_all_{country}.csv",
            {
                "country": country,
            }
        )

        if merged is None:
            merged = country_df
        else:
            merged = pd.concat([merged, country_df], ignore_index=True)

    if merged is None:
        raise ValueError("DataFrame could not be read")

    return merged


def d_tau_name(tau: int) -> str:
    return f"D_tau_{tau}".replace("-", "m")

def d_tau_index(tau_name: str) -> int:
    return int(tau_name.split("_")[-1].replace("m", "-"))


def prepare_did_variables(df: pd.DataFrame):
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    df["event_date"] = df["event_timestamp"].dt.normalize()

    tau_date_mapping = {
        -3: pd.Timestamp("2023-03-28", tz="UTC"),
        -2: pd.Timestamp("2023-03-29", tz="UTC"),
        -1: pd.Timestamp("2023-03-30", tz="UTC"),
        0: pd.Timestamp("2023-03-31", tz="UTC"), # ban date, will be the left out period
        1: pd.Timestamp("2023-04-03", tz="UTC"), # april 1st and 2nd are left away (weekend)
        2: pd.Timestamp("2023-04-04", tz="UTC"),
        3: pd.Timestamp("2023-04-05", tz="UTC"),
        4: pd.Timestamp("2023-04-06", tz="UTC"),
    }
    dates = list(tau_date_mapping.values())

    output_events = [
        "PushEvent",
        "PullRequestEvent",
        "PullRequestReviewCommentEvent",
        "CommitCommentEvent",
        "CreateEvent",
        "IssuesEvent"
    ]

    user_events = [
        "ReleaseEvent",
        "PushEvent"
    ]

    # df = cast(pd.DataFrame, df[df["event_type"].isin(user_events)])
    df = cast(pd.DataFrame, df[df["event_type"] == "ReleaseEvent"])

    df = cast(pd.DataFrame, df[df["event_date"].isin(dates)])

    # df = cast(pd.DataFrame, df[df["organization_id"].notnull()])

    user_country = df.drop_duplicates("username")[["username", "country"]]
    country_map = dict(zip(user_country["username"], user_country["country"]))
    user_organization_id = df.drop_duplicates("username")[["username", "organization_id"]]
    origanization_id_map = dict(zip(user_organization_id["username"], user_organization_id["organization_id"]))

    # Cartesian product of all users × all global dates
    user_days = (
        pd.MultiIndex.from_product(
            [df["username"].unique(), dates],
            names=["username", "event_date"]
        )
        .to_frame(index=False)
    )

    df = cast(pd.DataFrame, df[df["event_type"] == "ReleaseEvent"])

    df = cast(pd.DataFrame, (
        df.groupby(["username", "event_date"], as_index=False)
        .agg({"release_id": "first", "country": "first", "organization_id": "first"})
    ))

    df = user_days.merge(df, on=["username", "event_date"], how="left")

    df["country"] = df["country"].fillna(df["username"].map(country_map)) # type: ignore
    # df["organization_id"] = df["organization_id"].fillna(df["username"].map(origanization_id_map)) # type: ignore

    df["Y"] = df["release_id"].notna().astype(int)

    for tau, date in tau_date_mapping.items():
        df[d_tau_name(tau)] = ((df["event_date"] == date) & (df["country"] == "italy")).astype(int)

    # keep_cols = ["username", "event_date", "Y"] + [d_tau_name(tau) for tau in range(-3, 5)]
    # df = df.loc[:, keep_cols]

    return df


def plot_did(df: pd.DataFrame):
    plot_taus = [-3, -2, -1, 1, 2, 3, 4] # 0 is the ban date, which is left away
    betas = " + ".join([d_tau_name(tau) for tau in plot_taus])
    formula = f"Y ~ {betas} + C(username) + C(event_date)"  # α_i and λ_t are fixed effects

    print(f"formula: {formula}")

    # Estimate the model
    model = smf.ols(formula, data=df).fit(
        cov_type="cluster",
        cov_kwds={"groups": df["username"]}  # cluster by user ID
    )

    print("Model calculation complete.")

    results = model.params
    ses = model.bse

    beta_df = pd.DataFrame({
        "tau": [d_tau_index(c) for c in results.index if c.startswith("D_tau_")],
        "beta": [results[c] for c in results.index if c.startswith("D_tau_")],
        "se": [ses[c] for c in ses.index if c.startswith("D_tau_")]
    })

    beta_df["ci_low"] = beta_df["beta"] - 1.96 * beta_df["se"]
    beta_df["ci_high"] = beta_df["beta"] + 1.96 * beta_df["se"]

    fig, ax = plt.subplots(figsize=(6, 4))

    # Main DID dots and lines
    ax.errorbar(
        beta_df["tau"],
        beta_df["beta"],
        yerr=1.96 * beta_df["se"],
        fmt="o-",
        color="black",
        ecolor="black",
        elinewidth=1,
        capsize=3,
    )

    # Add horizontal line at 0
    ax.axhline(0, color="gray", linewidth=1)

    # Add vertical dashed line at the policy date (τ = 0)
    ax.axvline(0, color="red", linestyle="--", linewidth=1)

    ax.set_xlabel("Days to ChatGPT Ban")
    # ax.set_ylabel("Y")
    ax.set_title("The Effect of the ChatGPT Ban on GitHub Releases")

    # Make x-axis integer ticks
    ax.set_xticks(sorted(beta_df["tau"].unique()))

    plt.tight_layout()
    plt.savefig(f"{Path(__file__).resolve().parent}/plots/did_release_events.png")


def main():
    df = read_and_merge_dfs()
    df = prepare_did_variables(df)

    # smf is ot able to handle timezones, thus remove it
    df["event_date"] = df["event_date"].dt.tz_localize(None)

    # df = df = df[df["Y"] == 1]
    
    # filter to only include users with organizatino
    # df = cast(pd.DataFrame, df[df["organization_id"].notnull()])

    print(f"Amount of users: {df["username"].nunique()}")

    df.to_csv(f"{Path(__file__).resolve().parent}/data/did_release_events.csv", index=False)

    plot_did(df)


if __name__ == "__main__":
    main()
