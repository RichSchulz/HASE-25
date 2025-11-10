import pandas as pd
import os
from typing import Any
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf


def csv_to_df(csv_file: str, extra_columns: dict[str, Any]) -> pd.DataFrame:
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    # pyarrow better handles nullable types
    # default is using numpy which converts int columns to float if it contains null
    # because it converts to NaN which is float
    df = pd.read_csv(csv_file, dtype_backend="pyarrow")
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
            f"data/release_all_{country}.csv",
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


def prepare_did_variables(df: pd.DataFrame):
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    df["event_date"] = df["event_timestamp"].dt.normalize()

    pre_start_date = pd.Timestamp("2023-03-28", tz="UTC")
    pre_end_date = pd.Timestamp("2023-03-30", tz="UTC")

    post_start_date = pd.Timestamp("2023-04-03", tz="UTC")
    post_end_date = pd.Timestamp("2023-04-06", tz="UTC")

    date_mask = ((df["event_date"] >= pre_start_date) & (df["event_date"] <= pre_end_date) | (df["event_date"] >= post_start_date) & (df["event_date"] <= post_end_date))
    df = df.loc[date_mask].copy()

    df = (
        df.groupby(["username", "event_date"], as_index=False)
        .agg({"release_id": "first", "country": "first"})
    )  # type: ignore

    user_country = df.drop_duplicates("username")[["username", "country"]]
    country_map = dict(zip(user_country["username"], user_country["country"]))

    pre_dates = pd.date_range(pre_start_date, pre_end_date, freq="D")
    post_dates = pd.date_range(post_start_date, post_end_date, freq="D")
    all_dates = pre_dates.union(post_dates)

    # Cartesian product of all users × all global dates
    user_days = (
        pd.MultiIndex.from_product(
            [df["username"].unique(), all_dates],
            names=["username", "event_date"]
        )
        .to_frame(index=False)
    )

    df = user_days.merge(df, on=["username", "event_date"], how="left")

    df["country"] = df["country"].fillna(df["username"].map(country_map)) # type: ignore

    df["Y"] = df["release_id"].notna().astype(int)

    ban_date = pd.Timestamp("2023-03-31", tz="UTC")

    for tau in range(-3, 0):
        current_date = ban_date + pd.Timedelta(days=tau)
        df[f"D_tau_{tau+3}"] = ((df["event_date"] == current_date) & (df["country"] == "italy")).astype(int)

    for tau in range(0, 5): # should this start at 1?
        current_date = ban_date + pd.Timedelta(days=tau+2) # weekend days (april 1st and 2nd) are left away
        df[f"D_tau_{tau+3}"] = ((df["event_date"] == current_date) & (df["country"] == "italy")).astype(int)

    keep_cols = ["username", "event_date", "Y"] + [f"D_tau_{tau+3}" for tau in range(-3, 5)]
    # df = df.loc[:, keep_cols]

    return df


def plot_did(df: pd.DataFrame):
    betas = " + ".join([f"D_tau_{tau+3}" for tau in range(-3, 5)])  # event-time dummies
    formula = f"Y ~ {betas} + C(username) + C(event_date)"  # α_i and λ_t are fixed effects

    # Estimate the model
    model = smf.ols(formula, data=df).fit(
        cov_type="cluster",
        cov_kwds={"groups": df["username"]}  # cluster by user ID
    )

    results = model.params
    ses = model.bse

    beta_df = pd.DataFrame({
        "tau": [int(c.split("_")[-1]) - 3 for c in results.index if c.startswith("D_tau_")],
        "beta": [results[c] for c in results.index if c.startswith("D_tau_")],
        "se": [ses[c] for c in ses.index if c.startswith("D_tau_")]
    })

    beta_df["ci_low"] = beta_df["beta"] - 1.96 * beta_df["se"]
    beta_df["ci_high"] = beta_df["beta"] + 1.96 * beta_df["se"]

    print(beta_df[["tau", "beta", "se"]])
    print("Range of beta:", beta_df["beta"].min(), "→", beta_df["beta"].max())

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
    ax.set_ylabel("Effect on GitHub Releases")
    ax.set_title("The Effect of the ChatGPT Ban on GitHub Releases")

    # Make x-axis integer ticks
    ax.set_xticks(sorted(beta_df["tau"].unique()))

    plt.tight_layout()
    plt.show()


def main():
    df = read_and_merge_dfs()
    df = prepare_did_variables(df)

    # smf is ot able to handle timezones, thus remove it
    df["event_date"] = df["event_date"].dt.tz_localize(None)

    df.to_csv("data/did_release_events.csv", index=False)

    plot_did(df)


if __name__ == "__main__":
    main()
