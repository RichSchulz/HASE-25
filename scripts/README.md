# Scripts for the paper "An Empirical Analysis of ChatGPT's Impact on Developer Productivity Using Italy's ChatGPT Ban as a Natural Experiment"

The following directory contains various scripts which were used while working at this paper. The files are organized in the following directories:

- `./analyses`: Contains the scripts with which the results of our analyses have been calculated, as well as downloaded.
- `./data`: Contains GitHub users for the corresponding countries, which were imported to a custom BigQuery table. Source: https://github.com/sodalabsio/github_scrape, as described in https://arxiv.org/abs/2304.09339
- `./exploration`: Contains scripts and plots which were used for exploration, but did not end up being used for the final report.
- `./large_data`: Contains large csv and sqlite3 files, which were loaded from BigQuery or the GitHub Rest API.
- `./reproduction`: Contains scripts for the reproduction of the release event DiD analysis, as described in https://arxiv.org/abs/2304.09339. Exact reproduction was not possible, due to incomplete information in the original paper, but the general ideas were reproducable.

Scripts are expected to be run from this directory.

Create a venv (`python -m venv .venv`), activate it (`source .venv/bin/activate`) and then install the required packages from `requirements.txt` (`pip install -r requirements.txt`)
