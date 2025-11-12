import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import statsmodels.api as sm
from statsmodels.formula.api import ols
from stargazer.stargazer import Stargazer

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# Load the data
print("Loading data...")
# Load VPN data
df_vpn = pd.read_csv('vpn.csv', skiprows=2)  # Skip header rows
df_vpn.columns = ['date', 'germany', 'italy', 'austria']
df_vpn['date'] = pd.to_datetime(df_vpn['date'])
df_vpn = df_vpn.dropna()
df_vpn['search_type'] = 'VPN'

# Load Tor browser data
df_tor = pd.read_csv('tor browser.csv', skiprows=2)  # Skip header rows
df_tor.columns = ['date', 'germany', 'italy', 'austria']
df_tor['date'] = pd.to_datetime(df_tor['date'])
df_tor = df_tor.dropna()
df_tor['search_type'] = 'Tor Browser'

# Combine both datasets
df = pd.concat([df_vpn, df_tor], ignore_index=True)

# Treatment date: April 1st, 2023
treatment_date = pd.to_datetime('2023-04-01')

# Reshape data from wide to long format for DiD analysis
df_long = pd.melt(df, id_vars=['date', 'search_type'], 
                  value_vars=['germany', 'italy', 'austria'],
                  var_name='country', value_name='search_volume')

# Create treatment indicator (1 for Italy, 0 for control countries)
df_long['treatment'] = (df_long['country'] == 'italy').astype(int)

# Create post-treatment indicator (1 for dates >= April 1st, 2023)
df_long['post'] = (df_long['date'] >= treatment_date).astype(int)

# Create interaction term (DiD coefficient)
df_long['treatment_post'] = df_long['treatment'] * df_long['post']

print("\nData Summary:")
print(df_long.groupby(['search_type', 'country', 'post'])['search_volume'].describe())

# Run separate DiD regressions for VPN and Tor Browser
print("\n" + "="*60)
print("Difference-in-Differences Regression Results")
print("="*60)

# Separate data by search type
df_vpn_long = df_long[df_long['search_type'] == 'VPN']
df_tor_long = df_long[df_long['search_type'] == 'Tor Browser']

# VPN regression: Y = α + β1*Treatment + β2*Post + β3*(Treatment*Post) + ε
print("\n" + "-"*60)
print("VPN Regression")
print("-"*60)
model_vpn = ols('search_volume ~ treatment + post + treatment_post', data=df_vpn_long).fit()
print(model_vpn.summary())

# Extract VPN DiD coefficient
did_coef_vpn = model_vpn.params['treatment_post']
did_pvalue_vpn = model_vpn.pvalues['treatment_post']
did_se_vpn = model_vpn.bse['treatment_post']

print("\n" + "-"*60)
print(f"VPN DiD Coefficient (Treatment Effect): {did_coef_vpn:.2f}")
print(f"Standard Error: {did_se_vpn:.2f}")
print(f"P-value: {did_pvalue_vpn:.4f}")
print(f"95% Confidence Interval: [{did_coef_vpn - 1.96*did_se_vpn:.2f}, {did_coef_vpn + 1.96*did_se_vpn:.2f}]")
print("-"*60)

# Tor Browser regression: Y = α + β1*Treatment + β2*Post + β3*(Treatment*Post) + ε
print("\n" + "-"*60)
print("Tor Browser Regression")
print("-"*60)
model_tor = ols('search_volume ~ treatment + post + treatment_post', data=df_tor_long).fit()
print(model_tor.summary())

# Extract Tor Browser DiD coefficient
did_coef_tor = model_tor.params['treatment_post']
did_pvalue_tor = model_tor.pvalues['treatment_post']
did_se_tor = model_tor.bse['treatment_post']

print("\n" + "-"*60)
print(f"Tor Browser DiD Coefficient (Treatment Effect): {did_coef_tor:.2f}")
print(f"Standard Error: {did_se_tor:.2f}")
print(f"P-value: {did_pvalue_tor:.4f}")
print(f"95% Confidence Interval: [{did_coef_tor - 1.96*did_se_tor:.2f}, {did_coef_tor + 1.96*did_se_tor:.2f}]")
print("-"*60)

# Create LaTeX table using stargazer with both models
print("\n" + "="*60)
print("LaTeX Regression Table")
print("="*60)
stargazer = Stargazer([model_vpn, model_tor])
stargazer.covariate_order(['treatment', 'post', 'treatment_post', 'Intercept'])
stargazer.rename_covariates({
    'treatment': 'Italy', 
    'post': 'Post-Ban', 
    'treatment_post': 'Italy × Post-Ban', 
    'Intercept': 'Constant'
})
stargazer.show_model_numbers(False)
stargazer.show_degrees_of_freedom(True)
stargazer.custom_columns(['VPN', 'Tor Browser'])

# Save LaTeX table to file
latex_table = stargazer.render_latex()
with open('did_regression_table.tex', 'w') as f:
    f.write(latex_table)

print("\nLaTeX table saved to 'did_regression_table.tex'")
print("\nLaTeX Table:")
print("="*60)
print(latex_table)
print("="*60)

# Create visualization - VPN plots
print("\nCreating VPN plots...")

# VPN Time series plot
fig1, ax1 = plt.subplots(figsize=(12, 6))
for country in ['italy', 'austria', 'germany']:
    country_data = df_vpn_long[df_vpn_long['country'] == country]
    label = country.capitalize()
    if country == 'italy':
        ax1.plot(country_data['date'], country_data['search_volume'], 
                marker='o', label=label, linewidth=2, markersize=6)
    else:
        ax1.plot(country_data['date'], country_data['search_volume'], 
                marker='s', label=label, linewidth=2, markersize=5, alpha=0.7)

ax1.axvline(x=treatment_date, color='red', linestyle='--', linewidth=2, 
           label='Ban Start (April 1st)')
ax1.set_xlabel('Date', fontsize=12)
ax1.set_ylabel('VPN Search Volume', fontsize=12)
ax1.set_title('VPN Search Volume Over Time', fontsize=14, fontweight='bold')
ax1.legend(loc='best', fontsize=10)
ax1.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('did_vpn_timeseries_plot.png', dpi=300, bbox_inches='tight')
print("VPN time series plot saved as 'did_vpn_timeseries_plot.png'")
plt.close(fig1)

# VPN Before/After plot
fig2, ax2 = plt.subplots(figsize=(10, 6))
pre_post_data_vpn = df_vpn_long.groupby(['country', 'post'])['search_volume'].mean().reset_index()
pre_post_pivot_vpn = pre_post_data_vpn.pivot(index='country', columns='post', values='search_volume')
pre_post_pivot_vpn.columns = ['Before Ban', 'After Ban']
x = np.arange(len(pre_post_pivot_vpn.index))
width = 0.35
bars1 = ax2.bar(x - width/2, pre_post_pivot_vpn['Before Ban'], width, 
                label='Before Ban', alpha=0.8)
bars2 = ax2.bar(x + width/2, pre_post_pivot_vpn['After Ban'], width, 
                label='After Ban', alpha=0.8)
italy_idx = list(pre_post_pivot_vpn.index).index('italy')
bars1[italy_idx].set_color('orange')
bars2[italy_idx].set_color('darkorange')
ax2.set_xlabel('Country', fontsize=12)
ax2.set_ylabel('Average Search Volume', fontsize=12)
ax2.set_title('Average VPN Search Volume: Before vs After Ban', 
              fontsize=14, fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels([c.capitalize() for c in pre_post_pivot_vpn.index])
ax2.legend(fontsize=10)
ax2.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.savefig('did_vpn_before_after_plot.png', dpi=300, bbox_inches='tight')
print("VPN before/after plot saved as 'did_vpn_before_after_plot.png'")
plt.close(fig2)

# Create visualization - Tor Browser plots
print("Creating Tor Browser plots...")
df_tor_long = df_long[df_long['search_type'] == 'Tor Browser']

# Tor Browser Time series plot
fig3, ax3 = plt.subplots(figsize=(12, 6))
for country in ['italy', 'austria', 'germany']:
    country_data = df_tor_long[df_tor_long['country'] == country]
    label = country.capitalize()
    if country == 'italy':
        ax3.plot(country_data['date'], country_data['search_volume'], 
                marker='o', label=label, linewidth=2, markersize=6)
    else:
        ax3.plot(country_data['date'], country_data['search_volume'], 
                marker='s', label=label, linewidth=2, markersize=5, alpha=0.7)

ax3.axvline(x=treatment_date, color='red', linestyle='--', linewidth=2, 
           label='Ban Start (April 1st)')
ax3.set_xlabel('Date', fontsize=12)
ax3.set_ylabel('Tor Browser Search Volume', fontsize=12)
ax3.set_title('Tor Browser Search Volume Over Time', fontsize=14, fontweight='bold')
ax3.legend(loc='best', fontsize=10)
ax3.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('did_tor_timeseries_plot.png', dpi=300, bbox_inches='tight')
print("Tor Browser time series plot saved as 'did_tor_timeseries_plot.png'")
plt.close(fig3)

# Tor Browser Before/After plot
fig4, ax4 = plt.subplots(figsize=(10, 6))
pre_post_data_tor = df_tor_long.groupby(['country', 'post'])['search_volume'].mean().reset_index()
pre_post_pivot_tor = pre_post_data_tor.pivot(index='country', columns='post', values='search_volume')
pre_post_pivot_tor.columns = ['Before Ban', 'After Ban']
x = np.arange(len(pre_post_pivot_tor.index))
width = 0.35
bars1 = ax4.bar(x - width/2, pre_post_pivot_tor['Before Ban'], width, 
                label='Before Ban', alpha=0.8)
bars2 = ax4.bar(x + width/2, pre_post_pivot_tor['After Ban'], width, 
                label='After Ban', alpha=0.8)
italy_idx = list(pre_post_pivot_tor.index).index('italy')
bars1[italy_idx].set_color('orange')
bars2[italy_idx].set_color('darkorange')
ax4.set_xlabel('Country', fontsize=12)
ax4.set_ylabel('Average Search Volume', fontsize=12)
ax4.set_title('Average Tor Browser Search Volume: Before vs After Ban', 
              fontsize=14, fontweight='bold')
ax4.set_xticks(x)
ax4.set_xticklabels([c.capitalize() for c in pre_post_pivot_tor.index])
ax4.legend(fontsize=10)
ax4.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.savefig('did_tor_before_after_plot.png', dpi=300, bbox_inches='tight')
print("Tor Browser before/after plot saved as 'did_tor_before_after_plot.png'")
plt.close(fig4)

# Additional summary statistics
print("\n" + "="*60)
print("Summary Statistics by Group and Period")
print("="*60)
summary = df_long.groupby(['search_type', 'treatment', 'post'])['search_volume'].agg(['mean', 'std', 'count'])
print(summary)

# Calculate DiD manually for verification - VPN
italy_before_vpn = df_vpn_long[(df_vpn_long['country'] == 'italy') & (df_vpn_long['post'] == 0)]['search_volume'].mean()
italy_after_vpn = df_vpn_long[(df_vpn_long['country'] == 'italy') & (df_vpn_long['post'] == 1)]['search_volume'].mean()
control_before_vpn = df_vpn_long[(df_vpn_long['country'] != 'italy') & (df_vpn_long['post'] == 0)]['search_volume'].mean()
control_after_vpn = df_vpn_long[(df_vpn_long['country'] != 'italy') & (df_vpn_long['post'] == 1)]['search_volume'].mean()
did_manual_vpn = (italy_after_vpn - italy_before_vpn) - (control_after_vpn - control_before_vpn)

# Calculate DiD manually for verification - Tor Browser
italy_before_tor = df_tor_long[(df_tor_long['country'] == 'italy') & (df_tor_long['post'] == 0)]['search_volume'].mean()
italy_after_tor = df_tor_long[(df_tor_long['country'] == 'italy') & (df_tor_long['post'] == 1)]['search_volume'].mean()
control_before_tor = df_tor_long[(df_tor_long['country'] != 'italy') & (df_tor_long['post'] == 0)]['search_volume'].mean()
control_after_tor = df_tor_long[(df_tor_long['country'] != 'italy') & (df_tor_long['post'] == 1)]['search_volume'].mean()
did_manual_tor = (italy_after_tor - italy_before_tor) - (control_after_tor - control_before_tor)

print("\n" + "="*60)
print("Manual DiD Calculation - VPN:")
print(f"Italy Before: {italy_before_vpn:.2f}")
print(f"Italy After: {italy_after_vpn:.2f}")
print(f"Control Before: {control_before_vpn:.2f}")
print(f"Control After: {control_after_vpn:.2f}")
print(f"DiD (Manual): {did_manual_vpn:.2f}")
print(f"DiD (Regression): {did_coef_vpn:.2f}")
print("\nManual DiD Calculation - Tor Browser:")
print(f"Italy Before: {italy_before_tor:.2f}")
print(f"Italy After: {italy_after_tor:.2f}")
print(f"Control Before: {control_before_tor:.2f}")
print(f"Control After: {control_after_tor:.2f}")
print(f"DiD (Manual): {did_manual_tor:.2f}")
print(f"DiD (Regression): {did_coef_tor:.2f}")
print("="*60)

