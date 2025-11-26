#%%
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
# %%
df=pd.read_excel('cricket_cleaned_data.xlsx')
print(df)
# %% changing the data type of some columns as they should be 'int','float','object'
df.info()
df.isnull().sum()
# BF      54 non-null     float64 , This shoul be int 
df['BF']=df['BF'].astype(int)

# %%feature engineering part 
df.head(2)
# %%
df['Runs_per_innings']=df['Runs']/df['Inns']
df.head(2)
# %%
df['Boundary%']=(df['4s']*4+df['6s']*6)/df['Runs']

# %%
df.head(2)
# %%
df['Balls_per_innings']=df['BF']/df['Inns']
# %% efficiency/impact
df['Impact_score']=(df['Ave']*df['SR'])/100
# %% CONVERSION rate
df['Conversion_rate']=df['100']/(df['100']+df['50'].replace(0,np.nan))

# %% DUCK 
df['Duck']=df['0']/df['Inns']
# %% DEPENDENCE INDEX
df['Dependence_index']=((df['Runs']/df['Inns'])*df['SR']*df['Conversion_rate'].fillna(0))-(df['Duck']*100)

# %%
df.head(2)
# %% top 15 players by dependence index 
top_player=df.sort_values('Dependence_index',ascending=False).head(15)
plt.figure(figsize=(12,6))
plt.bar(top_player['Player'],top_player['Dependence_index'],color='blue')
plt.xticks(rotation=45)
plt.ylabel('Dependence Index')
plt.title('Top 15 Players by Dependence Index')
plt.show()
# %%impact of players 
impactfull_players= df.sort_values('Impact_score',ascending=False).head(15)
plt.figure(figsize=(12,6))
plt.bar(impactfull_players['Player'],impactfull_players['Impact_score'],color='red')
plt.xticks(rotation=45)
plt.ylabel('Impact_score')
plt.title('Top 15 impactfull player since 2019 to 2025')
plt.show()
# %% agression of batters 
plt.figure(figsize=(20,6))
plt.scatter(df['Balls_per_innings'],df['Boundary%'],c=df['Impact_score'],cmap='plasma',s=100)
plt.colorbar(label='impact score')
plt.xlabel('Balls_per_innings')
plt.ylabel('boundary%')
plt.title('agression')
plt.show()

# %%
# Top 15 players by Duck %
top15_duck = df.sort_values('Duck', ascending=False).head(15)

plt.figure(figsize=(12,6))
plt.bar(top15_duck['Player'], top15_duck['Duck']*100, color='red')
plt.xticks(rotation=45)
plt.ylabel('Duck %')
plt.title('Top 15 Players - Failure Rate (Duck %)')
plt.show()


# %%
from sqlalchemy import create_engine, NVARCHAR, Float
import pandas as pd

# Agar Aggression plot ke liye Balls_per_innings aur Boundary% ka data chahiye, usko column me add kar lo
df_sql = cricket_index[['Player','Dependence_index','Impact_score','Balls_per_innings','Boundary%','Duck']].copy()

# NaN ya inf remove karlo
df_sql.replace([float('inf'), -float('inf')], 0, inplace=True)
df_sql.fillna(0, inplace=True)

#  Dtype dictionary for SQL Server

dtype_dict = {
    'Player': NVARCHAR(100),
    'Dependence_index': Float,
    'Impact_score': Float,
    'Balls_per_innings': Float,
    'Boundary%': Float,
    'Duck': Float
}


# Create connection using Windows Authentication

engine = create_engine("mssql+pyodbc://DESKTOP-IIUOEIH/YourDatabaseName?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

#  Export DataFrame safely

df_sql.to_sql('player_analysis', engine, if_exists='replace', index=False, dtype=dtype_dict)

print(" cricket_index exported to SQL Server successfully!")

# %%
from sqlalchemy import create_engine

server = 'DESKTOP-IIUOEIH'
database = 'cricket_index'  # tumhara SQL Server database
engine = create_engine(f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')


# %%
# Assuming df as python dataframe

df_sql = df[['Player','Dependence_index','Impact_score','Balls_per_innings','Boundary%','Duck']].copy()

# handling values
df_sql.replace([float('inf'), -float('inf')], 0, inplace=True)
df_sql.fillna(0, inplace=True)

# %%
from sqlalchemy.types import NVARCHAR, Float

# Data type define 
dtype_dict = {
    'Player': NVARCHAR(100),
    'Dependence_index': Float(),
    'Impact_score': Float(),
    'Balls_per_innings': Float(),
    'Boundary%': Float(),
    'Duck': Float()
}

# Export
df_sql.to_sql('player_stats', engine, if_exists='replace', index=False, dtype=dtype_dict)
print(" Data transferred to SQL Server")

# %%
from sqlalchemy.types import NVARCHAR, Float

dtype_dict = {
    'Player': NVARCHAR(None),  # None = NVARCHAR(MAX)
    'Dependence_index': Float(),
    'Impact_score': Float(),
    'Balls_per_innings': Float(),
    'Boundary%': Float(),
    'Duck': Float()
}

df_sql.to_sql('player_stats', engine, if_exists='replace', index=False, dtype=dtype_dict)
print(" Data transferred safely!")

# %%
# Final dataset with all engineered features
final_columns = [
    'Player', 'Runs', 'Inns', 'BF', 'Ave', 'SR', '4s', '6s', '100', '50', '0',
    'Runs_per_innings', 'Boundary%', 'Balls_per_innings', 'Impact_score',
    'Conversion_rate', 'Duck', 'Dependence_index'
]

df_final = df[final_columns]
df_final.to_csv('cricket_final_ready.csv', index=False)
print(" Final dataset exported successfully!")

# %%

