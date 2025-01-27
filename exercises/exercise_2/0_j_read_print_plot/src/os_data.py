import sys 
from pathlib import Path
import pandas as pd
import duckdb
import seaborn as sns


data_path = Path(__file__).parent 
print(f"\n {data_path} \n")

print("\n")
print(f"{sys.version =}\n")


athlete_events_df = pd.read_csv(data_path / "athlete_events.csv")
#athlete_events_df.head().to_csv(data_path/ "athlete_events_head.csv")


new_df = duckdb.query(
    """
    SELECT 
    Year, Sex, count(Sex) as count 
    FROM athlete_events_df 
    group by Year, Sex 
    ORDER BY Year ASC
    """
).df()

sns_plot = sns.lineplot(new_df, x=new_df["Year"], y=new_df["count"], hue="Sex")
fig = sns_plot.get_figure()
fig.savefig(Path(__file__).parent / "output.png")