## About This Group Project
Home teams are widely believed to have the advantages of playing at a familiar venue, a supportive crowd and the last-change rule.
As individuals new to ice hockey, our group wanted to quantify the advantages - how large is the gap in win rate and performance
when a team plays at home versus away. 

## Project Source
This ELT project uses the [Kaggle NHL Game Data](https://www.kaggle.com/datasets/martinellis/nhl-game-data) by Martin Ellis.
The dataset spans over 19 seasons of NHL games from the year 2000 to 2020(excluding the cancelled season in 2004/2005). 
Our project used 3 tables - 'game_teams_stats.csv', 'game.csv' and 'team_info.csv' for this analysis.

## Tool Stack
- Microsoft Fabric (trial account)
- Medallion architecture with the Bronze and Silver in a Lakehouse and Gold in a Lakehouse. 
- Spark SQL and PySpark with Delta tables in the Gold Layer
- Power BI for reporting

## Database Design
The Gold layer follows a star schema because this structure supports analytic work. 
The project compares performance at team-level and thus the fact table grain is one row per team per game.
(Screenshot available: Refer to Screenshots/Semantic Modelling.JPG)

## Data Quality
The silver layer inherited timezone-venue inconsistency where several unrelated timezones are associated with 1 venue, e.g Ericsson Globe is associated with 3 timezone ids - America/New_York, America/Los_Angeles, America/Denver. (Screenshot available: Refer to timezone-venue_inconsistency.JPG)
It is unclear if venue or timezone id was the incorrect data, and there was no source to confirm. However, each value is a valid timezone ID, so my teammate preserved the data as-is. 
As venue is no longer unique, I have used composite key (venue and timezone id), thereby avoiding any bloating of fact table in a join.
To preserve original data and limit downstream impact, I added surrogate key to represent pairs of venue and timezone info. 

## My Scope
This repo shows only my individual contribution to the Gold Layer and does not run standalone as it is only part of a pipeline. (Notebook: Refer to Silver to Gold Notebooks/Silver_to_Gold Notebook_final.ipynb)
The Fabric workspace was a trial account that is not publicly accessible, so the transformation code is exported as a notebook.

**Silver to Gold transformation**  
- Created 1 fact table with grain at one row per team per game with opponent's stats alongside. (Refer to Screenshots/Silver Lake - *)
- Created 4 dimension tables - season, venue, date and team info (Refer to Screenshots/Gold Lake - *)
- Gold Delta tables are built with SparkSQL
- Validated and logged result with PySpark to prepare for semantic modelling: 
	- Table existence, 
	- Key-uniqueness and 
	- Non-null

**Power BI Report Page**
Contributed 1 report page to existing 5 reports.
My 1-page report(Refer to Screenshots/NHL_report dashboard_layout.JPG) provides a dashboard layout to let users compare a selected team's perfromance at home versus away in 3 areas:
-Shoot / Goal Efficiency
-Puck Possession
-Penalty

## Team
A 4-member group project completed as part of Generation Singapore's Junior Data Engineer Programme. 

Credit to my teammates for:
- Bronze ingestion
- Data cleaning at the Silver layer
- Semantic Modelling
- Testing
- The other reports and the analysis report


