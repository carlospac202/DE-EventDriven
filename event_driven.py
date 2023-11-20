import pandas as pd
from pandasql import sqldf
import matplotlib.pyplot as plt
from config import Config


class EventDriven(Config):

    def __init__(self):
        """
        Constructor
        """
        self.config()

    def parse_dates(self, date_field):
        return pd.to_datetime(date_field)

    def read_data(self, feature) -> pd.DataFrame:
        file = self._yaml_config.get(feature).get('file')
        return pd.read_csv(file).fillna('')

    def add_engagement_duration(self):
        up_df = self.read_data('user_profiles')
        up_df['signup_date'] = self.parse_dates(up_df['signup_date'])
        up_df['last_login_date'] = self.parse_dates(up_df['last_login_date'])
        up_df['engagement_duration'] = up_df['last_login_date'] - up_df['signup_date']
        return up_df

    def engagement_analysis(self):
        ue_df = self.read_data('user_events')

        # Calculate Daily Active Users (DAU)
        query = """
        SELECT 
              COUNT(DISTINCT user_id) AS users,
              event_date AS dates
        FROM ue_df
        GROUP BY event_date
        """
        dau = sqldf(query, locals())

        # Calculate Monthly Active Users (MAU)
        query = """
        SELECT 
              COUNT(DISTINCT user_id) AS users,
              strftime('%Y-%m', event_date) AS month
        FROM ue_df
        GROUP BY strftime('%Y-%m', event_date)
        """
        mau = sqldf(query, locals())

        # Top 5 Most Engaged Users
        query = """
        SELECT
              user_id,
              COUNT(event_type) AS events
        FROM ue_df
        GROUP BY user_id
        ORDER BY events DESC
        LIMIT 5
        """
        meu = sqldf(query, locals())

        # Most Common Event Types
        query = """
        SELECT
              event_type,
              COUNT(*) AS frequency,
              event_date
        FROM ue_df
        GROUP BY event_type, event_date
        ORDER BY frequency DESC
        """
        met = sqldf(query, locals())

        return dau, mau, meu, met

    def timeline_analysis(self):
        up_df = self.read_data('user_profiles')
        ue_df = self.read_data('user_events')

        up_df['signup_date'] = self.parse_dates(up_df['signup_date'])
        ue_df['event_date'] = self.parse_dates(ue_df['event_date'])

        merged_df = pd.merge(up_df, ue_df, on='user_id', how='inner')

        merged_df['engagement_period'] = (merged_df['event_date'] - merged_df['signup_date']).dt.days // 7

        print(merged_df)

    def process(self):
        dau, mau, meu, met = self.engagement_analysis()
        self.logger.info(met)
        # self.logger.info(f"Daily Active Users (DAU): {dau}")
        # self.logger.info(f"Monthly Active Users (MAU):\n {mau}")


test = EventDriven()
a = test.process()
a
