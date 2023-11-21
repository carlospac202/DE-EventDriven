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
        up_df = up_df.dropna()
        return up_df.head(5)

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
        ue_df = self.read_data('user_events')
        up_df = self.read_data('user_profiles')
        up_df['signup_date'] = self.parse_dates(up_df['signup_date'])

        # Calculate daily event counts for each user
        query = """
            SELECT 
                   user_id,
                   COUNT(event_type) AS event_count,
                   event_date
            FROM ue_df
            WHERE user_id = 2
            GROUP BY user_id, event_date
        """
        user_daily_engagement = sqldf(query, locals())


        # Plot individual user engagement trends
        plt.figure(figsize=(10, 6))
        for user, data in user_daily_engagement.groupby('user_id'):
            plt.plot(data['event_date'], data['event_count'], label=f'User {user}')

        plt.title('Daily User Engagement Trends')
        plt.xlabel('Date')
        plt.ylabel('Number of Events')
        plt.legend(loc='upper right')
        plt.grid(True)
        plt.tight_layout()
        plt.show()


        # Calculate overall daily event counts (all users combined)
        overall_daily_event_counts = user_daily_engagement.groupby('event_date')['event_count'].sum()

        # Detect anomalies or patterns in overall engagement
        rolling_mean = overall_daily_event_counts.rolling(window=7, min_periods=1).mean()
        rolling_std = overall_daily_event_counts.rolling(window=7, min_periods=1).std()

        # Plot overall engagement with rolling mean and standard deviation
        plt.figure(figsize=(10, 6))
        plt.plot(overall_daily_event_counts.index, overall_daily_event_counts, label='Overall Daily Events')
        plt.plot(rolling_mean.index, rolling_mean, label='Rolling Mean', linestyle='--')
        plt.fill_between(rolling_std.index, rolling_mean - rolling_std, rolling_mean + rolling_std, alpha=0.3,
                         label='Rolling Std')
        plt.title('Overall Daily User Engagement with Rolling Mean and Std Deviation')
        plt.xlabel('Date')
        plt.ylabel('Number of Events')
        plt.legend(loc='upper right')
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    def behavioral_analysis(self):
        ue_df = self.read_data('user_events')

        # SQL-like query to analyze event type sequences for each user
        query = """
            SELECT 
                   user_id,
                   GROUP_CONCAT(event_type, ' -> ') AS event_sequence
            FROM ue_df
            GROUP BY user_id
        """

        user_event_sequences = sqldf(query, locals())

        # SQL-like query to determine most popular pages
        query = """
            SELECT 
                   page,
                   COUNT(*) AS page_count
            FROM ue_df
            GROUP BY page
            ORDER BY page_count DESC
        """

        popular_pages = sqldf(query, locals())

        return user_event_sequences, popular_pages


    def process(self):
        # Adding new feature engagement duration
        self.logger.info(self.add_engagement_duration())

        # Engagement Analysis
        dau, mau, meu, met = self.engagement_analysis()

        # Timeline Analysis
        self.logger.info(self.timeline_analysis())

        # Behavioral Analysis
        df1, df2 = self.behavioral_analysis()
        self.logger.info(df1)
