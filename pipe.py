import pandas as pd


class Pipe():
    """
    Class to contain and process subscription and booking data
    """

    def __init__(self, subscriptions_file, bookings_file):
        """
        Initialize a Pipe object and read the data files as 
        dataframes and do some initial cleanup

        Args:
            subscriptions_file (str): path to subscriptions csv
            bookings_file (str): path to bookings csv
        """
        self.subscriptions_df = pd.read_csv(subscriptions_file)
        self.bookings_df = pd.read_csv(bookings_file)

        # drop unneeded columns
        self.subscriptions_df.drop(columns=['Unnamed: 0'], inplace=True)
        self.bookings_df.drop(columns='Unnamed: 0', inplace=True)

    def deduplicate_subscriptions(self):
        """
        Mark duplicated rows in subscriptions_df as invalid and then 
        drop the duplicates, keeping just one copy of the row
        """
        # change dates datatype
        self.subscriptions_df['dates'] = pd.to_datetime(
            self.subscriptions_df['dates'])

        # first find duplicate values
        duplicated = self.subscriptions_df.duplicated(
            subset=['dates', 'sub_id'], keep=False)
        # mark the duplicates as invalid
        self.subscriptions_df = pd.concat(
            [self.subscriptions_df, duplicated], axis=1)
        # give the new column an informative name
        self.subscriptions_df.columns = [
            'sub_id', 'status', 'dates', 'invalid_status']
        # drop the duplicate columns
        self.subscriptions_df.drop_duplicates(
            subset=['sub_id', 'dates'], inplace=True)

    def fill_missing_months(self):
        """
        Fill in the missing months between dates per subscriber in subscriptions_df 
        """
        # Make a dataframe with min and max month per sub_id
        # first get min date per sub_id
        dates = self.subscriptions_df.groupby(
            'sub_id')['dates'].min().to_frame(name='min')
        # get max date per group and concatenate to new dates df
        max_dates = self.subscriptions_df.groupby(
            'sub_id')['dates'].max().to_frame(name='max')
        dates = dates.merge(max_dates, how='inner', on='sub_id')

        # Create MultiIndex with date ranges per sub_id
        midx = pd.MultiIndex.from_frame(
            dates.apply(
                lambda x: pd.date_range(x['min'], x['max'], freq='MS'), axis=1
            ).explode().reset_index(name='dates')[['dates', 'sub_id']]
        )

        # flatten the index
        self.subscriptions_df = (
            self.subscriptions_df.set_index(['dates', 'sub_id'])
            # use fill_value to mark the missing months as invalid
                .reindex(midx, fill_value=True)
                .reset_index()
        )
        self.subscriptions_df.sort_values(
            by=['sub_id', 'dates']).to_csv('test1.csv')

    def updated_statuses(self):
        """ 
        Replace the subscription status from months that are 
        duplicated or missing for each subscriber. If there is a
        previous valid status, use that. If no previous valid status
        is available, use the following valid status. If neither of 
        these exist, set the status to canceled
        """

        def get_last_valid_status(row):
            """
            Helper function to find a valid status as defined above

            Args:
                row (pd.Series): a row of the subscription df

            Returns:
                str: 'active' or 'canceled'
            """
            if row['invalid_status']:
                sub_id = row['sub_id']
                month = row['dates']
                prev_valid_status = self.subscriptions_df[
                    (self.subscriptions_df['sub_id'] == sub_id)
                    & (self.subscriptions_df['dates'] < month)
                    & (self.subscriptions_df['invalid_status'] == False)
                ].sort_values(by='dates')

                # if there is no previous valid status check for next valid status
                if len(prev_valid_status) == 0:
                    next_valid_status = self.subscriptions_df[(self.subscriptions_df['sub_id'] == sub_id)
                                                              & (self.subscriptions_df['dates'] > month)
                                                              & (self.subscriptions_df['invalid_status'] == False)
                                                              ].sort_values(by='dates')
                    # if there is no next valid status return canceled
                    if len(next_valid_status) == 0:
                        return 'canceled'

                # if theres a prev valid status, get the last status
                # from the sorted list
                if len(prev_valid_status) > 0:
                    valid_status = prev_valid_status.iloc[-1, 2]

                # if theres a next valid status, get the first
                # status from the sorted list
                elif len(next_valid_status) > 0:
                    valid_status = next_valid_status.iloc[0, 2]

                return valid_status
            else:
                return row['status']

        # replace the invalid status with the last/next valid status or canceled
        self.subscriptions_df['status'] = self.subscriptions_df.apply(
            lambda x: get_last_valid_status(x),
            axis=1)


    def calculate_months_since_first_subscription(self):
        """
        Calculate the number of months since each subscriber's first 
        subscription, for every month. Set to N/A if they have no 
        previous subscription
        """
        def get_months_since_first_subscription(row):
            """
            helper function to fetch number of months since first subscription
            per subscriber on the monthly grain

            Args:
                row (pd.Series): a row of the subscription df

            Returns:
                int: number of months since first subscription
            """
            sub_id = row['sub_id']
            month = row['dates']
            # get first month with active status before current month
            first_sub_month = self.subscriptions_df[(self.subscriptions_df['sub_id'] == sub_id)
                                                    & (self.subscriptions_df['status'] == 'active')
                                                    & (self.subscriptions_df['dates'] <= month)
                                                    ]['dates'].min()

            # if there is no previous month with active subscription return None
            if first_sub_month is pd.NaT:
                return None

            # convert to period to subtract months then .n to get it as an int
            months_since_first_subscription = (row['dates'].to_period('M')
                                               - first_sub_month.to_period('M')).n

            return months_since_first_subscription

        self.subscriptions_df['months_since_first_subscription'] = self.subscriptions_df.apply(
            lambda x: get_months_since_first_subscription(x), axis=1).astype('Int64')

    def get_num_active_and_canceled_months(self):
        """
        Get the number of months a subscriber was active and number
        of months a subscriber was canceled up to each month
        """
        def get_status_counts(row):
            """
            Helper function to fetch the number of active months 
            and number of canceled months for up to the given month 

            Args:
                row (pd.Series): a row of the subscription df

            Returns:
                int: number of months since first subscription
            """
            sub_id = row['sub_id']
            month = row['dates']

            counts = self.subscriptions_df[(self.subscriptions_df['sub_id'] == sub_id)
                                           & (self.subscriptions_df['dates'] <= month)
                                           # use value_counts to avoid creating
                                           # two functions to count statuses
                                           ]['status'].value_counts()

            # use get in case a status is not in the counts
            canceled_months = counts.get('canceled', 0)
            active_months = counts.get('active', 0)
            return active_months, canceled_months

        self.subscriptions_df[['active_months', 'canceled_months']] = self.subscriptions_df.apply(
            lambda x: get_status_counts(x), axis=1, result_type='expand')

    def calculate_months_since_status_change(self):
        """
        Get the number of months since a subscriber did their last 
        status change for each month
        """
        def get_months_since_status_change(row):
            """
            Helper function to calculate number of months since
            subscriber's last status change. Returns N/A if there
            was no previous status change 

            Args:
                row (pd.Series): a row of the subscription df

            Returns:
                int: number of months since last status change
            """
            sub_id = row['sub_id']
            month = row['dates']
            status = row['status']
            last_status_change_date = self.subscriptions_df[(self.subscriptions_df['sub_id'] == sub_id)
                                                            & (self.subscriptions_df['dates'] < month)
                                                            & (self.subscriptions_df['status'] != status)
                                                            ]['dates'].max()
            # if there is no last status change return 0
            if last_status_change_date is pd.NaT:
                return None
            months_since_status_change = (row['dates'].to_period('M')
                                          - last_status_change_date.to_period('M')).n

            return months_since_status_change

        self.subscriptions_df['months_since_status_change'] = self.subscriptions_df.apply(
            lambda x: get_months_since_status_change(x), axis=1).astype('Int64')

    def get_monthly_bookings(self):
        """
        Create a new df containing the number of confirmed bookings 
        per subcscriber per month
        """

        # first transform the timestamps into months
        def timestamp_to_month(booking_date):
            """
            Helper function to transform a timestamp into a date. 
            The date is the first day of the month of the timestamp.

            Args:
                booking_date (str): timestamp of a booking

            Returns:
                str: the first day of the month when the timestamp occured
            """
            # remove time
            timestamp = booking_date.split(' ')[0]
            # change to first of month to group by month
            timestamp = timestamp.split('-')
            timestamp[2] = '01'
            timestamp = '-'.join(timestamp)
            return timestamp

        self.bookings_df['month'] = self.bookings_df['booking_date'].apply(
            lambda x: timestamp_to_month(x))

        self.bookings_per_month = (self.bookings_df[
            self.bookings_df['booking_status'] == 'Confirmed']
            .groupby(['month', 'subscriber_id'])
            .count()
            # just get booking_status so we're not left with
            # extra column
            ['booking_status']
            # flatten the index
            .reset_index())

        # change datatype to datetime
        self.bookings_per_month['month'] = pd.to_datetime(
            self.bookings_per_month['month'])

        # rename columns to identify number of confirmed bookings and make join cleaner
        self.bookings_per_month.columns = [
            'dates', 'sub_id', 'confirmed_bookings']

    def save_to_csv(self):
        """
        Combine the procesed subcriptions data with the processed monthly bookings 
        data, remove unnecessary columns and save to csv in the local directory
        """
        # create a new df joining the subscription df with the monthly bookings df
        self.output_df = self.subscriptions_df.merge(self.bookings_per_month,
                                                     on=['sub_id', 'dates'],
                                                     how='left')

        # replace the null values of confirmed_bookings with 0
        self.output_df['confirmed_bookings'] = self.output_df[
            'confirmed_bookings'].fillna(0).astype(int)

        # drop column used for processing
        self.output_df.drop(columns='invalid_status', inplace=True)

        self.output_df.sort_values(by=['sub_id', 'dates']).to_csv(
            'DE_challenge_results.csv', index=False)


def main(subscriptions_file, bookings_file):
    """
    Instantiate a pipeline object which will read and process the files.
    Saves the result to csv in the local directory

    Args:
        subscriptions_file (str): path to subscriptions csv
        bookings_file (str): path to bookings csv
    """
    p = Pipe(subscriptions_file, bookings_file)
    p.deduplicate_subscriptions()
    p.fill_missing_months()
    p.updated_statuses()
    p.calculate_months_since_first_subscription()
    p.get_num_active_and_canceled_months()
    p.calculate_months_since_status_change()
    p.get_monthly_bookings()
    p.save_to_csv()


main('Subscription.csv', 'Bookings.csv')
