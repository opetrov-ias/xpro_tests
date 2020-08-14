import logging
import sys
import os
from datetime import datetime, timedelta
import boto3
import snowflake.connector

logger = logging.getLogger(__name__)


def get_date_list(today_str, number_of_days):
    dt_obj = datetime.strptime(today_str, '%Y-%m-%d')
    dates = []
    dt_obj_from = dt_obj - timedelta(number_of_days)
    for i in range(number_of_days):
        d_obj = dt_obj_from + timedelta(i)
        dates.append(d_obj.strftime('%Y-%m-%d'))
    return dates


class S3Serv:

    def __init__(self):
        self.con = None
        self.client = boto3.client('s3')
        self.paginator = self.client.get_paginator('list_objects_v2')
        self.bucket = 'partner-measured'
        self.log_prefix = 'raw_logs'

    def get_size(self, partner, date):
        yyyy, mm, dd = date.split('-')
        logger.info('DATE: {0}'.format(date))
        prefix = '{0}/{1}/{2}/{3}/{4}'.format(partner, self.log_prefix, yyyy, mm, dd)
        logger.info('PATH: {0}'.format(prefix))
        pages = self.paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        total_size = 0
        for page in pages:
            for obj in page['Contents']:
                total_size += obj['Size']
        return total_size


class SnowFlakeServ:

    def __init__(self):
        self.USER = os.environ['SNOWSQL_USER']
        self.PASSWORD = os.environ['SNOWSQL_PWD']
        self.ACCOUNT = os.environ['SNOWSQL_ACCOUNT']
        self.WAREHOUSE = os.environ['SNOWSQL_WAREHOUSE']
        self.DATABASE = os.environ['SNOWSQL_DATABASE']
        self.SCHEMA = os.environ['SNOWSQL_SCHEMA']
        self.REGION = 'us-east-1'
        self.con = snowflake.connector.connect(
            user=self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT,
            region=self.REGION,
            warehouse=self.WAREHOUSE,
            database=self.DATABASE,
            schema=self.SCHEMA
        )

    def get_size(self, partner, date):
        cur = self.con.cursor()
        pm_id = 0
        if partner == 'pinterest':  #todo remove hardcode
            pm_id = 11
        try:
            cur.execute("""select sum(imps) from analytics.AGG_PARTNER_MEASURED_VIEWABILITY v 
                where hit_date = '{0}' and measurement_source_id = {1}""".format(date, pm_id))
            one_row = cur.fetchone()
            value = one_row[0]
        finally:
            cur.close()
        return value


class PMPipelineCheck:

    def __init__(self, context, in_serv, out_serv):
        self.context = context
        self.date_list = get_date_list(self.context.get('today'), self.context.get('scan_period', 1))
        self.in_serv = in_serv
        self.out_serv = out_serv

    def get_input_data(self, partner):
        data = {}
        for date in self.date_list:
            data[date] = self.in_serv.get_size(partner, date)
        return data

    def get_output_data(self, partner):
        data = {}
        for date in self.date_list:
            data[date] = self.out_serv.get_size(partner, date)
        return data

    def check_data(self, partner, data):
        logger.warning('check_data running...')
        for d in range(self.context.get('scan_period', 1)):
            # logger.info('data: {0}'.format(d))
            None

    @staticmethod
    def evaluate_data(dt_in, dt_out):
        logger.warning('INPUT_DATA:{0}'.format(dt_in))
        logger.warning('OUTPUT_DATA:{0}'.format(dt_out))
        if not dt_in:
            logger.error('no input data')
        if not dt_out:
            logger.error('no output data')
        result = []
        for date in dt_in.keys():
            value_in = dt_in[date]
            value_out = dt_out[date]
            if value_in and value_out:
                rc = value_out * 100.0 / value_in
                result.append((date, value_in, value_out, rc))
            else:
                result.append((date, value_in, value_out, None)) 
        return result

    @staticmethod
    def apply_rule(total):
        average = 0
        count = 0
        for record in total:
            if not record[3]:
                continue
            average += record[3]
            count += 1
        average = average / count
        last_value = total[-1][3]
        difference = abs(average - last_value)
        logger.warning('AVG: {0} DIFF: {1}'.format(average, difference))

        if difference > 0.005:
            logger.critical('DIFF: {0}'.format(difference))
        elif difference > 0.001:
            logger.error('DIFF: {0}'.format(difference))
        else:
            logger.warning('DIFF: {0}'.format(difference))

    def scan_partners(self, partners):
        for partner in partners:
            logger.info('partner:{0}'.format(partner))
            input_data = self.get_input_data(partner)
            # self.check_data(partner, input_data)

            output_data = self.get_output_data(partner)
            # self.check_data(partner, output_data)

            total = self.evaluate_data(input_data, output_data)
            logger.warning('TOTAL:{0}'.format(total))

            self.apply_rule(total)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)

    all_partners = ['pinterest',]
    prev_day = datetime.today()-timedelta(1)
    day = prev_day.strftime('%Y-%m-%d')
    global_context = {'scan_period': 3, 'today': day}

    s3_serv = S3Serv()
    sf_serv = SnowFlakeServ()

    pmc = PMPipelineCheck(global_context, in_serv=s3_serv, out_serv=sf_serv)
    pmc.scan_partners(all_partners)
