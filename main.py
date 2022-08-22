import click
import pandas as pd
import sqlite3
from report_params import read_report_params, ReportParams


def get_report(report_params: ReportParams, config_path):

    # создадим подключение к базе данных
    conn = sqlite3.connect('source_data/de_test.db')

    # создадим dataframes из таблиц:
    sales = import_db_tables(con=conn, name_of_table='sales')
    kkt_info = import_db_tables(con=conn, name_of_table='kkt_info')
    kkt_categories = import_db_tables(con=conn, name_of_table='kkt_categories')
    kkt_activity = import_db_tables(con=conn, name_of_table='kkt_activity')

    # загрузим данные из таблицы с разметкой брендов:
    prod_names = pd.read_csv(report_params.product_name_path)

    # объединим таблицы с продажами и таблицу с разметкой и оставим только бренды из входящего файла:
    sales_with_brands = sales.join(prod_names.set_index('product_name_hash'), on="product_name_hash").dropna()

    # оставим данные в заданном промежукте времени:
    sales_with_brands_filtered_by_date = sales_with_brands[sales_with_brands.receipt_date.between(
        report_params.period.date_from, report_params.period.date_to)]

    # проверим наличие фиьтров и применим их:
    if report_params.kkt_category_filters:
        # оставим только последние версии классификации
        last_vers = kkt_categories.groupby('kkt_number').max().version.reset_index()
        actual_categories = last_vers.merge(kkt_categories[['kkt_number', 'version', 'category']],
                                            left_on=['kkt_number', 'version'], right_on=['kkt_number', 'version'],
                                            how='left').drop_duplicates()

        # объединим с полученной ранее таблицей
        result_data = sales_with_brands_filtered_by_date.join(actual_categories.set_index('kkt_number'), on="kkt_number")

        # применим фильтр по категориям:
        list_of_categories = report_params.kkt_category_filters.replace(' ', '').split(',')
        result_data = result_data[result_data.category.isin(list_of_categories)]
    else:
        result_data = sales_with_brands_filtered_by_date

    # все разрезы соберем в список:
    group_by_list = []

    if report_params.group_by.receipt_date:
        group_by_list.append('receipt_date')

    if report_params.group_by.region:
        group_by_list.append('region')
        # добавим колонку с регионом в результирующие даанные:
        result_data = result_data.join(kkt_info[['kkt_number', 'region']].set_index('kkt_number'), on="kkt_number")

    if report_params.group_by.channel:
        group_by_list.append('channel')
        # добавим колонку с каналом в результирующие даанные, сначала определим тип магазина:

        # оределим активная ли касса
        kkt_activity['active'] = ~(
                (kkt_activity['receipt_date_min'] > report_params.period.date_to) | (
                kkt_activity['receipt_date_max'] < report_params.period.date_from)
        )
        kkt_info_active = kkt_info.join(kkt_activity[['kkt_number', 'active']].set_index('kkt_number'), on='kkt_number')

        # посчитаем количество действующих магазинов:
        org_type = kkt_info_active[kkt_info_active.active].groupby('org_inn').count()['shop_id'].reset_index().rename(
            columns={"shop_id": "shop_count"})

        # определим тип магазина (сетевой/несетевой)
        org_type['channel'] = org_type["shop_count"].apply(lambda t: "chain" if t > 2 else "nonchain")

        result_data = result_data.join(org_type[['org_inn', 'channel']].set_index('org_inn'), on='org_inn')

    # сделаем группировку по разрезам и посчитаем процент по товару
    if len(group_by_list):
        features = group_by_list.copy()
        group_by_list.append('brand')
        report = result_data.groupby(group_by_list).sum().total_sum.reset_index()
        report['total_sum_pct'] = report[features+['total_sum']].groupby(features).transform(lambda x: round((x / x.sum()), 2))
    else:
        report = result_data.groupby('brand').sum().total_sum.reset_index()
        report['total_sum_pct'] = (report.total_sum / report.sum().total_sum).round(2)

    # загрузим отчет в csv файл:
    report.to_csv(config_path.replace('config.yaml', 'report.csv'), index=False)


@click.command(name="config")
@click.argument("config_path")
def get_report_command(config_path: str):
    params = read_report_params(config_path)
    get_report(params, config_path)


def import_db_tables(con, name_of_table):
    df =  pd.read_sql(f'SELECT * FROM {name_of_table}', con=con)
    return df


if __name__ == "__main__":
    get_report_command()
