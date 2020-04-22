import pandas as pd
import re, os
import glob
import time
import datetime as dt
import yaml


def get_time(start_time):
    diff = time.time() - start_time
    return diff


def yaml_init(filename):
    with open(filename, 'r') as stream:
        f_yaml = yaml.safe_load(stream)
    return f_yaml


def excel_date(year):
    # Note, not 31st Dec but 30th!
    delta = float((dt.date(int(year), 1, 1) - dt.date(1899, 12, 30)).days)
    return delta


def df_convert(df, row_name="Test", new_column="Test"):
    s_old_column = df.columns[0]
    s_old_index = df.index.name if df.index.name else 'index'
    df = df.reset_index().set_index([[row_name] * len(df)])
    df = df.pivot(columns=s_old_index, values=[s_old_column])
    df = df.rename(columns={s_old_column:new_column}).rename_axis('Company')\
        .rename_axis(["Parent", "Child"], axis="columns")
    return df


def excel_calculating(FILENAME, HEADER=0, FOOTER=0, NA_VALUES=[]):
    # ##################################################
    # ----- Блок инициализации
    pd.options.display.max_colwidth = None  # Устранение обрезания значений в pandas
    l_error = []
    SHEET_NAMES = []
    ENGINE = 'pyxlsb' if FILENAME.endswith('.xlsb') else None
    df_sheets = pd.DataFrame()
    # ----- Конец блока инициализации -----

    # ##################################################
    # ----- Блок подготовки файла и фильтрации данных -----
    start_time = time.time()
    # Выгрузка данных из EXCEL
    d_series = pd.read_excel(FILENAME,
                                    nrows=HEADER - 2,
                                    usecols='A',
                                    squeeze=True,
                                    engine=ENGINE,
                                    sheet_name=None)
    print('Первое чтение файла: {}'.format(get_time(start_time)))

    # ##################################################
    # ----- Блок фильтрации листов -----
    for s_sheet in d_series.keys():
        if 'Public Ownership' in d_series[s_sheet].to_string():
            SHEET_NAMES.append(s_sheet)
        else:
            print("Sheet: {}".format(s_sheet))
            s_description = 'Status: Bad Sheet Structure. Comment: No \'Public Ownership\' phrase ' \
                            'on the "5" row on the column "A". Maybe this list not CIQ formatting'
            print(s_description)
            l_error.append({'00. Filename': FILENAME, '01. Sheet': s_sheet, '05. Description': s_description})
    # ----- Конец блока фильтрации листов -----
    # ##################################################

    data_frame_all = pd.read_excel(FILENAME,
                                    header=HEADER - 1,
                                    skipfooter=FOOTER,
                                    index_col="Holder",
                                    engine=ENGINE,
                                    sheet_name=SHEET_NAMES,
                                    na_values={'% Of CSO': NA_VALUES})
    print('Второе чтение файла: {}'.format(get_time(start_time)))
    print('--------------------------------------')
    # ----- Конец блока подготовки файла и фильтрации данных -----

    # ##################################################
    # ----- Блок обработки листов файла -----
    # В цикл передаются две переменные d_series и data_frame
    for s_sheet in SHEET_NAMES:
        data_frame = data_frame_all[s_sheet]

        # ----- Блок обработки исключений -----
        # Замена названия столбца, для случаев "Share Held"
        if 'Shares Held' in data_frame.columns:
            data_frame = data_frame.rename(columns={'Shares Held':'Common Stock Equivalent Held'})
        # ----- Конец обработки исключений -----

        # ----- Блок обработки общей информации Excel листа -----
        try:
            s_company_raw = d_series[s_sheet][3]
            s_date_raw = d_series[s_sheet][9]
            # Пример sCompany_raw: "Permanent TSB Group Holdings plc (ISE:IL0A) > Public Ownership > Detailed"
            # Пример s_date_raw: "Position Date: Dec-31-2013"
            l_ticker = re.findall(r'\((.+\:.+)\)', s_company_raw)
            l_exchange = re.findall(r'\((.+)\:.+\)', s_company_raw)
            l_ticker_id = re.findall(r'\(.+\:(.+)\)', s_company_raw)
            l_company_name = re.findall(r'^(.*?)\s[(>]', s_company_raw)
            l_year = re.findall(r'^.*-(\d\d\d\d)$', s_date_raw)
            # Забираем первое значение в итераторе re.findall, если его нет, то записываем ''
            s_ticker = next(iter(l_ticker), '')
            s_exchange = next(iter(l_exchange), '')
            s_ticker_id = next(iter(l_ticker_id), '')
            s_company_name = next(iter(l_company_name), '')
            s_year = next(iter(l_year), '')
            print("Sheet: %s, Ticker: %s, Year: %s, Company: %s" % (s_sheet, s_ticker, s_year, s_company_name))
            dt.datetime(int(s_year), 1, 1)  # Это тест на корректность выгрузки года, для try-except.
        except ValueError:
            print("Sheet: {}".format(s_sheet))
            s_description = 'Status: Bad Sheet Structure. Comment: Can\'t parsing 5A or 11A cell. ' \
                            'Commonly Date (11A) formatting problem'
            print(s_description)
            l_error.append({'00. Filename': FILENAME, '01. Sheet': s_sheet, '05. Description': s_description})
            continue
        # ----- Конец обработки общей части информации -----

        # ----- Блок составления DataFrame в общими данными по листу -----
        in_company = pd.MultiIndex.from_product([['01. Company Information'],
                                                ['Company', 'Year', 'Ticker', 'Exchange', 'Ticker_ID']],
                                                names=['Parent', 'Child'])
        df_company = pd.DataFrame([[s_company_name, s_year, s_ticker, s_exchange, s_ticker_id]],\
                                columns=in_company, index=[s_company_name])
        # ----- Конец блока -----

        # ----- Блок обработки информации по инвесторам -----
        if data_frame['Position Date'].dtype == 'datetime64[ns]':
            serial_date = dt.datetime(int(s_year), 1, 1)
        else:
            serial_date = excel_date(s_year)
        try:
            df_filter = data_frame[['% Of CSO',
                                    'Owner Type',
                                    'Market Cap Emphasis',
                                    'Investment Orientation',
                                    'Calculated Investment Style',
                                    'Portfolio Turnover Category']] \
                                    [(data_frame['% Of CSO'] > 0) &\
                                    (data_frame['Position Date'] >= serial_date)]

            # Подсчет суммарного % Of CSO сгруппированного по полям
            df_owt = df_filter.groupby(['Owner Type']).sum()
            df_mce = df_filter.groupby(['Market Cap Emphasis']).sum()
            df_cis = df_filter.groupby(['Calculated Investment Style']).sum()
            df_ptc = df_filter.groupby(['Portfolio Turnover Category']).sum()
            df_owt_io = df_filter.groupby(['Investment Orientation', 'Owner Type']).sum()

            ps_top_cso = pd.Series(dtype=float)
            ps_top_cso['Top 03 CSO'] = df_filter['% Of CSO'].nlargest(3).sum()
            ps_top_cso['Top 05 CSO'] = df_filter['% Of CSO'].nlargest(5).sum()
            ps_top_cso['Top 10 CSO'] = df_filter['% Of CSO'].nlargest(10).sum()
            ps_top_cso['Top 20 CSO'] = df_filter['% Of CSO'].nlargest(20).sum()

            ps_mi_ptc_owt = df_filter.groupby(['Portfolio Turnover Category'])['Owner Type'].value_counts()

            df_filter = data_frame[['% Of CSO',
                                    'Owner Type',
                                    'Common Stock Equivalent Held']]\
                                    [(data_frame['% Of CSO'] > 0) &\
                                    (data_frame['Position Date'] >= serial_date)]

            df_top = df_filter.nlargest(3, columns='Common Stock Equivalent Held')
            df_top['Name'] = df_top.index

            df_owt = df_convert(df_owt, row_name=s_company_name, new_column='06. Owner Type (% Of CSO by)')
            df_mce = df_convert(df_mce, row_name=s_company_name, new_column='07. Market Cap Emphasis (% Of CSO by)')
            df_cis = df_convert(df_cis, row_name=s_company_name, new_column='08. Calculated Investment Style (% Of CSO by)')
            df_ptc = df_convert(df_ptc, row_name=s_company_name, new_column='10. Portfolio Turnover Category (% Of CSO by)')

            df_owt_io = df_owt_io.T\
                            .rename({'% Of CSO': s_company_name})\
                            .rename(columns=lambda x: '09. Investment Orientation ({}) (% Of CSO by)'.format(x), level=0)

            df_top_cso = df_convert(ps_top_cso.to_frame(), row_name=s_company_name, new_column='02. Tops')

            df_top = df_top.stack().to_frame().T\
                            .set_index([[s_company_name]])\
                            .rename_axis('Company')\
                            .rename_axis(["Parent", "Child"], axis="columns")
            df_top.columns.set_levels(['03. Top 1', '04. Top 2', '05. Top 3'], level=0, inplace=True)

            df_mi_ptc_owt = ps_mi_ptc_owt.to_frame().T\
                            .rename({'Owner Type': s_company_name})\
                            .rename_axis('Company')\
                            .rename_axis(["Parent", "Child"], axis="columns")\
                            .rename(columns=lambda x: '11. Portfolio Turnover Category ({}) (Count by Owner Type)'.format(x), level=0)

            df_sheet_snapshot = pd.concat([df_company, df_owt, df_mce, df_cis, df_ptc, df_owt_io, df_top_cso, df_top, df_mi_ptc_owt], axis=1)
        except (IndexError, TypeError):
            s_description = 'Status: IndexError. Comment: Smth wrong with column values. Commonly with \'% Of CSO\''
            print(s_description)
            l_error.append({'00. Filename': FILENAME,
                            '01. Sheet': s_sheet,
                            '02. Company': s_company_name,
                            '03. Ticker': s_ticker,
                            '04. Year': s_year,
                            '05. Description': s_description})
            continue
        except KeyError:
            s_description = 'Status: KeyError. Comment: This sheet haven\'t some columns. Commonly missed \'Owner Type\''
            print(s_description)
            l_error.append({'00. Filename': FILENAME,
                            '01. Sheet': s_sheet,
                            '02. Company': s_company_name,
                            '03. Ticker': s_ticker,
                            '04. Year': s_year,
                            '05. Description': s_description})
            continue

        df_sheets = pd.concat([df_sheets, df_sheet_snapshot])
        print('Status: Processed. Comment: 0')
        print('--------------------------------------')
    # Конец цикла обработки DataFrame'ов выгруженных из EXCEL
    # ##################################################

    return df_sheets, l_error


def main():
    main_time = time.time()
    # ##################################################
    # ----- Инициализация констант из файла конфигуарции -----
    l_constants = ['SOURCE_FOLDER', 'OUTPUT_EXCEL_FILE', 'HEADER_PLACER', 'FOOTER_COUNTER', 'NA_VALUES']
    etc = yaml_init('defines.yaml')
    for s_constant in l_constants:
        globals()[s_constant] = etc[s_constant]
    OUTPUT_RESULT = OUTPUT_EXCEL_FILE + '.xlsx'
    OUTPUT_ERROR_LOGS = OUTPUT_EXCEL_FILE + '_error_logs.xlsx'
    OUTPUT_ERROR_SHEETS = OUTPUT_EXCEL_FILE + '_error_sheets.xlsx'
    df_result = pd.DataFrame()
    l_error_logs = []
    # ----- Конец инициализации констант -----
    # ##################################################

    # for file in os.listdir(SOURCE_FOLDER):
        # path = SOURCE_FOLDER+'/'+file
    for path in glob.glob(SOURCE_FOLDER+'/*.xls*', recursive=True):
        print('###############################################################################################')
        print('Начало обработки файла: {}'.format(path))
        df_file_snapshot, l_error_sheets = excel_calculating(path,
                                            HEADER=HEADER_PLACER,
                                            FOOTER=FOOTER_COUNTER,
                                            NA_VALUES=NA_VALUES)

        df_result = pd.concat([df_result, df_file_snapshot])
        if l_error_sheets:
            l_error_logs += l_error_sheets
        print('Конец обработки файла: {}'.format(get_time(main_time)))
        print('###############################################################################################')

    # ----- Блок записи результатов в файлы -----
    # Запись результатов расчетов в файл
    df_result.to_excel(OUTPUT_RESULT)

    # Запись логов ошибок в отдельный файл
    df_errors = pd.DataFrame(l_error_logs)
    df_errors.sort_index(axis=1, inplace=True)
    df_errors.to_excel(OUTPUT_ERROR_LOGS)

    # Запись необработанных листов в отдельный файл
    # условие - только если l_error_logs не пустой
    if l_error_logs:
        i_sheet_number = 0
        with pd.ExcelWriter(OUTPUT_ERROR_SHEETS) as writer:
            for d_error in l_error_logs:
                s_filename = d_error['00. Filename']
                s_sheetname = d_error['01. Sheet']
                s_new_sheetname = '{}. {}'.format(str(i_sheet_number), s_sheetname)
                ENGINE = 'pyxlsb' if s_filename.endswith('.xlsb') else None
                df_error_sheets = pd.read_excel(s_filename, sheet_name=s_sheetname, engine=ENGINE)
                df_error_sheets.to_excel(writer, sheet_name=s_new_sheetname, index=False)
                i_sheet_number += 1
    # Все файлы записаны
    # ----- Конец блок записи в файлы -----
    print('Конец обработки скрипта: {}'.format(get_time(main_time)))
    print('Length df_result: {}'.format(len(df_result)))
    print('Length df_errors: {}'.format(len(df_errors)))


if __name__ == '__main__':
    main()
