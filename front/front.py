import xlsxwriter
import subprocess
from json import loads

BIGDATA_DIR = "C:\\Users\\Maciej Kolanski\\Desktop\\bidata"
XLS_FILE = BIGDATA_DIR + "\\" + "output.xlsx"
START_FILE = BIGDATA_DIR + "\\" + "start"
SEGMENTS_FILE = BIGDATA_DIR + "\\" + "segments_output.json"
OUTPUT_FILE = BIGDATA_DIR + "\\" + "main_output.json"
EXCEL = "C:\\Program Files\\Microsoft Office 15\\root\\office15\\EXCEL.EXE"

START_ROW = 2
COUNTRY_C = 1
YEAR_C = 2
POPULATION_C = 3
POPULATION_CHANGE_C = 4
GDP_C = 5
GDP_CHANGE_C = 6
EDUCATION_C = 7
EDUCATION_CHANGE_C = 8

SEGMENTS = [
    "Bardzo mała",
    "Mała",
    "Średnia",
    "Duża",
    "Bardzo duża",
    "Ogromna",
]

CHANGE = {
    -1: "Spadek",
    0: "Stagnacja",
    1: "Wzrost"
}


def make_title(worksheet, titleRowFormat):
    worksheet.write(START_ROW, COUNTRY_C, 'Państwo', titleRowFormat)
    worksheet.write(START_ROW, YEAR_C, 'Rok', titleRowFormat)
    worksheet.write(START_ROW, POPULATION_C, 'Populacja', titleRowFormat)
    worksheet.write(START_ROW, POPULATION_CHANGE_C, 'Zmiana Populacji', titleRowFormat)
    worksheet.write(START_ROW, GDP_C, 'Gospodarka', titleRowFormat)
    worksheet.write(START_ROW, GDP_CHANGE_C, 'Zmiana Gospodarki', titleRowFormat)
    worksheet.write(START_ROW, EDUCATION_C, 'Edukacja per capita', titleRowFormat)


PREV_DATA_LINE = None
def make_data_line(worksheet, row, lineData, dataRowFormat):
    worksheet.write(row, COUNTRY_C, lineData['countryName'], dataRowFormat)
    worksheet.write(row, YEAR_C, lineData['_id']['year'], dataRowFormat)
    worksheet.write(row, POPULATION_C, SEGMENTS[lineData['population_segment']], dataRowFormat)
    worksheet.write(row, POPULATION_CHANGE_C, CHANGE[lineData['population_change']], dataRowFormat)
    worksheet.write(row, GDP_C, SEGMENTS[lineData['gdp_segment']], dataRowFormat)
    worksheet.write(row, GDP_CHANGE_C, CHANGE[lineData['gdp_change']], dataRowFormat)
    worksheet.write(row, EDUCATION_C, lineData['educationPerCapita'], dataRowFormat)


def make_data_rows(worksheet, dataRowFormat):
    with open(OUTPUT_FILE) as dataFile:
        for row, lineRaw in enumerate(dataFile.readlines(), START_ROW + 1):
            lineJson = loads(lineRaw)
            make_data_line(worksheet, row, lineJson, dataRowFormat)


def make_xls():
    workbook = xlsxwriter.Workbook(XLS_FILE)
    worksheet = workbook.add_worksheet()

    worksheet.set_column(COUNTRY_C, EDUCATION_C, 15)
    worksheet.set_row(START_ROW, 15)

    titleRowFormat = workbook.add_format({'bold': True, 'text_wrap': True, 'align': 'center'})
    make_title(worksheet, titleRowFormat)

    dataRowFormat = workbook.add_format({'text_wrap': True, 'align': 'center'})
    make_data_rows(worksheet, dataRowFormat)

    workbook.close()


make_xls()

subprocess.call([EXCEL, XLS_FILE])