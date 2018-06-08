import subprocess
import os
from json import loads
from time import sleep
import xlsxwriter


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
REGION_C = 9
POPULATION_SEGMENT_C = 10
GDP_SEGMENT_C = 11
OVERALL_C = 12

SEGMENTS = [
    "Bardzo mała",
    "Mała",
    "Średnia",
    "Duża",
    "Bardzo duża",
    "Ogromna",
]

def change(c):
    if c == 0:
        return "Stagnacja"
    elif c > 0:
        return "Wzrost"
    else:
        return "Spadek"


def make_title(worksheet, titleRowFormat):
    worksheet.write(START_ROW, COUNTRY_C, 'Państwo', titleRowFormat)
    worksheet.write(START_ROW, YEAR_C, 'Rok', titleRowFormat)
    worksheet.write(START_ROW, POPULATION_C, 'Populacja', titleRowFormat)
    worksheet.write(START_ROW, POPULATION_CHANGE_C, 'Zmiana Populacji', titleRowFormat)
    worksheet.write(START_ROW, GDP_C, 'Gospodarka', titleRowFormat)
    worksheet.write(START_ROW, GDP_CHANGE_C, 'Zmiana Gospodarki', titleRowFormat)
    worksheet.write(START_ROW, EDUCATION_C, 'Edukacja per capita', titleRowFormat)
    worksheet.write(START_ROW, EDUCATION_CHANGE_C, 'Edukacja per capita zmiana', titleRowFormat)
    worksheet.write(START_ROW, REGION_C, 'Region', titleRowFormat)
    worksheet.write(START_ROW, POPULATION_SEGMENT_C, 'Segment Populacji', titleRowFormat)
    worksheet.write(START_ROW, GDP_SEGMENT_C, 'Segment Gospodarczy', titleRowFormat)
    worksheet.write(START_ROW, OVERALL_C, 'Ogółem', titleRowFormat)


PREV_DATA_LINE = {}
def make_data_line(worksheet, segments, row, lineData, dataRowFormat):
    global PREV_DATA_LINE

    worksheet.write(row, COUNTRY_C, lineData['countryName'], dataRowFormat['basic'])
    worksheet.write(row, YEAR_C, lineData['_id']['year'], dataRowFormat['basic'])
    worksheet.write(row, POPULATION_C, SEGMENTS[lineData['population_segment']], dataRowFormat['basic'])
    worksheet.write(row, POPULATION_CHANGE_C, change(lineData['population_change']), dataRowFormat['basic'])
    worksheet.write(row, GDP_C, SEGMENTS[lineData['gdp_segment']], dataRowFormat['basic'])
    worksheet.write(row, GDP_CHANGE_C, change(lineData['gdp_change']), dataRowFormat['basic'])
    worksheet.write(row, EDUCATION_C, lineData['educationPerCapita'], dataRowFormat['basic'])

    educationPerCapitaChange = 0
    if PREV_DATA_LINE.get("_id", {}).get("code") == lineData["_id"]["code"]:
        educationPerCapitaChange = lineData["educationPerCapita"] - PREV_DATA_LINE.get("educationPerCapita", 0)
    worksheet.write(row, EDUCATION_CHANGE_C, educationPerCapitaChange, dataRowFormat['basic'])

    def calculate_color_format(segmentScore):
        if segmentScore < 95:
            return dataRowFormat['red']
        elif segmentScore < 105:
            return dataRowFormat['yellow']
        else:
            return dataRowFormat['green']

    regionPercent = (int)((lineData['educationPerCapita'] / segments[lineData['_id']['year'], "region " + lineData['region']]) * 100)
    worksheet.write(row, REGION_C, "{}%".format(regionPercent), calculate_color_format(regionPercent))

    populationPercent = (int)((lineData['educationPerCapita'] / segments[lineData['_id']['year'], "population {}".format(lineData['population_segment'])]) * 100)
    worksheet.write(row, POPULATION_SEGMENT_C, "{}%".format(populationPercent), calculate_color_format(populationPercent))

    gdpPercent = (int)((lineData['educationPerCapita'] / segments[lineData['_id']['year'], "gdp {}".format(lineData['gdp_segment'])]) * 100)
    worksheet.write(row, GDP_SEGMENT_C, "{}%".format(gdpPercent), calculate_color_format(gdpPercent))

    overallPercent = (int)((lineData['educationPerCapita'] / segments[lineData['_id']['year'], "overall"]) * 100)
    worksheet.write(row, OVERALL_C, "{}%".format(overallPercent), calculate_color_format(overallPercent))

    PREV_DATA_LINE = lineData


def make_data_rows(worksheet, segments, dataRowFormat):
    with open(OUTPUT_FILE) as dataFile:
        for row, lineRaw in enumerate(dataFile.readlines(), START_ROW + 1):
            lineJson = loads(lineRaw)
            make_data_line(worksheet, segments, row, lineJson, dataRowFormat)


def read_segments():
    segments = {}
    with open(SEGMENTS_FILE) as segmentsFile:
        for lineRaw in segmentsFile.readlines():
            segmentLineJson = loads(lineRaw)
            segments[segmentLineJson['_id']['year'], segmentLineJson['_id']['segment']] = segmentLineJson['meanEducationPerCapita']

    return segments


def make_xls():
    workbook = xlsxwriter.Workbook(XLS_FILE)
    worksheet = workbook.add_worksheet()

    worksheet.set_column(COUNTRY_C, EDUCATION_C, 15)
    worksheet.set_row(START_ROW, 15)

    titleRowFormat = workbook.add_format({'bold': True, 'text_wrap': True, 'align': 'center'})
    make_title(worksheet, titleRowFormat)

    segments = read_segments()

    formats = {
        'basic': workbook.add_format({'text_wrap': True, 'align': 'center'}),
        'green': workbook.add_format({'text_wrap': True, 'align': 'center', 'bg_color': '#C6EFCE'}),
        'yellow': workbook.add_format({'text_wrap': True, 'align': 'center', 'bg_color': '#FFEB9C'}),
        'red': workbook.add_format({'text_wrap': True, 'align': 'center', 'bg_color': '#FFC7CE'})
    }

    make_data_rows(worksheet, segments, formats)

    workbook.close()

while 1:
    if os.path.isfile(START_FILE):
        os.remove(START_FILE)
        make_xls()
        subprocess.call([EXCEL, XLS_FILE])
    sleep(2)