import numpy as np
import os
from fastapi.responses import FileResponse
import pandas as pd
import xlrd
import xlwt
from xlutils.copy import copy

def split_str(string: str, sep_symbol):
    if sep_symbol in string:
        first_part = string.split(sep_symbol)[0]
        return first_part
    else:
        return string


def hstack(*args: list):
    def safe_int(x):
        try:
            return float(x)
        except ValueError:
            return x

    arrays = [np.array([safe_int(val) for val in arg], dtype=object)[:, np.newaxis] for arg in args]
    array = np.hstack(arrays)
    return array


def delete_file(url):
    current_directory = os.getcwd()
    file_name0 = url.split("/")[-1]
    file_name = file_name0.split("=")[-1]

    path_dir = 'data'
    file_path0 = os.path.join(current_directory, path_dir)
    file_path = os.path.join(file_path0, file_name)
    # 如果文件已经存在，先删除原文件
    if os.path.exists(file_path):
        os.remove(file_path)


def save_file(file_name, content):
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, data_dir)
    excel_file_path = os.path.join(file_path, file_name)

    with open(excel_file_path, "wb") as f:
        f.write(content)

    return excel_file_path


def uploadOptData(data_dict):
    try:
        platdata = data_dict['liqSetting']
        mixpipedata = data_dict['oilSetting']
        msepdata = data_dict['sepSetting']
        wsepdata = data_dict['waterHandleSetting']
        pipedata = data_dict['waterSetting']
        bpumpdata = data_dict['bootPumpSetting']
        ipumpdata = data_dict['pumpSetting']
        welldata = data_dict['injWellSetting']

        script_dir = os.getcwd()
        base_dir = script_dir.replace('\\', '/') + '/output/'

        rb = xlrd.open_workbook('oil_set.xls', formatting_info=True)
        rs = rb.sheet_by_index(4)
        ro = rb.sheet_by_index(3)
        rc = rb.sheet_by_index(5)

        wb = copy(rb)
        ws = wb.get_sheet(4)
        wo = wb.get_sheet(3)
        wc = wb.get_sheet(5)

        num = 1
        for row_num in range(rs.nrows):
            row_list = rs.row_values(row_num)
            qs = row_list[11]
            if row_list[4] == 'WHPA-I':
                for i in range(len(platdata)):
                    if platdata[i]['platName'] == 'WHPA':
                        qs = platdata[i]['prodLiquid'] * 0.25 * (-1) / 24 / 3600

            elif row_list[4] == 'WHPA-O':
                for i in range(len(platdata)):
                    if platdata[i]['platName'] == 'WHPA':
                        qs = platdata[i]['prodLiquid'] * 0.75 * (-1) / 24 / 3600

            elif row_list[4] == 'WHPB-I':
                for i in range(len(platdata)):
                    if platdata[i]['platName'] == 'WHPB':
                        qs = platdata[i]['prodLiquid'] * 0.35 * (-1) / 24 / 3600

            elif row_list[4] == 'WHPB-A':
                for i in range(len(platdata)):
                    if platdata[i]['platName'] == 'WHPB':
                        qs = platdata[i]['prodLiquid'] * 0.65 * (-1) / 24 / 3600

            else:
                for i in range(len(platdata)):
                    if platdata[i]['platName'] == row_list[4]:
                        qs = platdata[i]['prodLiquid'] * (-1) / 24 / 3600

            if qs == None:
                qs = ''
            ws.write(num - 1, 11, str(qs))
            num += 1

        num = 1
        for row_num in range(ro.nrows):
            row_list = ro.row_values(row_num)
            qo = row_list[6]
            for i in range(len(mixpipedata)):
                if mixpipedata[i]['pipeName'] == row_list[4]:
                    qo = mixpipedata[i]['designThroughput']
            for j in range(len(msepdata)):
                if msepdata[j]['sepName'] == row_list[4]:
                    qo = msepdata[j]['maxProcessing']
            if qo == None:
                qo = ''

            wo.write(num - 1, 6, str(qo))
            num += 1

        num = 1
        for row_num in range(rc.nrows):
            row_list = rc.row_values(row_num)
            qc = row_list[9]
            for i in range(len(platdata)):
                name = row_list[4].split('-')[0]
                if platdata[i]['platName'] == name:
                    qc = platdata[i]['WC'] / 100
            if qc == None:
                qc = ''

            wc.write(num - 1, 9, str(qc))
            num += 1

        file1 = 'oil_set.xls'
        newfile1 = base_dir + file1
        wb.save(newfile1)

        rb2 = xlrd.open_workbook('water_set.xls', formatting_info=True)
        rs2 = rb2.sheet_by_index(3)  # opt
        rp2 = rb2.sheet_by_index(6)  # pump

        # Create a writable copy of the original workbook
        wb2 = copy(rb2)
        ws2 = wb2.get_sheet(3)
        wp2 = wb2.get_sheet(6)

        num = 1
        for row_num in range(rs2.nrows):
            row_list = rs2.row_values(row_num)
            qoo = row_list[6]
            for i in range(len(pipedata)):
                if pipedata[i]['pipeName'] == row_list[4]:
                    qoo = pipedata[i]['designThroughput']

            for j in range(len(wsepdata)):
                if wsepdata[j]['equipmentName'] == row_list[4]:
                    qoo = wsepdata[j]['maxProcessing']

            if qoo == None:
                qoo = ''

            ws2.write(num - 1, 6, str(qoo))
            num += 1

        num = 1
        for row_num in range(rp2.nrows):
            row_list = rp2.row_values(row_num)
            qp = row_list[8]
            for i in range(len(ipumpdata)):
                if ipumpdata[i]['injPumpName'] == row_list[4]:
                    qp = ipumpdata[i]['runStatus']

            for j in range(len(bpumpdata)):
                if bpumpdata[j]['injBoosterPumpName'] == row_list[4]:
                    qp = bpumpdata[j]['runStatus']
            if qp == None:
                qp = ''

            wp2.write(num - 1, 8, str(qp))
            num += 1

        file2 = 'water_set.xls'
        newfile2 = base_dir + file2
        wb2.save(newfile2)
        res = [file1, file2]
        print('1')
        return res

    except Exception:
        print('2')
        file1 = 'oil_set.xls'
        file2 = 'water_set.xls'
        res = [file1, file2]
        return res


