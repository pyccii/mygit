import os

import numpy as np
import xlrd
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
        print(data_dict)
        platdata = data_dict['liqSetting']
        mixpipedata = data_dict['oilSetting']
        msepdata = data_dict['sepSetting']
        wsepdata = data_dict['waterHandleSetting']
        pipedata = data_dict['waterSetting']
        bpumpdata = data_dict['bootPumpSetting']
        ipumpdata = data_dict['pumpSetting']
        welldata = data_dict['injWellSetting']
        medicdata = data_dict['mediSetting']

        script_dir = os.getcwd()
        base_dir = script_dir.replace('\\', '/') + '/output/'

        rb = xlrd.open_workbook('oil_set.xls', formatting_info=True)
        rs = rb.sheet_by_index(4)  # Stream
        ro = rb.sheet_by_index(3)  # Opt
        rc = rb.sheet_by_index(5)  # Fluid

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
            qw = row_list[5]
            for i in range(len(mixpipedata)):
                if mixpipedata[i]['pipeName'] == row_list[4]:
                    qo = mixpipedata[i]['designThroughput']
            for j in range(len(msepdata)):
                if msepdata[j]['sepName'] == row_list[4]:
                    qo = msepdata[j]['maxProcessing']
            for k in range(len(medicdata)):
                if medicdata[k]['mediName'] == row_list[4]:
                    qo = medicdata[k]['mediCon']
                    qw = medicdata[k]['unitPrice']
            if qo == None:
                qo = ''
            wo.write(num - 1, 6, str(qo))
            wo.write(num - 1, 5, str(qw))
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
        ws2 = wb2.get_sheet(3)  # Opt
        wp2 = wb2.get_sheet(6)  # Pump

        num = 1
        for row_num in range(rs2.nrows):
            row_list = rs2.row_values(row_num)
            qoo = row_list[6]
            qow = row_list[5]
            for i in range(len(pipedata)):
                if pipedata[i]['pipeName'] == row_list[4]:
                    qoo = pipedata[i]['designThroughput']

            for j in range(len(wsepdata)):
                if wsepdata[j]['equipmentName'] == row_list[4]:
                    qoo = wsepdata[j]['maxProcessing']

            for k in range(len(medicdata)):
                if medicdata[k]['mediName'] == row_list[4]:
                    qoo = medicdata[k]['mediCon']
                    qow = medicdata[k]['unitPrice']
            if qoo == None:
                qoo = ''

            ws2.write(num - 1, 6, str(qoo))
            ws2.write(num - 1, 5, str(qow))
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
            wp2.write(num - 1, 8, qp)
            num += 1

        file2 = 'water_set.xls'
        newfile2 = base_dir + file2
        wb2.save(newfile2)
        res = [file1, file2]
        return res

    except Exception:
        print('------------------------uploadDataException---------------------')
        print(data_dict)
        file1 = 'oil_set.xls'
        file2 = 'water_set.xls'
        res = [file1, file2]
        return res


def check(mixpipe_df1, sep_df1, wsep_df2, pipe_df2, injpump_df2, boosterpump_df2, well_df2):
    limitIndex = []
    limitId = []
    limitFlow = []

    # 混输管线
    pipeId2 = mixpipe_df1['名称'].values
    pipeFlow2 = mixpipe_df1['液'].values
    designFlow2 = mixpipe_df1['设计输量'].values
    differPipe2 = designFlow2 - pipeFlow2
    findex1 = [num for num, x in enumerate(differPipe2) if x < 0]
    fid1 = pipeId2[findex1].tolist()
    fflow1 = differPipe2[findex1].tolist()

    # 油分离器
    sepId2 = sep_df1['名称'].values
    sepFlow2 = sep_df1['液'].values
    sepdesignFlow2 = sep_df1['最大处理量'].values
    differSep2 = sepdesignFlow2 - sepFlow2
    findex2 = [num for num, x in enumerate(differSep2) if x < 0]
    fid2 = sepId2[findex2].tolist()
    fflow2 = differSep2[findex2].tolist()

    # 水分离器
    wsepId2 = wsep_df2['名称'].values
    wsepFlow2 = wsep_df2['处理量'].values
    wsepdesignFlow2 = wsep_df2['最大处理量'].values
    differWsep2 = wsepdesignFlow2 - wsepFlow2
    findex3 = [num for num, x in enumerate(differWsep2) if x < 0]
    fid3 = wsepId2[findex3].tolist()
    fflow3 = differWsep2[findex3].tolist()

    # 注水管线
    wpipeId2 = pipe_df2['名称'].values
    wpipeFlow2 = pipe_df2['输量'].values
    wpipedesignFlow2 = pipe_df2['设计输量'].values
    differWpipe2 = wpipedesignFlow2 - wpipeFlow2
    findex4 = [num for num, x in enumerate(differWpipe2) if x < 0]
    fid4 = wpipeId2[findex4].tolist()
    fflow4 = differWpipe2[findex4].tolist()

    # 增压和注水泵
    injpumpId2 = injpump_df2['名称'].values
    injpumpFlow2 = injpump_df2['排量'].values
    injpumpdesignFlow2 = injpump_df2['额定排量'].values
    differInjpump21 = injpumpdesignFlow2 * 1.3 - injpumpFlow2
    findex5 = [num for num, x in enumerate(differInjpump21) if x < 0]
    fid5 = injpumpId2[findex5].tolist()
    fflow5 = differInjpump21[findex5].tolist()

    boosterpumpId2 = boosterpump_df2['名称'].values
    boosterpumpFlow2 = boosterpump_df2['排量'].values
    boosterpumpdesignFlow2 = boosterpump_df2['额定排量'].values
    differInjpump21 = boosterpumpdesignFlow2 * 1.3 - boosterpumpFlow2
    findex6 = [num for num, x in enumerate(differInjpump21) if x < 0]
    fid6 = boosterpumpId2[findex6].tolist()
    fflow6 = differInjpump21[findex6].tolist()

    # # 注水井
    # wellId2 = well_df2['名称'].values
    # wellFlow2 = well_df2['注水量'].values
    # welldesignFlow2 = well_df2['最大处理量'].values
    # differWell2 = welldesignFlow2 - wellFlow2
    # findex7 = [num for num, x in enumerate(differWell2) if x < 0]
    # fid7 = wellId2[findex7].tolist()
    # fflow7= differWell2[findex7].tolist()
    limitIndex = (findex1 + findex2 + findex3 + findex4 + findex5 + findex6)
    limitId = (fid1 + fid2 + fid3 + fid4 + fid5 + fid6)
    limitFlow = (fflow1 + fflow2 + fflow3 + fflow4 + fflow5 + fflow6)
    return limitIndex
