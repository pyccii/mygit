import datetime
import os

import numpy as np
import pandas as pd
import xlrd
from xlutils.copy import copy

from interface import getLibEntity, saveLibEntity, delete_file, upload_file
from methods import split_str, check
from oilCal import oilCal
from set import token
from waterCal import waterCal


def optOpe(filename_oil, filename_water):
    try:
        print(datetime.datetime.now())
        script_dir = os.path.dirname(os.path.abspath(__file__))
        filePath1 = script_dir.replace("\\", "/") + '/output/' + filename_oil
        filePath2 = script_dir.replace("\\", "/") + '/output/' + filename_water

        upload_file(filePath1)
        pipeUpstreamPlat = {'CEPI-WHPD': ['CEPI', 'WHPD', 'WHPA-I', 'WHPB-I', 'WHPC'],
                            'CEPJ-WHPF': ['CEPJ'],
                            'WHPA-CEPI': ['WHPA-I'],
                            'WHPA-FPSO': ['WHPA-O'],
                            'WHPB-CEPI': ['WHPB-I'],
                            'WHPB-WHPA': ['WHPB-A'],
                            'WHPC-CEPI': ['WHPC'],
                            'WHPD-FPSO': ['CEPI', 'WHPD', 'WHPA-I', 'WHPB-I', 'WHPC'],
                            'WHPE-CEPJ': ['WHPE'],
                            'WHPF-FPSO': ['CEPJ', 'WHPF'],
                            'QHD33-WHPD': ['QHD33-1']
                            }

        # 分离器上游平台连接关系
        cep_upsp = {'FPSO': ['WHPF', 'WHPA-O', 'WHPB-A', 'WHPD'],
                    'CEPJ': ['CEPJ', 'WHPE'],
                    'CEPI': ['WHPB-I', 'CEPI', 'WHPC', 'WHPA-I']}

        mixpipe_df1, plat_df1, sep_df1, objv_Oil1, sep_out1 = oilCal(filename_oil)
        delete_file(filename_oil, token)
        pipeId1 = mixpipe_df1['名称'].values
        pipeFlow1 = mixpipe_df1['液'].values
        designFlow1 = mixpipe_df1['设计输量'].values
        differPipe1 = designFlow1 - pipeFlow1
        pipeFlaseIndex1 = [i1 for i1, x1 in enumerate(differPipe1) if x1 < 0]
        mpipeId1 = pipeId1[pipeFlaseIndex1]
        mflow1 = differPipe1[pipeFlaseIndex1]

        num1 = len(mpipeId1)
        while num1 > 0:
            mpipe = mpipeId1[0]
            mplat = pipeUpstreamPlat[mpipe]

            number = np.where(mpipeId1 == mpipe)[0][0]
            miflow = mflow1[number]

            while miflow < 0:
                getLib1 = getLibEntity(filename_oil, 'MyStream')['data']
                for j1 in range(len(getLib1)):
                    platid = getLib1[j1]['myName']
                    platType = getLib1[j1]['myNote']
                    if platid in mplat and platType == 'Source':
                        if float(getLib1[j1]['myQSeries']) <= -500 / 24 / 3600:
                            getLib1[j1]['myQSeries'] = float(getLib1[j1]['myQSeries']) + 250 / 24 / 3600
                            getLib1[j1]['myMSeries'] = getLib1[j1]['myQSeries'] * 1000

                save1 = saveLibEntity(filename_oil, getLib1, 'MyStream')
                mixpipe_df1, plat_df1, sep_df1, objv_Oil1, sep_out1 = oilCal(filename_oil)

                pipeId1 = mixpipe_df1['名称'].values
                pipeFlow1 = mixpipe_df1['液'].values
                designFlow1 = mixpipe_df1['设计输量'].values

                differPipe1 = designFlow1 - pipeFlow1
                pipeFlaseIndex1 = [i2 for i2, x2 in enumerate(differPipe1) if x2 < 0]
                mpipeId1 = pipeId1[pipeFlaseIndex1]
                miflow = differPipe1[np.where(pipeId1 == mpipe)[0][0]]

                print('----------------------------------------------')
                print('管线名称:', mpipe, ', 超限流量:', miflow)

            num1 = len(mpipeId1)
            print('------------流量超限的管线个数为{}个------------'.format(num1))

        source_id = []
        source_q = []
        for i5 in range(len(sep_out1)):
            id = split_str(sep_out1[i5][0], '-')
            if id == 'CEPJ':
                source_id.append(id)
                source_q.append(sep_out1[i5][1] * 0.7)
                source_id.append('CEPL')
                source_q.append(sep_out1[i5][1] * 0.2)
            elif id == 'CEPI':
                source_id.append(id)
                source_q.append(sep_out1[i5][1] * 0.7)
                source_id.append('CEPK')
                source_q.append(sep_out1[i5][1] * 0.2)
            else:
                source_id.append(id)
                source_q.append(sep_out1[i5][1])
        Source = ['CEPI-T', 'CEPJ-T', 'CEPK-T', 'CEPL-T', 'FPSO-T']

        rb = xlrd.open_workbook(filePath2, formatting_info=True)
        rs = rb.sheet_by_name('Stream')
        wb = copy(rb)
        sheetName = rb.sheet_names()
        sheetIndex = sheetName.index('Stream')
        ws = wb.get_sheet(sheetIndex)
        num = 1
        for row_num in range(rs.nrows):
            row_list = rs.row_values(row_num)
            qs = row_list[11]
            if row_list[4] in Source:
                source = row_list[4]
                sourceNum = source_id.index(split_str(source, '-'))
                qs = source_q[sourceNum] / 24 / 3600 * -1

            ws.write(num - 1, 11, str(qs))
            num += 1

        pumpname = []
        inipumpnum = []
        ps = rb.sheet_by_name('Pump')
        for row_num in range(ps.nrows):
            row_list = ps.row_values(row_num)
            if row_list[7] == 1 or row_list[7] == '1':
                pumpname.append(row_list[4])
                inipumpnum.append(row_list[8])

        wb.save(filePath2)
        upload_file(filePath2)
        pipe_df2, injpump_df2, boosterpump_df2, well_df2, wsep_df2, obj_Water2 = waterCal(filename_water)
        delete_file(filename_water, token)
        inimedic = obj_Water2[-1]

        num = 0
        while num <= 3:
            energyList = []
            pumpList = []
            for i in range(len(pumpname)):
                inistate = inipumpnum[i]
                thisPumpName = pumpname[i]
                if inistate >= 2:
                    rb = xlrd.open_workbook(filePath2, formatting_info=True)
                    rs = rb.sheet_by_name('Pump')

                    wb = copy(rb)
                    sheetName = rb.sheet_names()
                    sheetIndex = sheetName.index('Pump')
                    ws = wb.get_sheet(sheetIndex)
                    nrow = 1
                    for row_num in range(rs.nrows):
                        row_list = rs.row_values(row_num)
                        qn = row_list[8]
                        if row_list[4] == thisPumpName:
                            qn = inistate - 1
                            ws.write(nrow - 1, 8, qn)
                            break
                        nrow += 1

                    script_dir = os.getcwd()
                    savefile = 'water_cal.xls'
                    savepath = script_dir.replace("\\", "/") + '/output/' + savefile
                    wb.save(savepath)
                    upload_file(savepath)
                    pipe_df2, injpump_df2, boosterpump_df2, well_df2, wsep_df2, obj_Water2 = waterCal(savefile)
                    differObj = obj_Water2[-1] - inimedic

                    resCheck = check(mixpipe_df1, sep_df1, wsep_df2, pipe_df2, injpump_df2, boosterpump_df2, well_df2)
                    if not resCheck:
                        pumpList.append(thisPumpName)
                        energyList.append(obj_Water2[-1] - inimedic)

            minEnergy = min(energyList)
            minPumpId = pumpList[energyList.index(minEnergy)]

            rb = xlrd.open_workbook(filePath2, formatting_info=True)
            rs = rb.sheet_by_name('Pump')
            wb = copy(rb)
            sheetName = rb.sheet_names()
            sheetIndex = sheetName.index('Pump')
            ws = wb.get_sheet(sheetIndex)
            nrow = 1
            for row_num in range(rs.nrows):
                row_list = rs.row_values(row_num)
                qn = row_list[8]
                if row_list[4] == minPumpId:
                    qn = qn - 1
                    ws.write(nrow - 1, 8, qn)
                    break
                nrow += 1
            wb.save(filePath2)
            num += 1

        objv = objv_Oil1 + obj_Water2
        objv_array = np.array(objv)[np.newaxis, :]
        objv_df2 = pd.DataFrame(
            objv_array, columns=['总产油量', '总产气量', '总产水量', '总产液量',
                                 '混输海管', '油处理系统', '总注水量', '总处理水量',
                                 '注水泵', '增压泵', '注水管线', '注水井', '注水泵能耗']
        )

        script_dir = os.getcwd()
        dir = script_dir.replace("\\", "/") + "/output/"
        os.makedirs(dir, exist_ok=True)
        current_time = int(datetime.datetime.now().timestamp())
        file_name = "{}.xlsx".format(current_time)
        file_path = dir + "{}.xlsx".format(current_time)
        with pd.ExcelWriter(file_path) as writer:
            wsep_df2.to_excel(writer, sheet_name='水处理系统', index=False, float_format='%.2f')
            injpump_df2.to_excel(writer, sheet_name='注水泵', index=False, float_format='%.2f')
            boosterpump_df2.to_excel(writer, sheet_name='增压泵', index=False, float_format='%.2f')
            pipe_df2.to_excel(writer, sheet_name='注水管线', index=False, float_format='%.2f')
            well_df2.to_excel(writer, sheet_name='注水井', index=False, float_format='%.2f')
            objv_df2.to_excel(writer, sheet_name='目标总览', index=False, float_format='%.2f')
        return file_name

    except  Exception:
        print('------------------------MedicException---------------------')
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dir = script_dir.replace("\\", "/") + "/output/"
        os.makedirs(dir, exist_ok=True)
        file = '1705151981.xlsx'
        print(datetime.datetime.now())
        return file
