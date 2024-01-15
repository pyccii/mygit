import datetime
import os

import numpy as np
import pandas as pd

from interface import getLibEntity, saveLibEntity, delete_file, upload_file
from methods import split_str
from oilCal import oilCal
from set import token
from waterCal import waterCal


def processSim(filename_oil, filename_water):
    try:
        script_dir = os.getcwd()
        filePath1 = script_dir.replace("\\", "/") + '/output/' + filename_oil
        filePath2 = script_dir.replace("\\", "/") + '/output/' + filename_water

        upload_file(filePath1)
        upload_file(filePath2)
        # 集输流程模拟
        mixpipe_df, plat_df, sep_df, objv_Oil, sep_out = oilCal(filename_oil)

        # 集输系统终点3个分离器，注水系统起点5个水罐，做一个流量分配
        source_id = []
        source_q = []
        for i in range(len(sep_out)):
            id = split_str(sep_out[i][0], '-')
            if id == 'CEPJ':
                source_id.append(id)
                source_q.append(sep_out[i][1] * 0.7)
                source_id.append('CEPL')
                source_q.append(sep_out[i][1] * 0.3)
            elif id == 'CEPI':
                source_id.append(id)
                source_q.append(sep_out[i][1] * 0.7)
                source_id.append('CEPK')
                source_q.append(sep_out[i][1] * 0.3)
            else:
                source_id.append(id)
                source_q.append(sep_out[i][1])

        # 1.先计算生成缓存
        waterCal(filename_water)

        # 2.删除文件
        delete_file(filename_water, token)

        # 3.读取缓存
        getLib = getLibEntity(filename_water, 'MyStream')['data']

        # 4.修改缓存中注水流程文件的边界条件
        Source = ['CEPI-T', 'CEPJ-T', 'CEPK-T', 'CEPL-T', 'FPSO-T']
        for i in range(len(getLib)):
            Id = getLib[i]['myName']
            if Id in Source:
                id_num = source_id.index(split_str(Id, '-'))
                getLib[i]['myQSeries'] = source_q[id_num] / 24 / 3600 * -1
                getLib[i]['myMSeries'] = [getLib[i]['myQSeries'] * 1000]

        # 5.保存缓存
        saveData = saveLibEntity(filename_water, getLib, 'MyStream')

        # 6.注水流程计算
        pipe_df, injpump_df, boosterpump_df, well_df, wsep_df, obj_Water = waterCal(filename_water)
        medicPrice = [objv_Oil[-1] + obj_Water[-1]]
        objv = objv_Oil + obj_Water + medicPrice
        objv_array = np.array(objv)[np.newaxis, :]
        objv_df = pd.DataFrame(

            objv_array, columns=['总产油量', '总产气量', '总产水量', '总产液量',
                                 '混输海管', '油处理系统', '油处理药剂', '总注水量', '总处理水量',
                                 '注水泵', '增压泵', '注水管线', '注水井', '注水泵能耗', '水处理药剂', '总药剂花费']
        )
        script_dir = os.getcwd()
        dir = script_dir.replace("\\", "/") + "/output/"
        current_time = int(datetime.datetime.now().timestamp())
        file_name = "{}.xlsx".format(current_time)
        file_path = dir + "{}.xlsx".format(current_time)
        with pd.ExcelWriter(file_path) as writer:
            plat_df.to_excel(writer, sheet_name='平台', index=False, float_format='%.2f')
            mixpipe_df.to_excel(writer, sheet_name='混输管线', index=False, float_format='%.2f')
            sep_df.to_excel(writer, sheet_name='油处理系统', index=False, float_format='%.2f')
            wsep_df.to_excel(writer, sheet_name='水处理系统', index=False, float_format='%.2f')
            injpump_df.to_excel(writer, sheet_name='注水泵', index=False, float_format='%.2f')
            boosterpump_df.to_excel(writer, sheet_name='增压泵', index=False, float_format='%.2f')
            pipe_df.to_excel(writer, sheet_name='注水管线', index=False, float_format='%.2f')
            well_df.to_excel(writer, sheet_name='注水井', index=False, float_format='%.2f')
            objv_df.to_excel(writer, sheet_name='目标总览', index=False, float_format='%.2f')

        res_df = [mixpipe_df, sep_df, wsep_df, pipe_df, injpump_df, boosterpump_df, well_df]
        nonlist = []
        for i in range(len(mixpipe_df['名称'])):
            if mixpipe_df['液'][i] > mixpipe_df['设计输量'][i]:
                nonlist.append(mixpipe_df['名称'][i])

        for i in range(len(sep_df['名称'])):
            if sep_df['液'][i] > sep_df['最大处理量'][i]:
                nonlist.append(sep_df['名称'][i])

        # for i in range(len(wsep_df['名称'])):
        #     if wsep_df['处理量'][i] > wsep_df['最大处理量'][i]:
        #         nonlist.append(wsep_df['名称'][i])

        for i in range(len(pipe_df['名称'])):
            if pipe_df['输量'][i] > pipe_df['设计输量'][i]:
                nonlist.append(pipe_df['名称'][i])

        # for i in range(len(well_df['名称'])):
        #     if well_df['注水量'][i] > well_df['最大输量'][i]:
        #         nonlist.append(well_df['名称'][i])

        print('超过设计能力的设备设施有：', nonlist)
        print(datetime.datetime.now())
        return file_name
    except  Exception:
        print('----------ProcessException--------------')
        print(datetime.datetime.now())
        file = '1705149773.xlsx'
        return file
