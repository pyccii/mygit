import os
import pandas as pd
import numpy as np


def adjust(file1, file2):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        filePath1 = base_dir.replace("\\", "/") + '/output/' + file1
        filePath2 = base_dir.replace("\\", "/") + '/output/' + file2

        excel_file1 = pd.ExcelFile(filePath1)
        data_dict1 = {}
        for sheet_name in excel_file1.sheet_names:
            df1 = excel_file1.parse(sheet_name).dropna(how='all').reset_index(drop=True)
            data_dict1[sheet_name] = df1

        excel_file2 = pd.ExcelFile(filePath2)
        data_dict2 = {}
        for sheet_name in excel_file2.sheet_names:
            df2 = excel_file2.parse(sheet_name).dropna(how='all').reset_index(drop=True)
            data_dict2[sheet_name] = df2

        plat_data1 = data_dict1['平台']
        mixpipe_data1 = data_dict1['混输管线']
        msep_data1 = data_dict1['油处理系统']
        wsep_data1 = data_dict1['水处理系统']
        injpump_data1 = data_dict1['注水泵']
        boostpump_data1 = data_dict1['增压泵']
        pipe_data1 = data_dict1['注水管线']
        injwell_data1 = data_dict1['注水井']

        plat_data2 = data_dict2['平台']
        mixpipe_data2 = data_dict2['混输管线']
        msep_data2 = data_dict2['油处理系统']
        wsep_data2 = data_dict2['水处理系统']
        injpump_data2 = data_dict2['注水泵']
        boostpump_data2 = data_dict2['增压泵']
        pipe_data2 = data_dict2['注水管线']
        injwell_data2 = data_dict2['注水井']

        adjusted_schemes = []
        for i in range(len(plat_data1['名称'])):
            for j in range(len(plat_data2['名称'])):
                if plat_data1['名称'][i] == plat_data2['名称'][j]:
                    if plat_data1['液'][i] - plat_data2['液'][j] >= 50:
                        adjusted_schemes.append([plat_data1['名称'][i], '平台', plat_data2['液'][j], plat_data1['液'][i]])

        for i in range(len(msep_data1['名称'])):
            for j in range(len(msep_data2['名称'])):
                if msep_data1['名称'][i] == msep_data2['名称'][j]:
                    if msep_data1['液'][i] - msep_data2['液'][j] >= 50:
                        adjusted_schemes.append([msep_data1['名称'][i], '分离器', msep_data2['液'][j], msep_data1['液'][i]])

        for i in range(len(wsep_data1['名称'])):
            for j in range(len(wsep_data2['名称'])):
                if wsep_data1['名称'][i] == wsep_data2['名称'][j]:
                    if wsep_data1['处理量'][i] - wsep_data2['处理量'][j] >= 50:
                        adjusted_schemes.append(
                            [wsep_data1['名称'][i], '水处理系统', wsep_data2['处理量'][j], wsep_data1['处理量'][i]]
                            )

        for i in range(len(injpump_data1['名称'])):
            for j in range(len(injpump_data2['名称'])):
                if injpump_data1['名称'][i] == injpump_data2['名称'][j]:
                    if injpump_data1['并联数'][i] - injpump_data2['并联数'][j] != 0:
                        adjusted_schemes.append(
                            [injpump_data1['名称'][i], '注水泵', injpump_data2['并联数'][j], injpump_data1['并联数'][i]]
                        )

        for i in range(len(boostpump_data1['名称'])):
            for j in range(len(boostpump_data1['名称'])):
                if boostpump_data1['名称'][i] == boostpump_data2['名称'][j]:
                    if boostpump_data1['并联数'][i] - boostpump_data2['并联数'][j] != 0:
                        adjusted_schemes.append(
                            [boostpump_data1['名称'][i], '增压泵', boostpump_data2['并联数'][j],
                             boostpump_data1['并联数'][i]]
                        )

        for i in range(len(injwell_data1['名称'])):
            for j in range(len(injwell_data2['名称'])):
                if injwell_data1['名称'][i] == injwell_data2['名称'][j]:
                    if injwell_data1['注水量'][i] - injwell_data2['注水量'][j] >= 50:
                        adjusted_schemes.append(
                            [injwell_data1['名称'][i], '注水井', injwell_data2['注水量'][j], injwell_data1['注水量'][i]]
                            )

        adjusted_array = np.array(adjusted_schemes)
        adjusted_df = pd.DataFrame(adjusted_array, columns=['名称', '类型', '优化前', '优化后'])
        script_dir = os.getcwd()
        dir = script_dir.replace('\\', '/') + "/output/"
        file_name = "调整方案.xlsx"
        file_path = dir + file_name
        with pd.ExcelWriter(file_path) as writer:
            adjusted_df.to_excel(writer, sheet_name='调整方案', index=False, float_format='%.2f')

        num1 = 0  # 操作简便
        num2 = 0  # 人力统筹
        plat = []
        for k in range(len(adjusted_schemes)):
            if adjusted_schemes[k][1] == '注水泵' and adjusted_schemes[k][1] == '增压泵':
                num1 += abs(adjusted_schemes[k][3] - adjusted_schemes[k][3])
                plat.append(adjusted_schemes[k][0].split('_')[0])

        le = len(set(plat))
        num2 = num1/le
        res = ['调整方案.xlsx','操作简便:{}'.format(num1), '人力统筹:{}'.format(num2)]
        return res
    except Exception:
        return ['调整方案.xlsx']
