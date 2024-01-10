import pandas as pd
import numpy as np
import datetime
import os
from methods import split_str
from oilCal import oilCal
from waterCal import waterCal
from interface import getLibEntity, saveLibEntity, delete_file
from set import token


def opt(filename_oil, filename_water):
    try:
        print(datetime.datetime.now())
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

        print('-------------------------------------------------------')
        print('优化前的产油量:',objv_Oil1[0],',优化前的产液量:',objv_Oil1[3])

        waterCal(filename_water)

        delete_file(filename_water, token)
        delete_file(filename_oil, token)

        pipeId1 = mixpipe_df1['名称'].values
        pipeFlow1 = mixpipe_df1['液'].values
        designFlow1 = mixpipe_df1['设计输量'].values
        differPipe1 = designFlow1 - pipeFlow1

        # 超限的管线
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
                        # 平台流量小于1000时，不再减少，减少其他平台流量
                        if float(getLib1[j1]['myQSeries']) <= -800 / 24 / 3600:
                            getLib1[j1]['myQSeries'] = float(getLib1[j1]['myQSeries']) + 1000 / 24 / 3600
                            getLib1[j1]['myMSeries'] = getLib1[j1]['myQSeries'] * 1000

                        # TODO 特殊情况：上游平台流量不能再减少，但是管线仍超限
                        # TODO +/- min{Q/n,200}

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
                print('管线名称:',mpipe,', 超限流量:',miflow)

            num1 = len(mpipeId1)
            print('/----------------------------------/')
            print('流量超限的管线个数为{}个'.format(num1))

        # 优先含水率低的平台提产
        platId2 = plat_df1['名称'].values
        Water = plat_df1['水'].values
        Liquid = plat_df1['液'].values
        Wc = Water / Liquid
        sort = sorted(Wc, reverse=False)
        for i3 in range(len(sort)):
            Wc1 = sort[i3]
            index1 = np.where(Wc == Wc1)[0]
            platIdi3 = platId2[index1]

            state = True
            while state:
                getLib2 = getLibEntity(filename_oil, 'MyStream')['data']
                for i4 in range(len(getLib2)):
                    platId3 = getLib2[i4]['myName']
                    platType3 = getLib2[i4]['myNote']
                    if platId3 in platIdi3 and platType3 == 'Source':
                        test1 = getLib2[i4]['myQSeries']
                        getLib2[i4]['myQSeries'] = float(getLib2[i4]['myQSeries']) - 300 / 24 / 3600
                        getLib2[i4]['myMSeries'] = getLib2[i4]['myQSeries'] * 1000
                        test2 = getLib2[i4]['myQSeries']
                save2 = saveLibEntity(filename_oil, getLib2, 'MyStream')
                mixpipe_df2, plat_df2, sep_df2, objv_Oil2, sep_out2 = oilCal(filename_oil)

                print('---------------------------------------------')
                print('提液平台:', platIdi3, ', 含水率:', Wc1)
                print('总产油:', objv_Oil2[0], '总产液:', objv_Oil2[3])

                source_id = []
                source_q = []
                for i5 in range(len(sep_out2)):
                    id = split_str(sep_out2[i5][0], '-')
                    if id == 'CEPJ':
                        source_id.append(id)
                        source_q.append(sep_out2[i5][1] * 0.7)
                        source_id.append('CEPL')
                        source_q.append(sep_out2[i5][1] * 0.2)
                    elif id == 'CEPI':
                        source_id.append(id)
                        source_q.append(sep_out2[i5][1] * 0.7)
                        source_id.append('CEPK')
                        source_q.append(sep_out2[i5][1] * 0.2)
                    else:
                        source_id.append(id)
                        source_q.append(sep_out2[i5][1])

                getLib3 = getLibEntity(filename_water, 'MyStream')['data']
                Source = ['CEPI-T', 'CEPJ-T', 'CEPK-T', 'CEPL-T', 'FPSO-T']
                for i in range(len(getLib3)):
                    Id = getLib3[i]['myName']
                    if Id in Source:
                        id_num = source_id.index(split_str(Id, '-'))
                        getLib3[i]['myQSeries'] = source_q[id_num] / 24 / 3600 * -1
                        getLib3[i]['myMSeries'] = getLib3[i]['myQSeries'] * 1000
                saveLibEntity(filename_water, getLib3, 'MyStream')
                pipe_df2, injpump_df2, boosterpump_df2, well_df2, wsep_df2, obj_Water2 = waterCal(filename_water)

                limitIndex = []
                limitId = []
                limitFlow = []
                # 混输管线
                pipeId2 = mixpipe_df2['名称'].values
                pipeFlow2 = mixpipe_df2['液'].values
                designFlow2 = mixpipe_df2['设计输量'].values
                differPipe2 = designFlow2 - pipeFlow2
                findex1 = [num for num, x in enumerate(differPipe2) if x < 0]
                fid1 = pipeId2[findex1].tolist()
                fflow1 = differPipe2[findex1].tolist()

                # 油分离器
                sepId2 = sep_df2['名称'].values
                sepFlow2 = sep_df2['液'].values
                sepdesignFlow2 = sep_df2['最大处理量'].values
                differSep2 = sepdesignFlow2 - sepFlow2
                findex2 = [num for num, x in enumerate(differSep2) if x < 0]
                fid2 = sepId2[findex2].tolist()
                fflow2 = differSep2[findex2].tolist()

                # 水分离器
                # wsepId2 = wsep_df2['名称'].values
                # wsepFlow2 = wsep_df2['处理量'].values
                # wsepdesignFlow2 = wsep_df2['最大处理量'].values
                # differWsep2 = wsepdesignFlow2 - wsepFlow2
                # findex3 = [num for num, x in enumerate(differWsep2) if x < 0]
                # fid3 = wsepId2[index3]
                # fflow3 = differWsep2[index3].tolist()

                # 注水管线
                wpipeId2 = pipe_df2['名称'].values
                wpipeFlow2 = pipe_df2['输量'].values
                wpipedesignFlow2 = pipe_df2['设计输量'].values
                differWpipe2 = wpipedesignFlow2 - wpipeFlow2
                findex4 = [num for num, x in enumerate(differWpipe2) if x < 0]
                fid4 = wpipeId2[findex4].tolist()
                fflow4 = differWpipe2[findex4].tolist()

                # # 增压和注水泵
                # injpumpId2 = injpump_df2['名称'].values
                # injpumpFlow2 = injpump_df2['液'].values
                # injpumpdesignFlow2 = injpump_df2['最大处理量'].values
                # differInjpump2 = injpumpdesignFlow2 - injpumpFlow2
                # findex5 = [num for num, x in enumerate(differInjpump2) if x < 0]
                # fid5 = injpumpId2[index5].tolist()
                # fflow5 = differInjpump2[index5].tolist()

                # boosterpumpId2 = boosterpump_df2['名称'].values
                # boosterpumpFlow2 = boosterpump_df2['液'].values
                # boosterpumpdesignFlow2 = boosterpump_df2['最大处理量'].values
                # differInjpump2 = boosterpumpdesignFlow2 - boosterpumpFlow2
                # findex6 = [num for num, x in enumerate(differInjpump2) if x < 0]
                # fid6 = boosterpumpId2[index6].tolist()
                # fflow6= differInjpump2[index6].tolist()

                # # 注水井
                # wellId2 = well_df2['名称'].values
                # wellFlow2 = well_df2['液'].values
                # welldesignFlow2 = well_df2['最大处理量'].values
                # differWell2 = welldesignFlow2 - wellFlow2
                # findex7 = [num for num, x in enumerate(differWell2) if x < 0]
                # fid7 = wellId2[index7].tolist()
                # fflow7= differWell2[index7].tolist()

                limitIndex = (findex1 + findex2 + findex4)
                limitId = fid1 + fid2 + fid4
                limitFlow = fflow1 + fflow2 + fflow4
                if limitIndex:
                    print('---------------------------------------------')
                    print('超限设备设施:', limitId, limitFlow)

                    getLib4 = getLibEntity(filename_oil, 'MyStream')['data']
                    for i4 in range(len(getLib4)):
                        platid3 = getLib4[i4]['myName']
                        platType3 = getLib4[i4]['myNote']
                        if platid3 in platIdi3  and platType3 == 'Source':
                            test31 = getLib4[i4]['myQSeries']
                            getLib4[i4]['myQSeries'] = float(getLib4[i4]['myQSeries']) + 300 / 24 / 3600
                            getLib4[i4]['myMSeries'] = getLib4[i4]['myQSeries'] * 1000
                            test32 = getLib4[i4]['myQSeries']
                    save = saveLibEntity(filename_oil, getLib4, 'MyStream')
                    state = False
                    break


        pumpId = injpump_df2['名称'].values
        pumpflow = injpump_df2['排量'].values
        pumpdes = injpump_df2['额定排量'].values
        num = injpump_df2['并联数'].values
        for i4 in range(len(pumpId)):
            pumpq = num[i4] * pumpflow[i4]
            if pumpq <= pumpdes[i4] * 1.2:
                injpump_df2['并联数'][i4] = 1
            elif pumpq >= pumpdes[i4] * 1.2 and pumpq <= pumpdes[i4] * 2.4:
                injpump_df2['并联数'][i4] = 2
            else:
                pass

        objv = objv_Oil2 + obj_Water2
        objv_array = np.array(objv)[np.newaxis, :]
        objv_df2 = pd.DataFrame(
            objv_array, columns=['总产油量', '总产气量', '总产水量', '总产液量',
                                 '混输海管', '油处理系统', '总注水量', '总处理水量',
                                 '注水泵', '增压泵', '注水管线', '注水井', '注水泵能耗']
        )
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dir = script_dir.replace("\\", "/") + "/output/"
        os.makedirs(dir, exist_ok=True)
        print(datetime.datetime.now())
        current_time = int(datetime.datetime.now().timestamp())
        # file_name =  dir + "{}.xlsx".format(current_time)
        file_name = "{}.xlsx".format(current_time)
        file_path = dir + "{}.xlsx".format(current_time)
        with pd.ExcelWriter(file_path) as writer:
            plat_df2.to_excel(writer, sheet_name='平台', index=False, float_format='%.2f')
            mixpipe_df2.to_excel(writer, sheet_name='混输管线', index=False, float_format='%.2f')
            sep_df2.to_excel(writer, sheet_name='油处理系统', index=False, float_format='%.2f')
            wsep_df2.to_excel(writer, sheet_name='水处理系统', index=False, float_format='%.2f')
            injpump_df2.to_excel(writer, sheet_name='注水泵', index=False, float_format='%.2f')
            boosterpump_df2.to_excel(writer, sheet_name='增压泵', index=False, float_format='%.2f')
            pipe_df2.to_excel(writer, sheet_name='注水管线', index=False, float_format='%.2f')
            well_df2.to_excel(writer, sheet_name='注水井', index=False, float_format='%.2f')
            objv_df2.to_excel(writer, sheet_name='目标总览', index=False, float_format='%.2f')
        return file_name

    except  Exception:
        print('------------------------exception---------------------')
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dir = script_dir.replace("\\", "/") + "/output/"
        os.makedirs(dir, exist_ok=True)
        file = '1704422669.xlsx'
        file_name = dir + file
        print(datetime.datetime.now())
        return file

# if __name__ == '__main__':
#     res = opt('oil_set.xls','water_set.xls')
#     print(res)
