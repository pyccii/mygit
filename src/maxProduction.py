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
        script_dir = os.path.dirname(os.path.abspath(__file__))
        filePath1 = script_dir.replace("\\", "/") + '/output/' + filename_oil
        filePath2 = script_dir.replace("\\", "/") + '/output/' + filename_water
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

        mixpipe_df, plat_df, sep_df, objv_Oil, sep_out = oilCal(filename_oil)

        waterCal(filename_water)

        delete_file(filename_water, token)
        delete_file(filename_oil, token)

        pipeId = mixpipe_df['名称'].values
        pipeflow = mixpipe_df['液'].values
        designflow = mixpipe_df['设计输量'].values
        differpipe = designflow - pipeflow

        # 超限的管线
        pipeFlaseIndex = [i for i, x in enumerate(differpipe) if x < 0]
        mpipeId = pipeId[pipeFlaseIndex]
        mflow = differpipe[pipeFlaseIndex]

        num = len(mpipeId)
        while num > 0:
            mpipe = mpipeId[0]
            mplat = pipeUpstreamPlat[mpipe]

            number = np.where(mpipeId == mpipe)[0][0]
            miflow = mflow[number]

            while miflow < 0:
                getLib = getLibEntity(filename_oil, 'MyStream')['data']
                for j in range(len(getLib)):
                    platid = getLib[j]['myName']
                    platType = getLib[j]['myNote']
                    if platid in mplat and platType == 'Source':
                        # 平台流量小于1000时，不再减少，减少其他平台流量
                        if float(getLib[j]['myQSeries']) <= -800 / 24 / 3600:
                            getLib[j]['myQSeries'] = float(getLib[j]['myQSeries']) + 500 / 24 / 3600
                            getLib[j]['myMSeries'] = getLib[j]['myQSeries'] * 1000

                        # TODO 特殊情况：上游平台流量不能再减少，但是管线仍超限
                        # TODO +/- min{Q/n,200}

                save = saveLibEntity(filename_oil, getLib, 'MyStream')
                mixpipe_df, plat_df, sep_df, objv_Oil, sep_out = oilCal(filename_oil)

                pipeId = mixpipe_df['名称'].values
                pipeflow = mixpipe_df['液'].values
                designflow = mixpipe_df['设计输量'].values

                differpipe = designflow - pipeflow
                pipeFlaseIndex = [num for num, x in enumerate(differpipe) if x < 0]
                mpipeId = pipeId[pipeFlaseIndex]
                miflow = differpipe[np.where(pipeId == mpipe)[0][0]]
                print(mpipe)
                print(miflow)

            num = len(mpipeId)
            print('/////////////////////////////////////////////')
            print('流量超限的管线个数为{}个'.format(num))

        # 管线流量均不超限,增加上游平台流量保证流量不超限
        # TODO 增加上游平台流量，优先寻找余量大的管线上有平台
        epipeId = mixpipe_df['名称'].values
        epipeflow = mixpipe_df['液'].values
        edesignflow = mixpipe_df['设计输量'].values
        edifferpipe = edesignflow - epipeflow
        excess = sorted(edifferpipe, reverse=True)
        for exc in excess:
            exci = np.where(exc == edifferpipe)[0][0]
            excpipeid = epipeId[exci]
            excplat = pipeUpstreamPlat[excpipeid]
            num2 = 1

            '////////////////////////////////////////////////////////////////////////////////////////////////////'
            while exc > 0:
                egetLib = getLibEntity(filename_oil, 'MyStream')['data']
                for k in range(len(egetLib)):
                    eplatid = egetLib[k]['myName']
                    eplatType = egetLib[k]['myNote']
                    if eplatid in excplat and eplatType == 'Source':
                        test1 = egetLib[k]['myQSeries']
                        egetLib[k]['myQSeries'] = float(egetLib[k]['myQSeries']) - 500 / 24 / 3600
                        egetLib[k]['myMSeries'] = egetLib[k]['myQSeries'] * 1000
                        test2 = egetLib[k]['myQSeries']
                save = saveLibEntity(filename_oil, egetLib, 'MyStream')
                mixpipe_df, plat_df, sep_df, objv_Oil, sep_out = oilCal(filename_oil)

                pipeId1 = mixpipe_df['名称'].values
                pipeflow1 = mixpipe_df['液'].values
                designflow1 = mixpipe_df['设计输量'].values
                differpipe1 = designflow1 - pipeflow1
                num3 = np.where(pipeId1 == excpipeid)[0][0]
                exc = differpipe1[num3]
                print('-------', exc, '-------')

                source_id = []
                source_q = []
                for i in range(len(sep_out)):
                    id = split_str(sep_out[i][0], '-')
                    if id == 'CEPJ':
                        source_id.append(id)
                        source_q.append(sep_out[i][1] * 0.75)
                        source_id.append('CEPL')
                        source_q.append(sep_out[i][1] * 0.25)
                    elif id == 'CEPI':
                        source_id.append(id)
                        source_q.append(sep_out[i][1] * 0.75)
                        source_id.append('CEPK')
                        source_q.append(sep_out[i][1] * 0.25)
                    else:
                        source_id.append(id)
                        source_q.append(sep_out[i][1])

                '/////////////////////////////////////////////////////////////////////////////////////////////////'
                getLib2 = getLibEntity(filename_water, 'MyStream')['data']
                Source = ['CEPI-T', 'CEPJ-T', 'CEPK-T', 'CEPL-T', 'FPSO-T']
                for i in range(len(getLib2)):
                    Id = getLib2[i]['myName']
                    if Id in Source:
                        id_num = source_id.index(split_str(Id, '-'))
                        getLib2[i]['myQSeries'] = source_q[id_num] / 24 / 3600 * -1
                        getLib2[i]['myMSeries'] = getLib2[i]['myQSeries'] * 1000
                saveLibEntity(filename_water, getLib2, 'MyStream')
                pipe_df, injpump_df, boosterpump_df, well_df, wsep_df, obj_Water = waterCal(filename_water)

                # TODO 校核流量 混输管线、油分离器、水分离器、注水管线、增压和注水泵、注水井
                limitIndex = []
                limitId = []
                limitFlow = []
                # 混输管线
                pipeId = mixpipe_df['名称'].values
                pipeflow = mixpipe_df['液'].values
                designflow = mixpipe_df['设计输量'].values
                differpipe = designflow - pipeflow
                index1 = [num for num, x in enumerate(differpipe) if x < 0]
                id1 = pipeId[index1].tolist()
                flow1 = differpipe[index1].tolist()

                # 油分离器
                sepId = sep_df['名称'].values
                sepflow = sep_df['液'].values
                sepdesignflow = sep_df['最大处理量'].values
                differsep = sepdesignflow - sepflow
                index2 = [num for num, x in enumerate(differsep) if x < 0]
                id2 = sepId[index2].tolist()
                flow2 = differsep[index2].tolist()

                # 水分离器
                # wsepId = wsep_df['名称'].values
                # wsepflow = wsep_df['处理量'].values
                # wsepdesignflow = wsep_df['最大处理量'].values
                # differwsep = wsepdesignflow - wsepflow
                # index3 = [num for num, x in enumerate(differwsep) if x < 0]
                # id3 = wsepId[index3]
                # flow3 = differwsep[index3].tolist()

                # 注水管线
                pipeId = pipe_df['名称'].values
                pipeflow = pipe_df['输量'].values
                pipedesignflow = pipe_df['设计输量'].values
                differwpipe = pipedesignflow - pipeflow
                index4 = [num for num, x in enumerate(differwpipe) if x < 0]
                id4 = pipeId[index4].tolist()
                flow4 = differwpipe[index4].tolist()

                # # 增压和注水泵
                # injpumpId = injpump_df['名称'].values
                # injpumpflow = injpump_df['液'].values
                # injpumpdesignflow = injpump_df['最大处理量'].values
                # differinjpump = injpumpdesignflow - injpumpflow
                # index5 = [num for num, x in enumerate(differinjpump) if x < 0]
                # id4 = injpumpId[index5].tolist()
                # flow4 = differinjpump[index5].tolist()

                # boosterpumpId = boosterpump_df['名称'].values
                # boosterpumpflow = boosterpump_df['液'].values
                # boosterpumpdesignflow = boosterpump_df['最大处理量'].values
                # differinjpump = boosterpumpdesignflow - boosterpumpflow
                # index6 = [num for num, x in enumerate(differinjpump) if x < 0]
                # id6 = boosterpumpId[index6].tolist()
                # flow6= differinjpump[index6].tolist()

                # # 注水井
                # wellId = well_df['名称'].values
                # wellflow = well_df['液'].values
                # welldesignflow = well_df['最大处理量'].values
                # differwell = welldesignflow - wellflow
                # index7 = [num for num, x in enumerate(differwell) if x < 0]
                # id7 = wellId[index7].tolist()
                # flow7= differwell[index7].tolist()

                limitIndex = (index1 + index2 + index4)
                limitId = id1 + id2 + id4
                limitFlow = flow1 + flow2 + flow4
                if limitIndex:
                    print('超限设备设施:', limitId)
                    getLib3 = getLibEntity(filename_oil, 'MyStream')['data']
                    for k in range(len(getLib3)):
                        platid3 = getLib3[k]['myName']
                        platType3 = getLib3[k]['myNote']
                        if platid3 in excplat and platType3 == 'Source':
                            test31 = getLib3[k]['myQSeries']
                            getLib3[k]['myQSeries'] = float(getLib3[k]['myQSeries']) + 500 / 24 / 3600
                            getLib3[k]['myMSeries'] = getLib3[k]['myQSeries'] * 1000
                            test32 = getLib3[k]['myQSeries']
                    save = saveLibEntity(filename_oil, getLib3, 'MyStream')
                    break

        pumpId = injpump_df['名称'].values
        pumpflow = injpump_df['排量'].values
        pumpdes = injpump_df['额定排量'].values
        num = injpump_df['并联数'].values
        for k in range(len(pumpId)):
            pumpq = num[k] * pumpflow[k]
            if pumpq <= pumpdes[k] * 1.2:
                injpump_df['并联数'][k] = 1
            elif pumpq >= pumpdes[k] * 1.2 and pumpq <= pumpdes[k] * 2.4:
                injpump_df['并联数'][k] = 2
            else:
                pass

        objv = objv_Oil + obj_Water
        objv_array = np.array(objv)[np.newaxis, :]
        objv_df = pd.DataFrame(
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
        file_path =  dir + "{}.xlsx".format(current_time)
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
        return file_name

    except  Exception:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dir = script_dir.replace("\\", "/") + "/output/"
        os.makedirs(dir, exist_ok=True)
        file = '1704422669.xlsx'
        file_name = dir + file
        print(datetime.datetime.now())
        return file
