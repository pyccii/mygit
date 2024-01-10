import pandas as pd

from interface import netsim_steady, getLibEntity
from methods import split_str, hstack


def waterCal(file_name):
    # 管网稳态模拟
    res_edge, res_node = netsim_steady(file_name)

    # 获取实体数据
    edge_data = getLibEntity(file_name, 'MyEdge')['data']
    node_data = getLibEntity(file_name, 'MyNode')['data']
    pump_data = getLibEntity(file_name, 'MyPump')['data']
    opt_data = getLibEntity(file_name, 'MyOpt')['data']
    pumpcurve = getLibEntity(file_name, 'MyPumpCurve')['data']

    # 所需数据列表
    pipe_id = []
    pipe_water = []
    pipe_dT = []
    pipe_dP = []
    pipe_Ps = []
    pipe_Pe = []
    pipe_design = []

    injpump_id = []
    injpump_flow = []
    injpump_dT = []
    injpump_dP = []
    injpump_design = []
    injpump_num = []

    boosterpump_id = []
    boosterpump_flow = []
    boosterpump_dT = []
    boosterpump_dP = []
    boosterpump_design = []
    boosterpump_num = []

    well_id = []
    well_flow = []
    well_T = []
    well_P = []

    wsep_id = []
    wsep_water = []
    wsep_design = []
    wsep_P = []
    wsep_T = []

    # 遍历所有res_edge， 取值
    for i in range(len(res_edge)):
        i_id = res_edge[i]['myName']
        for j in range(len(edge_data)):
            if edge_data[j]['myName'] == i_id:
                i_type = edge_data[j]['myVType']
                i_entity = edge_data[j]['myEName']
                i_Source = edge_data[j]['mySource']
                i_Target = edge_data[j]['myTarget']
                i_Note =  edge_data[j]['myNote']
                break

        for k in range(len(opt_data)):
            if opt_data[k]['myName'] == i_id:
                i_design = opt_data[k]['myValue']
                break

        if i_type == 'Link':
            if i_Note == 'Pipe':
                # 注水管线名称
                i_name = split_str(i_Source, '-') + '-' + split_str(i_Target, '-')

                # 注水管线流量
                q_pipe_water = res_edge[i]['myProfileFwStdvol'][-1]
                Q_pipe_water = q_pipe_water * 24 * 3600

                # 注水管线温降
                T = res_edge[i]['myProfileT']
                dT = T[0] - T[len(T) - 1]

                # 注水管线压降
                P = res_edge[i]['myProfileP']
                dP = P[0] - P[len(P) - 1]
                pipe_Ps.append(P[0])
                pipe_Pe.append(P[len(P) - 1])

                pipe_id.append(i_name)
                pipe_water.append(Q_pipe_water)
                pipe_dT.append(dT)
                pipe_dP.append(dP)
                pipe_design.append(i_design)

            elif i_Note == 'Pump':
                for h in range(len(pump_data)):
                    if pump_data[h]['myName'] == i_id:
                        i_pump_num = pump_data[h]['myParallel']
                        i_pimp_curve = pump_data[h]['myCurveId']
                        break

                # 泵排量
                q_pump_water = res_edge[i]['myProfileFwStdvol'][0]
                Q_pump_water = q_pump_water * 3600 / (i_pump_num + 1e-6)

                # 泵温降
                T = res_edge[i]['myProfileT']
                dT = T[0] - T[len(T) - 1]

                # 注水泵压降
                P = res_edge[i]['myProfileP']
                dP = P[len(P) - 1] - P[0]

                # 泵类型
                for n in range(len(pumpcurve)):
                    if pumpcurve[n]['myName'] == i_pimp_curve:
                        i_pimp_type = pumpcurve[n]['myNote']

                if i_pimp_type == '增压泵':
                    boosterpump_id.append(i_id)
                    boosterpump_flow.append(Q_pump_water)
                    boosterpump_dT.append(dT)
                    boosterpump_dP.append(dP)
                    boosterpump_design.append(i_design)
                    boosterpump_num.append(i_pump_num)
                else:
                    injpump_id.append(i_id)
                    injpump_flow.append(Q_pump_water)
                    injpump_dT.append(dT)
                    injpump_dP.append(dP)
                    injpump_design.append(i_design)
                    injpump_num.append(i_pump_num)

    for j in range(len(res_node)):
        j_id = res_node[j]['myName']
        for k in range(len(node_data)):
            if node_data[k]['myName'] == j_id:
                j_type = node_data[k]['myVType']
                break

        for n in range(len(opt_data)):
            if opt_data[n]['myName'] == j_id:
                j_design = opt_data[n]['myValue']
                break

        # 流量
        q_water = res_node[j]['myFwStdvol']
        Q_water = q_water * 24 * 3600

        # 压力
        P = res_node[j]['myP']

        # 温度
        T = res_node[j]['myT'] - 273.15

        if j_type == 'Source':
            wsep_id.append(j_id)
            wsep_water.append(Q_water)
            wsep_design.append(j_design)
            wsep_P.append(P)
            wsep_T.append(T)

        elif j_type == 'Sink':
            well_id.append(j_id)
            well_flow.append(Q_water)
            well_P.append(P)
            well_T.append(T)

    # 注水管线数据
    pipe_array = hstack(pipe_id, pipe_water, pipe_design, pipe_dT, pipe_dP)
    pipe_df = pd.DataFrame(pipe_array, columns=['名称', '输量', '设计输量', '温降', '压降'])

    # 注水泵数据
    injpump_array = hstack(injpump_id, injpump_flow, injpump_design, injpump_dP, injpump_num)
    injpump_df = pd.DataFrame(injpump_array, columns=['名称', '排量', '额定排量', '扬程', '并联数'])

    # 增压泵数据
    boosterpump_array = hstack(boosterpump_id, boosterpump_flow, boosterpump_design, boosterpump_dP, boosterpump_num)
    boosterpump_df = pd.DataFrame(boosterpump_array, columns=['名称', '排量', '额定排量', '扬程', '并联数'])

    # 注水井数据
    well_array = hstack(well_id, well_flow, well_T, well_P)
    well_df = pd.DataFrame(well_array, columns=['名称', '注水量', '温度', '压力'])

    # 水处理数据
    wsep_array = hstack(wsep_id, wsep_water, wsep_design, wsep_T, wsep_P)
    wsep_df = pd.DataFrame(wsep_array, columns=['名称', '处理量', '最大处理量', '温度', '压力'])
    # wsep_array = hstack(wsep_id, wsep_water, wsep_T, wsep_P)
    # wsep_df = pd.DataFrame(wsep_array, columns=['名称', '处理量',  '温度', '压力'])

    # 总注水量、总处理水量、总泵流量
    sum_well_flow = sum(well_flow)
    sum_wsep_water = sum(wsep_water)
    sum_injpump_flow = sum(injpump_flow[i] * injpump_num[i] for i in range(len(injpump_flow)))
    sum_boosterpump_flow = sum(boosterpump_flow[i] * boosterpump_num[i] for i in range(len(boosterpump_flow)))
    sum_pipe_water = sum(pipe_water)

    # 注水泵能耗,KW
    power = 0
    for k in range(len(injpump_id)):
        power += injpump_flow[k] * injpump_dP[k] * injpump_num[k] / 3600 / 1000

    obj_Water = [sum_well_flow, sum_wsep_water,
                 sum_injpump_flow, sum_boosterpump_flow,
                 sum_pipe_water, sum_well_flow, power]

    res_water = [pipe_df, injpump_df, boosterpump_df, well_df, wsep_df, obj_Water]
    return res_water
