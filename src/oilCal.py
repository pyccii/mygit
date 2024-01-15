import json

import pandas as pd

from interface import netsim_steady, getLibEntity
from methods import split_str, hstack


def oilCal(file_name):
    # 管网稳态模拟
    res_edge, res_node = netsim_steady(file_name)

    # 获取实体数据
    edge_data = getLibEntity(file_name, 'MyEdge')['data']
    node_data = getLibEntity(file_name, 'MyNode')['data']
    opt_data = getLibEntity(file_name, 'MyOpt')['data']

    # 所需数据列表
    mixpipe_id = []
    mixpipe_oil = []
    mixpipe_gas = []
    mixpipe_water = []
    mixpipe_liquid = []
    mixpipe_dT = []
    mixpipe_dP = []
    mixpipe_design = []
    mixpipe_Ps = []
    mixpipe_Pe = []
    mixpipe_watercut = []

    plat_id = []
    plat_oil = []
    plat_gas = []
    plat_water = []
    plat_liquid = []
    plat_P = []
    plat_T = []
    plat_pro = []
    plat_oil_mass = []
    plat_gas_mass = []
    plat_water_mass = []
    plat_liquid_mass = []

    msep_id = []
    msep_oil = []
    msep_gas = []
    msep_water = []
    msep_liquid = []
    msep_oil_mass = []
    msep_gas_mass = []
    msep_water_mass = []
    msep_liquid_mass = []
    msep_design = []
    msep_P = []
    msep_T = []

    # 遍历所有res_edge， 取值
    for i in range(len(res_edge)):
        i_id = res_edge[i]['myName']
        for j in range(len(edge_data)):
            if edge_data[j]['myName'] == i_id:
                i_type = edge_data[j]['myVType']
                i_entity = edge_data[j]['myEName']
                i_Source = edge_data[j]['mySource']
                i_Target = edge_data[j]['myTarget']
                i_Note= edge_data[j]['myNote']
                break

        for k in range(len(opt_data)):
            if opt_data[k]['myName'] == i_id:
                i_design = opt_data[k]['myValue']
                break

        if i_Note == 'Pipe':
            i_name = i_name = split_str(i_Source, '-') + '-' + split_str(i_Target, '-')

            q_mixpipe_oil = res_edge[i]['myProfileFoStdvol'][-1]
            Q_mixpipe_oil = q_mixpipe_oil * 24 * 3600

            q_mixpipe_gas = res_edge[i]['myProfileFgStdvol'][-1]
            Q_mixpipe_gas = q_mixpipe_gas * 24 * 3600

            q_mixpipe_water = res_edge[i]['myProfileFwStdvol'][-1]
            Q_mixpipe_water = q_mixpipe_water * 24 * 3600

            Q_mixpipe_liquid = Q_mixpipe_oil + Q_mixpipe_water
            if Q_mixpipe_liquid != 0:
                watercut = Q_mixpipe_water / Q_mixpipe_liquid
            else:
                watercut = '——'

            T = res_edge[i]['myProfileT']
            dT = T[0] - T[len(T) - 1]

            P = res_edge[i]['myProfileP']
            dP = (P[0] - P[len(P) - 1])

            mixpipe_Pe.append(P[len(P) - 1])  # 终点压力,Pa
            mixpipe_Ps.append(P[0])  # 起点压力,Pa
            mixpipe_id.append(i_name)  # 边名称
            mixpipe_oil.append(Q_mixpipe_oil)  # 油相流量,m³/s
            mixpipe_gas.append(Q_mixpipe_gas)  # 气相流量,m³/s
            mixpipe_water.append(Q_mixpipe_water)  # 水相流量,m³/s
            mixpipe_liquid.append(Q_mixpipe_liquid)  # 液相流量,m³/s
            mixpipe_dT.append(dT)  # 温降,℃
            mixpipe_dP.append(dP)  # 压降，Pa
            mixpipe_design.append(i_design)
            mixpipe_watercut.append(watercut)

    # 遍历节点，取参
    sep_exit = ["CEPI-V2001-W", "CEPJ-V2001-W", "FPSO-V2001"]  # 分离器出口节点
    list = []
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

        q_oil = res_node[j]['myFoStdvol']
        q_oil_mass = res_node[j]['myFoMass']
        Q_oil = q_oil * 24 * 3600

        q_gas = res_node[j]['myFgStdvol']
        q_gas_mass = res_node[j]['myFgMass']
        Q_gas = q_gas * 24 * 3600

        q_water = res_node[j]['myFwStdvol']
        q_water_mass = res_node[j]['myFwMass']
        Q_water = q_water * 24 * 3600
        Q_liquid = Q_oil + Q_water
        q_liquid_mass = q_oil_mass + q_water_mass

        P = res_node[j]['myP']
        T = res_node[j]['myT'] - 273.15
        if j_id in sep_exit:
            list.append([j_id, Q_water])

        if j_type == 'Source':  # 平台
            # des = j_design * -24 * 3600
            plat_id.append(j_id)  # 节点名称
            plat_oil.append(Q_oil)  # 油相流量,m³/s
            plat_gas.append(Q_gas)  # 气相流量,m³/s
            plat_water.append(Q_water)  # 水相流量,m³/s
            plat_liquid.append(Q_liquid)  # 液相流量,m³/s
            plat_P.append(P)  # 压力,Pa
            plat_T.append(T)  # 温度,℃
            # plat_pro.append(des)
            plat_oil_mass.append(q_oil_mass)
            plat_gas_mass.append(q_gas_mass)
            plat_water_mass.append(q_water_mass)
            plat_liquid_mass.append(q_liquid_mass)

        elif j_type == 'Sep3':  # 分离器
            msep_id.append(j_id)
            msep_oil.append(Q_oil)
            msep_gas.append(Q_gas)
            msep_water.append(Q_water)
            msep_liquid.append(Q_liquid)
            msep_P.append(P)
            msep_T.append(T)
            msep_design.append(j_design)
            msep_oil_mass.append(q_oil_mass)
            msep_gas_mass.append(q_gas_mass)
            msep_water_mass.append(q_water_mass)
            msep_liquid_mass.append(q_liquid_mass)

        elif j_type == 'Junction' and j_id == "FPSO-V2001":  # 分离器
            msep_id.append(j_id)
            msep_oil.append(Q_oil)
            msep_gas.append(Q_gas)
            msep_water.append(Q_water)
            msep_liquid.append(Q_liquid)
            msep_P.append(P)
            msep_T.append(T)
            msep_design.append(j_design)
            msep_oil_mass.append(q_oil_mass)
            msep_gas_mass.append(q_gas_mass)
            msep_water_mass.append(q_water_mass)
            msep_liquid_mass.append(q_liquid_mass)

    # 混输管线数据
    mixpipe_array = hstack(mixpipe_id, mixpipe_oil, mixpipe_gas, mixpipe_water,
                           mixpipe_liquid, mixpipe_design, mixpipe_dT, mixpipe_dP,
                           mixpipe_watercut)
    mixpipe_df = pd.DataFrame(mixpipe_array,
                              columns=['名称', '油', '气', '水', '液', '设计输量', '温降', '压降',
                                       '含水率'])

    # 平台数据
    plat_array = hstack(plat_id, plat_oil, plat_gas, plat_water, plat_liquid, plat_P, plat_T)
    plat_df = pd.DataFrame(plat_array,
                           columns=['名称', '油', '气', '水', '液', '压力', '温度'])

    # 生产分离器数据
    msep_array = hstack(msep_id, msep_oil, msep_gas, msep_water, msep_liquid, msep_design, msep_P, msep_T)
    sep_df = pd.DataFrame(msep_array,
                          columns=['名称', '油', '气', '水', '液', '最大处理量', '压力', '温度'])

    # 总产油、总产气、总产水、总产液、世纪号产油量
    sum_plat_oil = sum(plat_oil)
    sum_plat_gas = sum(plat_gas)
    sum_plat_water = sum(plat_water)
    sum_plat_liquid = sum(plat_liquid)
    sum_mixpipe_liquid = sum(mixpipe_liquid)
    sum_msep_liquid = sum(msep_liquid)

    oilMedic = 0
    for i in range(len(msep_id)):
        msepplat1 = msep_id[i].split('-')[0]
        msepLiq = msep_liquid[i]
        for j in range(len(opt_data)):
            msepplat2 = opt_data[j]['myName'].split('-')[0]
            price = opt_data[j]['myNote']
            if msepplat2 == msepplat1 and price != '0':
                a = json.loads(price)
                b = json.loads(opt_data[j]['myValue'])
                oilMedic += msepLiq * a * b

    objv_Oil = [sum_plat_oil, sum_plat_gas, sum_plat_water, sum_plat_liquid, sum_mixpipe_liquid, sum_msep_liquid,
                oilMedic]
    res_oil = [mixpipe_df, plat_df, sep_df, objv_Oil, list]

    return res_oil
