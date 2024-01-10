import json
import os
import requests
from set import flowsim_url, token, Authorization


def upload_file(file_path):
    url = flowsim_url + "/file/upload"

    # file_path = r'D:\QHD32-6_MProject\模型输入\{}'.format(file_name)
    headers = {
        "Accept": "*/*",
        "myToken": token,
        "Request-Origion": "Knife4j",
    }
    files = {"file": open(file_path, "rb")}
    response = requests.post(url, headers=headers, files=files)

    if response.status_code == 200:
        print("upload_file: 文件上传成功")
        return True
    else:
        print(f"文件上传失败，HTTP状态码: {response.status_code}")
        return False


def delete_file(name: str, token: str):
    url = flowsim_url + "/file/delete"
    params = {"name": name}
    headers = {
        "Accept": "*/*",
        "myToken": token,
        "Request-Origion": "Knife4j",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        print("文件删除成功")
    else:
        print(f"删除文件失败，HTTP 状态码: {response.status_code}")
        print(response.text)


def netsim_steady(file_name):
    url = flowsim_url + "/netsim/steady"
    headers = {
        "Accept": "*/*",
        "myToken": token,
        "Request-Origion": "Knife4j",
        "Content-Type": "application/json"
    }
    data = {"name": file_name}
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        res_str = response.text
        res_dict = json.loads(res_str)
        res_data = res_dict['data']
        res_edge = res_data['edgesResult']
        res_node = res_data['nodesResult']
        res_state = res_data['converged']
        res = [res_edge, res_node]
        residual = res_data['residual']
        count = res_data['count']
        # print('netsim_steady:', res_state)
        # print('count:', count)
        # print('残差：', residual)
        return res
    else:
        print("API call failed with status code:", response.status_code)
        exit()


def netsim_steadyfile(file_name):
    file_path = os.path.join("D:\\QHD32-6_MProject\\模型输入", file_name)
    url = flowsim_url + "/netsim/steadyfile"
    headers = {
        "Accept": "*/*",
        "myToken": token,
        "Request-Origion": "Knife4j",
    }
    files = {"file": (file_name, open(file_path, "rb"), "multipart/form-data")}
    response = requests.post(url, headers=headers, files=files)
    print(response.status_code)
    print(response.text)

    if response.status_code == 200:
        res_str = response.text
        res_dict = json.loads(res_str)
        res_data = res_dict['data']
        res_edge = res_data['edgesResult']
        res_node = res_data['nodesResult']
        res_state = res_data['converged']
        res = [res_edge, res_node]
        print('netsim_steadyfile:', res_state)
        return res
    else:
        print("API call failed with status code:", response.status_code)
        exit()


def getLibEntity(project, type):
    url = flowsim_url + "/pro/getLibEntity"
    headers = {
        "Accept": "*/*",
        "myToken": token,
        "Authorization": "",
        "Request-Origion": "Knife4j",
        "Content-Type": "application/json"
    }
    data = {
        "list": [],
        "name": "",
        "project": project,
        "type": type
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        # print("Request successful")
        # print("getLibEntity-Response:", response.json())
        return response.json()
    else:
        print("Request failed with status code:", response.status_code)


def saveLibEntity(project, modify_data, type):
    url = flowsim_url + "/pro/saveLibEntity"
    headers = {
        "Accept": "*/*",
        "myToken": token,
        "Authorization": "",
        "Request-Origion": "Knife4j",
        "Content-Type": "application/json"
    }
    data = {
        "list": modify_data,
        "name": "",
        "project": project,
        "type": type
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        print("Request failed with status code:", response.status_code)
    return response.json()


def getFileDir():
    url = flowsim_url + "/file/getFileDir"
    headers = {
        "myToken": token,
        "Request-Origion": "Knife4j",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.get(url, headers=headers)
    res = response.json()
    return res['data']

