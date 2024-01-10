import uvicorn
import os
from fastapi import FastAPI, Depends, HTTPException, File, UploadFile
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from processsim import processSim
from interface import upload_file, getFileDir, netsim_steady
from typing import List
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from adjust import adjust
from methods import uploadOptData
import datetime
from maxPro import opt

app = FastAPI(
    title="秦皇岛32-6油水调控优化计算模型",
    version="2.1.0",
    description="优化模型接口",
    docs_url=None,
    redoc_url=None
)

security = HTTPBasic()

# 跨域
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 登录
@app.get(path="/docs", include_in_schema=False)
async def custom_swagger_ui_html(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != "admin" or credentials.password != "opt@cupb2024":
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password !",
            headers={"WWW-Authenticate": "Basic"}
        )
    else:
        return get_swagger_ui_html(
            openapi_url='/openapi.json',
            title="docs",
            swagger_js_url="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui-bundle.js",
            swagger_css_url="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui.css"
        )


@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_js_url="https://unpkg.com/redoc@next/bundles/redoc.standalone.js",
    )


# 上传文件
@app.post("/upload")
async def upload_files(files: List[UploadFile] = File(...)):
    for file in files:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        filePath = script_dir.replace("\\", "/") + '/output/'
        os.makedirs(filePath, exist_ok=True)
        file_path = filePath + file.filename
        with open(file_path, "wb") as file_object:
            file_object.write(file.file.read())

        res = upload_file(file_path)

    return res


@app.get("/uploadfiles_name")
async def get_uploadfiles_name():
    res = getFileDir()
    return res


@app.post("/sim")
async def simulation(data: dict):
    result_file_path = processSim(data['filename_oil'], data['filename_water'])
    return result_file_path


@app.post("/opt")
async def optimize(data: dict):
    result_file_path = opt(data['filename_oil'], data['filename_water'])
    return result_file_path


@app.get("/adj")
async def Adj(file1, file2):
    res = adjust(file1, file2)
    return res


@app.get("/outputfiles_name")
async def get_outputfiles_name():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filePath = script_dir.replace("\\", "/") + '/output/'
    files = []
    for filename in os.listdir(filePath):
        if os.path.isfile(os.path.join(filePath, filename)):
            files.append(filename)
    return files


@app.get("/download")
async def download_file(filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filePath = script_dir.replace("\\", "/") + '/output/' + filename
    return FileResponse(filePath, filename=filename)


@app.get('/del')
async def delete(filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filePath = script_dir.replace("\\", "/") + '/output/' + filename
    os.remove(filePath)
    return True


@app.get('/delAll')
async def deleteAll():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filePath = script_dir.replace("\\", "/") + '/output/'
    for filename in os.listdir(filePath):
        if os.path.isfile(os.path.join(filePath, filename)):
            os.remove(filePath + filename)
    return True


@app.post('/uploadOptdata')
async def uploadopt(data: dict):
    res = uploadOptData(data)
    return res


if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    starttime = datetime.datetime.now()
    print(starttime)
    uvicorn.run(app, host="0.0.0.0", port=9471)
