:: 执行方式 ./gitee-maven-repo.bat

chcp 65001
@ECHO off&title gitee-maven-repo
@setlocal enableextensions
setlocal enabledelayedexpansion
@cd /d "%~dp0"

::远程 gitee,github,gitlab
set REMOTE_URL=gitee
::远程存储库 snapshots,releases
set REMOTE_REPO=snapshots

::本地仓库
set LOCAL_REPO=D:\project\%REMOTE_URL%\public\maven-repo\%REMOTE_REPO%

if not exist %LOCAL_REPO% (
	goto NotExist
)

::项目根目录
cd %~dp0
echo 准备deploy,将项目发布到本地存储库%LOCAL_REPO%
::call mvn clean -U package deploy -Dmaven.test.skip -DaltDeploymentRepository=maven-repo-public-snapshots::default::file:%LOCAL_REPO%
if "snapshots" equ "%REMOTE_REPO%" (
    goto SNAPSHOTS
)

if "releases" equ "%REMOTE_REPO%" (
    goto RELEASES
)

:RELEASES
call mvn deploy -DaltDeploymentRepository=maven-repo-public-releases::default::file:%LOCAL_REPO%

goto LOCAL_REPO

:SNAPSHOTS
call mvn deploy -DaltDeploymentRepository=maven-repo-public-snapshots::default::file:%LOCAL_REPO%

:LOCAL_REPO

cd %LOCAL_REPO%
echo 将本地存储库发布到远程仓库上
git add .
git commit -m "update: 更新依赖"
git push

goto SUCCESS

:NotExist
echo not exist 本地仓库  %LOCAL_REPO%

:SUCCESS