:: 执行方式 ./mvn-deploy.bat

chcp 65001
@ECHO off&title mvn-deploy
@setlocal enableextensions
setlocal enabledelayedexpansion
@cd /d "%~dp0"

::远程仓库，项目存储库
set REMOTE_URL=ssh://git@elbgit-1200450932.cn-northwest-1.elb.amazonaws.com.cn:5337/zhengja/maven-repo-bj.git
::set REMOTE_URL=git@github.com:zhengjiaao/maven-repo.git
::部署分支 snapshots/releases
set DEPLOY_BRANCH=snapshots
::本地临时仓库
set LOCAL_REPO=%~d0\maven-repo

if exist %LOCAL_REPO% (
	rd /s /q %LOCAL_REPO%
)

if not exist %LOCAL_REPO% (
	md %LOCAL_REPO%
)

cd %LOCAL_REPO%
call git init
git remote add A %REMOTE_URL%
git fetch A
call git checkout -b %DEPLOY_BRANCH% A/%DEPLOY_BRANCH%
git pull

::项目根目录
cd %~dp0
echo 准备deploy,将项目发布到本地存储库%LOCAL_REPO%
::call mvn clean -U package deploy -Dmaven.test.skip -DaltDeploymentRepository=maven-repo-public-snapshots::default::file:%LOCAL_REPO%
call mvn deploy -DaltDeploymentRepository=maven-repo-public-snapshots::default::file:%LOCAL_REPO%

cd %LOCAL_REPO%
echo 将本地存储库发布到远程仓库分支: %DEPLOY_BRANCH% 上
git add .
git commit -m "依赖仓库更新"
git pull
git push A %DEPLOY_BRANCH%

echo mvn deploy 发布成功!
