@echo off
setlocal

set IMAGE_NAME=dwh-coding-challenge
set CONTAINER_NAME=dwh-solution

echo Building Docker image...
REM Build dari root repo (..), tapi Dockerfile ada di folder solution
docker build -t %IMAGE_NAME% -f "%~dp0Dockerfile" "%~dp0.."
if %ERRORLEVEL% NEQ 0 (
  echo Failed to build Docker image
  exit /b 1
)

echo Removing old container if exists...
docker rm -f %CONTAINER_NAME% 2>nul

echo Running Docker container...
docker run --name %CONTAINER_NAME% %IMAGE_NAME%

echo Cleaning up...
docker rm %CONTAINER_NAME% 2>nul

echo ================================
echo Execution completed!
echo ================================
pause
