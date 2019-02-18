cd %~dp0

::call sbt clean compile test server/assembly client/assembly

xcopy /e /y .\common .\test\s\common\
xcopy /e /y .\common .\test\c\common\

xcopy /e /y .\project .\test\s\project\
xcopy /e /y .\project .\test\c\project\

xcopy /e /y .\target .\test\s\target\
xcopy /e /y .\target .\test\c\target\


xcopy /e /y .\server .\test\s\server\
xcopy /e /y .\client .\test\c\client\

cd test\s


start "1" cmd /c "java -jar server/target/scala-2.12/server.jar -p 45678 & pause"
timeout 3

SET base=45682
SET clusterSize=6
SET /a top=%base%+%clusterSize%-2
for /l %%x in (%base%,1,%top%) do (
	timeout 1
	start "%%x" cmd /c "java -jar server/target/scala-2.12/server.jar -p %%x -s localhost:45678 & pause"	
)

REM timeout 30
REM cd ..\c
REM start "4" cmd /c "call java -jar client/target/scala-2.12/client.jar -p 56787 -s localhost:45678 & pause"
