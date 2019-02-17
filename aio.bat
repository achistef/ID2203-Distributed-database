cd %~dp0

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

for /l %%x in (45681,1,45685) do (
	timeout 10
	start "%%x" cmd /c "java -jar server/target/scala-2.12/server.jar -p %%x -s localhost:45678 & pause"	
)

::timeout 30
::cd ..\c
::start "4" cmd /c "call java -jar client/target/scala-2.12/client.jar -p 56787 -s localhost:45678 & pause"
