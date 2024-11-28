cd build

cmake ..
cmake --build . --config Release
cd main/Release
start myjqlserver.exe
start myjqlshell.exe

@pause