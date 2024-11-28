@REM rmdir /s /q build
@REM mkdir build
cd build
rmdir /s /q test
cmake ..
cmake --build . --config Release
cd test
ctest -C Release
@pause