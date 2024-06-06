@REM rmdir /s /q build
@REM mkdir build
cd build
rmdir /s /q test
cmake ..
cmake --build .
cd test
ctest -C Debug
@pause